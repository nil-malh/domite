//! Persistent on-disk and in-memory cache (`~/.cache/domite/`) for resource
//! views, namespace lists, and hydrated Schema Registry specifications.
//!
//! Implements a stale-while-revalidate strategy:
//! - On startup or view/namespace/context switch, cached rows are seeded into
//!   `Store` immediately (`store.synced = false`) so the UI is populated on
//!   frame 1 while the background poller fetches live state.
//! - Hydrated `Schema` objects are persisted per context with a `fetched_at`
//!   timestamp. Within `ttl_secs` (default 24h) they are considered fresh;
//!   once older than `ttl_secs`, their cached values are still displayed
//!   immediately (never blanking back to `-`) while the background hydrator
//!   revalidates them after any completely uncached subjects.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::atomicfile;
use crate::redact;
use crate::store::{Items, row_key};

/// Default TTL before a cached Schema Registry entry is background-revalidated (24 hours).
pub const DEFAULT_SCHEMA_TTL_SECS: i64 = 24 * 3600;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSchemaEntry {
    pub fetched_at: i64,
    pub value: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SchemaCacheFile {
    entries: HashMap<String, CachedSchemaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ViewCacheFile {
    updated_at: i64,
    items: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NamespacesCacheFile {
    updated_at: i64,
    namespaces: Vec<(String, bool)>,
}

enum DiskWriteJob {
    View {
        path: PathBuf,
        items: Vec<Value>,
    },
    Schemas {
        path: PathBuf,
        entries: HashMap<String, CachedSchemaEntry>,
    },
    Namespaces {
        path: PathBuf,
        namespaces: Vec<(String, bool)>,
    },
}

type ViewCacheKey = (String, String, String);
type ViewCacheMap = HashMap<ViewCacheKey, Vec<Value>>;
type SchemaCacheMap = HashMap<String, HashMap<String, CachedSchemaEntry>>;
type NamespaceCacheMap = HashMap<String, Vec<(String, bool)>>;

/// Shared persistent cache handle used by `App` and background poller tasks.
#[derive(Clone)]
pub struct PersistentCache {
    base_dir: Option<PathBuf>,
    schema_ttl_secs: i64,
    /// `(context, ns_or_all, resource_path)` -> cached `Vec<Value>`
    views: Arc<RwLock<ViewCacheMap>>,
    /// `context` -> (`"{ns}/{subject}"` -> `CachedSchemaEntry`)
    schemas: Arc<RwLock<SchemaCacheMap>>,
    /// `context` -> cached `Vec<(namespace, is_forbidden)>`
    namespaces: Arc<RwLock<NamespaceCacheMap>>,
    writer_tx: Option<mpsc::UnboundedSender<DiskWriteJob>>,
}

impl Default for PersistentCache {
    fn default() -> Self {
        Self::in_memory(DEFAULT_SCHEMA_TTL_SECS)
    }
}

impl PersistentCache {
    /// Create a persistent cache backed by `base_dir` (usually `~/.cache/domite`).
    pub fn new(base_dir: Option<PathBuf>, schema_ttl_secs: i64) -> Self {
        let writer_tx = base_dir.as_ref().map(|_| spawn_disk_writer());
        Self {
            base_dir,
            schema_ttl_secs: schema_ttl_secs.max(60),
            views: Arc::new(RwLock::new(HashMap::new())),
            schemas: Arc::new(RwLock::new(HashMap::new())),
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            writer_tx,
        }
    }

    /// Create an in-memory-only cache (used for `--demo` and unit tests).
    pub fn in_memory(schema_ttl_secs: i64) -> Self {
        Self::new(None, schema_ttl_secs)
    }

    pub fn default_cache_dir() -> Option<PathBuf> {
        if let Ok(env_dir) = std::env::var("DOMITE_CACHE_DIR")
            && !env_dir.is_empty()
        {
            return Some(PathBuf::from(env_dir));
        }
        dirs::cache_dir().map(|d| d.join("domite"))
    }

    // -----------------------------------------------------------------------
    // Resource View Snapshots (Topics, Connectors, ConsumerGroups, etc.)
    // -----------------------------------------------------------------------

    /// Load cached resource items for `(context, namespace, resource_path)` into `Store::Items`.
    pub fn load_view_items(
        &self,
        context: &str,
        namespace: Option<&str>,
        resource_path: &str,
    ) -> Option<Items> {
        let ns_key = normalize_ns(namespace);
        let tuple_key = (
            context.to_string(),
            ns_key.clone(),
            resource_path.to_string(),
        );

        let mut list = self
            .views
            .read()
            .ok()
            .and_then(|g| g.get(&tuple_key).cloned());

        if list.is_none()
            && let Some(file_path) = self.view_file_path(context, &ns_key, resource_path)
            && let Ok(raw) = std::fs::read_to_string(&file_path)
            && let Ok(parsed) = serde_json::from_str::<ViewCacheFile>(&raw)
        {
            list = Some(parsed.items.clone());
            if let Ok(mut guard) = self.views.write() {
                guard.insert(tuple_key, parsed.items);
            }
        }

        let mut items = list?;
        if items.is_empty() {
            return None;
        }

        // If this is the Schema view, overlay any hydrated schemas from the schema cache
        if resource_path == "schemas" {
            self.ensure_context_schemas_loaded(context);
            if let Ok(schemas_guard) = self.schemas.read()
                && let Some(ctx_schemas) = schemas_guard.get(context)
            {
                for obj in &mut items {
                    let key = row_key(obj);
                    if let Some(entry) = ctx_schemas.get(&key) {
                        *obj = entry.value.clone();
                    }
                }
            }
        }

        let mut map = Items::with_capacity(items.len());
        for obj in items {
            let k: Rc<str> = row_key(&obj).into();
            map.insert(k, Arc::new(obj));
        }
        Some(map)
    }

    /// Save the latest resource list for `(context, namespace, resource_path)` to memory and disk.
    pub fn save_view_items(
        &self,
        context: &str,
        namespace: Option<&str>,
        resource_path: &str,
        items: &[Value],
    ) {
        let ns_key = normalize_ns(namespace);
        let cleaned: Vec<Value> = items
            .iter()
            .map(|v| {
                let mut c = v.clone();
                if let Some(map) = c.as_object_mut() {
                    map.remove("_hydrating");
                }
                redact::redact_json(&mut c);
                c
            })
            .collect();

        if let Ok(mut guard) = self.views.write() {
            guard.insert(
                (
                    context.to_string(),
                    ns_key.clone(),
                    resource_path.to_string(),
                ),
                cleaned.clone(),
            );
        }

        if let (Some(tx), Some(path)) = (
            &self.writer_tx,
            self.view_file_path(context, &ns_key, resource_path),
        ) {
            let _ = tx.send(DiskWriteJob::View {
                path,
                items: cleaned,
            });
        }
    }

    // -----------------------------------------------------------------------
    // Namespace List Cache
    // -----------------------------------------------------------------------

    pub fn load_namespaces(&self, context: &str) -> Option<Vec<(String, bool)>> {
        if let Some(cached) = self
            .namespaces
            .read()
            .ok()
            .and_then(|g| g.get(context).cloned())
        {
            return Some(cached);
        }
        let path = self.namespaces_file_path(context)?;
        let raw = std::fs::read_to_string(path).ok()?;
        let parsed: NamespacesCacheFile = serde_json::from_str(&raw).ok()?;
        if let Ok(mut guard) = self.namespaces.write() {
            guard.insert(context.to_string(), parsed.namespaces.clone());
        }
        Some(parsed.namespaces)
    }

    pub fn save_namespaces(&self, context: &str, namespaces: &[(String, bool)]) {
        if let Ok(mut guard) = self.namespaces.write() {
            guard.insert(context.to_string(), namespaces.to_vec());
        }
        if let (Some(tx), Some(path)) = (&self.writer_tx, self.namespaces_file_path(context)) {
            let _ = tx.send(DiskWriteJob::Namespaces {
                path,
                namespaces: namespaces.to_vec(),
            });
        }
    }

    // -----------------------------------------------------------------------
    // Schema Registry Hydration Cache (with Stale-While-Revalidate TTL)
    // -----------------------------------------------------------------------

    pub fn ensure_context_schemas_loaded(&self, context: &str) {
        let already_loaded = self
            .schemas
            .read()
            .ok()
            .is_some_and(|g| g.contains_key(context));
        if already_loaded {
            return;
        }
        let mut loaded_entries = HashMap::new();
        if let Some(path) = self.schema_file_path(context)
            && let Ok(raw) = std::fs::read_to_string(&path)
            && let Ok(parsed) = serde_json::from_str::<SchemaCacheFile>(&raw)
        {
            loaded_entries = parsed.entries;
        }
        if let Ok(mut guard) = self.schemas.write() {
            guard.entry(context.to_string()).or_insert(loaded_entries);
        }
    }

    /// Look up a cached Schema by `key` (`"{ns}/{subject}"`).
    /// Returns `(cached_schema_value, is_stale)` where `is_stale` is true if
    /// `now - fetched_at >= schema_ttl_secs`.
    pub fn get_schema(&self, context: &str, key: &str, now_ts: i64) -> Option<(Value, bool)> {
        self.ensure_context_schemas_loaded(context);
        let guard = self.schemas.read().ok()?;
        let entry = guard.get(context)?.get(key)?;
        let is_stale = now_ts.saturating_sub(entry.fetched_at) >= self.schema_ttl_secs;
        Some((entry.value.clone(), is_stale))
    }

    /// Insert or update a hydrated Schema in memory, and optionally flush the
    /// context's schema cache file to disk.
    pub fn put_schema(&self, context: &str, key: String, mut value: Value, flush_to_disk: bool) {
        self.ensure_context_schemas_loaded(context);
        if let Some(map) = value.as_object_mut() {
            map.remove("_hydrating");
        }
        let now_ts = Utc::now().timestamp();
        if let Ok(mut guard) = self.schemas.write() {
            let ctx_map = guard.entry(context.to_string()).or_default();
            ctx_map.insert(
                key,
                CachedSchemaEntry {
                    fetched_at: now_ts,
                    value,
                },
            );
        }
        if flush_to_disk {
            self.flush_schemas(context);
        }
    }

    /// Mark all cached schemas in `namespace` (or all namespaces if `None`) as expired
    /// (`fetched_at = 0`) so the next poll keeps displaying their cached values while
    /// revalidating them in the background.
    pub fn expire_schemas(&self, context: &str, namespace: Option<&str>) {
        self.ensure_context_schemas_loaded(context);
        let prefix = namespace
            .filter(|ns| !ns.is_empty() && *ns != "*")
            .map(|ns| format!("{ns}/"));
        if let Ok(mut guard) = self.schemas.write()
            && let Some(ctx_map) = guard.get_mut(context)
        {
            for (k, entry) in ctx_map.iter_mut() {
                if prefix.as_ref().is_none_or(|p| k.starts_with(p)) {
                    entry.fetched_at = 0;
                }
            }
        }
        self.flush_schemas(context);
    }

    /// Persist the current in-memory schema map for `context` to disk asynchronously.
    pub fn flush_schemas(&self, context: &str) {
        let Some(tx) = &self.writer_tx else {
            return;
        };
        let Some(path) = self.schema_file_path(context) else {
            return;
        };
        let snapshot = self
            .schemas
            .read()
            .ok()
            .and_then(|g| g.get(context).cloned());
        if let Some(entries) = snapshot {
            let _ = tx.send(DiskWriteJob::Schemas { path, entries });
        }
    }

    #[cfg(test)]
    pub fn schema_count(&self, context: &str) -> usize {
        self.ensure_context_schemas_loaded(context);
        self.schemas
            .read()
            .ok()
            .and_then(|g| g.get(context).map(HashMap::len))
            .unwrap_or(0)
    }

    // -----------------------------------------------------------------------
    // File Path Helpers
    // -----------------------------------------------------------------------

    fn view_file_path(&self, context: &str, ns_key: &str, resource_path: &str) -> Option<PathBuf> {
        let base = self.base_dir.as_ref()?;
        Some(
            base.join("views")
                .join(sanitize_segment(context))
                .join(format!(
                    "{}__{}.json",
                    sanitize_segment(ns_key),
                    sanitize_segment(resource_path)
                )),
        )
    }

    fn namespaces_file_path(&self, context: &str) -> Option<PathBuf> {
        let base = self.base_dir.as_ref()?;
        Some(
            base.join("views")
                .join(sanitize_segment(context))
                .join("_namespaces.json"),
        )
    }

    fn schema_file_path(&self, context: &str) -> Option<PathBuf> {
        let base = self.base_dir.as_ref()?;
        Some(
            base.join("schemas")
                .join(format!("{}.json", sanitize_segment(context))),
        )
    }
}

fn normalize_ns(namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) if !ns.is_empty() && ns != "*" => ns.to_string(),
        _ => "_all".to_string(),
    }
}

fn sanitize_segment(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn spawn_disk_writer() -> mpsc::UnboundedSender<DiskWriteJob> {
    let (tx, mut rx) = mpsc::unbounded_channel::<DiskWriteJob>();
    std::thread::Builder::new()
        .name("domite-cache-writer".into())
        .spawn(move || {
            while let Some(job) = rx.blocking_recv() {
                // Coalesce any queued jobs for the same path
                let mut latest_by_path: HashMap<PathBuf, DiskWriteJob> = HashMap::new();
                latest_by_path.insert(job_path(&job).to_path_buf(), job);
                while let Ok(next) = rx.try_recv() {
                    latest_by_path.insert(job_path(&next).to_path_buf(), next);
                }
                for (_, item) in latest_by_path {
                    write_job_to_disk(item);
                }
            }
        })
        .ok();
    tx
}

fn job_path(job: &DiskWriteJob) -> &Path {
    match job {
        DiskWriteJob::View { path, .. }
        | DiskWriteJob::Schemas { path, .. }
        | DiskWriteJob::Namespaces { path, .. } => path,
    }
}

fn write_job_to_disk(job: DiskWriteJob) {
    let now_ts = Utc::now().timestamp();
    match job {
        DiskWriteJob::View { path, items } => {
            let payload = ViewCacheFile {
                updated_at: now_ts,
                items,
            };
            if let Ok(json) = serde_json::to_string(&payload) {
                let _ = atomicfile::write(&path, &json);
            }
        }
        DiskWriteJob::Schemas { path, entries } => {
            let payload = SchemaCacheFile { entries };
            if let Ok(json) = serde_json::to_string(&payload) {
                let _ = atomicfile::write(&path, &json);
            }
        }
        DiskWriteJob::Namespaces { path, namespaces } => {
            let payload = NamespacesCacheFile {
                updated_at: now_ts,
                namespaces,
            };
            if let Ok(json) = serde_json::to_string(&payload) {
                let _ = atomicfile::write(&path, &json);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn persists_and_reloads_views_namespaces_and_schemas_across_restarts() {
        let dir = std::env::temp_dir().join(format!("domite-cache-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        // Session 1: populate cache and write synchronously via write_job_to_disk
        let cache1 = PersistentCache::new(Some(dir.clone()), 3600);
        let topic = json!({
            "apiVersion": "v1",
            "kind": "Topic",
            "metadata": { "name": "orders.v1", "namespace": "team-orders" },
            "spec": { "partitions": 6 }
        });
        cache1.save_view_items("dev", Some("team-orders"), "topics", &[topic]);
        cache1.save_namespaces(
            "dev",
            &[("team-orders".into(), false), ("restricted".into(), true)],
        );
        let schema = json!({
            "apiVersion": "v1",
            "kind": "Schema",
            "metadata": { "name": "orders.v1-value", "namespace": "team-orders" },
            "spec": { "id": 42, "version": 2, "compatibility": "BACKWARD" }
        });
        cache1.put_schema("dev", "team-orders/orders.v1-value".into(), schema, true);

        // Wait briefly for background writer thread to flush
        std::thread::sleep(std::time::Duration::from_millis(40));

        // Session 2: brand new PersistentCache instance reading from disk
        let cache2 = PersistentCache::new(Some(dir.clone()), 3600);
        let loaded_topics = cache2
            .load_view_items("dev", Some("team-orders"), "topics")
            .expect("topics view should load from disk");
        assert_eq!(loaded_topics.len(), 1);
        assert_eq!(
            loaded_topics["team-orders/orders.v1"]
                .pointer("/spec/partitions")
                .and_then(Value::as_i64),
            Some(6)
        );

        let loaded_ns = cache2
            .load_namespaces("dev")
            .expect("namespaces should load from disk");
        assert_eq!(loaded_ns.len(), 2);

        let now = Utc::now().timestamp();
        let (loaded_schema, is_stale) = cache2
            .get_schema("dev", "team-orders/orders.v1-value", now)
            .expect("schema should load from disk");
        assert!(!is_stale);
        assert_eq!(
            loaded_schema.pointer("/spec/id").and_then(Value::as_i64),
            Some(42)
        );

        // Check TTL expiration marks schema as stale while still returning its cached value
        let (_, stale_after_ttl) = cache2
            .get_schema("dev", "team-orders/orders.v1-value", now + 7200)
            .unwrap();
        assert!(stale_after_ttl);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
