//! Per-view polling loop that replaces Kubernetes watches for ns4kafka.
//!
//! Fetches the active resource list at a configurable cadence, diffs the new
//! list against the previous snapshot for that `generation`, and emits
//! generation-tagged `Msg::{Applied, Deleted, Synced, PollError}` over an
//! unbounded channel to the UI event loop.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{RwLock, Semaphore, mpsc, watch};

use crate::api::client::Ns4KafkaClient;
use crate::api::discovery::ResourceDefinition;
use crate::store::{Msg, row_key};

/// Bounded concurrency for background Schema Registry detail hydration.
const SCHEMA_HYDRATE_CONCURRENCY: usize = 6;

pub type SchemaCache = Arc<RwLock<HashMap<String, Value>>>;
pub type SchemaInFlight = Arc<RwLock<HashSet<String>>>;

/// Parameters describing the currently active view being polled.
#[derive(Clone)]
pub struct PollTarget {
    pub generation: u64,
    pub client: Arc<dyn Ns4KafkaClient>,
    pub namespace: Option<String>,
    pub resource: ResourceDefinition,
    pub interval: Duration,
    /// Shared hint of the user's current row cursor so background schema hydration
    /// prioritizes subjects visible around the cursor first.
    pub cursor_hint: Arc<AtomicUsize>,
}

/// Perform one immediate poll and send the diff messages (`Applied`, `Deleted`, `Synced`)
/// or `PollError` onto `tx`. Updates `prev` in place so unchanged objects do not
/// trigger redundant `Applied` updates.
pub async fn poll_once(
    target: &PollTarget,
    prev: &mut HashMap<String, Value>,
    first_sync: &mut bool,
    tx: &mpsc::UnboundedSender<Msg>,
) {
    let dummy_cache: SchemaCache = Arc::new(RwLock::new(HashMap::new()));
    let dummy_in_flight: SchemaInFlight = Arc::new(RwLock::new(HashSet::new()));
    poll_once_with_cache(
        target,
        prev,
        first_sync,
        &dummy_cache,
        &dummy_in_flight,
        None,
        tx,
    )
    .await;
}

pub async fn poll_once_with_cache(
    target: &PollTarget,
    prev: &mut HashMap<String, Value>,
    first_sync: &mut bool,
    schema_cache: &SchemaCache,
    in_flight: &SchemaInFlight,
    cancel_rx: Option<watch::Receiver<Option<PollTarget>>>,
    tx: &mpsc::UnboundedSender<Msg>,
) {
    match target
        .client
        .list_resources(target.namespace.as_deref(), &target.resource)
        .await
    {
        Ok(mut items) => {
            let is_schema = target.resource.kind == "Schema";
            let ctx_name = target.client.context().name.clone();
            let mut unhydrated: Vec<(usize, String, String, String)> = Vec::new();

            if is_schema {
                // Sort by subject name first so row indices match table default sort
                items.sort_by(|a, b| {
                    let na = a
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let nb = b
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    na.cmp(nb)
                });
                let cache = schema_cache.read().await;
                let mut flight = in_flight.write().await;
                for (idx, obj) in items.iter_mut().enumerate() {
                    let has_spec =
                        obj.pointer("/spec/id").is_some() || obj.pointer("/spec/schema").is_some();
                    if !has_spec {
                        let key = row_key(obj);
                        let cache_key = format!("{ctx_name}/{key}");
                        if let Some(cached) = cache.get(&cache_key) {
                            *obj = cached.clone();
                        } else {
                            let ns = obj
                                .pointer("/metadata/namespace")
                                .and_then(Value::as_str)
                                .or(target.namespace.as_deref())
                                .unwrap_or(&target.client.context().namespace)
                                .to_string();
                            let subject = obj
                                .pointer("/metadata/name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            if !subject.is_empty() {
                                if let Some(map) = obj.as_object_mut() {
                                    map.insert("_hydrating".into(), Value::Bool(true));
                                }
                                if flight.insert(cache_key) {
                                    unhydrated.push((idx, ns, subject, key));
                                }
                            }
                        }
                    }
                }
            }

            let mut next_map = HashMap::with_capacity(items.len());
            for obj in items {
                let key = row_key(&obj);
                let changed = prev.get(&key) != Some(&obj);
                if changed || *first_sync {
                    let _ = tx.send(Msg::Applied {
                        generation: target.generation,
                        key: key.clone(),
                        obj: obj.clone(),
                    });
                }
                next_map.insert(key, obj);
            }
            for old_key in prev.keys() {
                if !next_map.contains_key(old_key) {
                    let _ = tx.send(Msg::Deleted {
                        generation: target.generation,
                        key: old_key.clone(),
                    });
                }
            }
            *prev = next_map;
            *first_sync = false;
            let _ = tx.send(Msg::Synced {
                generation: target.generation,
            });

            if !unhydrated.is_empty() {
                spawn_schema_hydrator(
                    target.clone(),
                    unhydrated,
                    schema_cache.clone(),
                    in_flight.clone(),
                    cancel_rx,
                    tx.clone(),
                );
            }
        }
        Err(err) => {
            let _ = tx.send(Msg::PollError {
                generation: target.generation,
                error: err.message,
                failure: err.failure,
            });
        }
    }
}

/// Spawn bounded-concurrency background hydration of unhydrated Schema subjects,
/// prioritizing rows closest to the user's current table cursor.
fn spawn_schema_hydrator(
    target: PollTarget,
    mut unhydrated: Vec<(usize, String, String, String)>,
    schema_cache: SchemaCache,
    in_flight: SchemaInFlight,
    cancel_rx: Option<watch::Receiver<Option<PollTarget>>>,
    tx: mpsc::UnboundedSender<Msg>,
) {
    tokio::spawn(async move {
        let cursor = target.cursor_hint.load(Ordering::Relaxed);
        unhydrated.sort_by_key(|(idx, _, _, _)| idx.abs_diff(cursor));

        let sem = Arc::new(Semaphore::new(SCHEMA_HYDRATE_CONCURRENCY));
        let ctx_name = target.client.context().name.clone();
        let generation = target.generation;

        let mut handles = Vec::with_capacity(unhydrated.len());
        for (_, ns, subject, key) in unhydrated {
            let cache_key = format!("{ctx_name}/{key}");
            if let Some(rx) = &cancel_rx
                && rx.borrow().as_ref().map(|t| t.generation) != Some(generation)
            {
                in_flight.write().await.remove(&cache_key);
                continue;
            }
            let Ok(permit) = sem.clone().acquire_owned().await else {
                in_flight.write().await.remove(&cache_key);
                continue;
            };
            if let Some(rx) = &cancel_rx
                && rx.borrow().as_ref().map(|t| t.generation) != Some(generation)
            {
                in_flight.write().await.remove(&cache_key);
                continue;
            }

            let client = target.client.clone();
            let cache = schema_cache.clone();
            let flight = in_flight.clone();
            let tx_clone = tx.clone();

            handles.push(tokio::spawn(async move {
                let _permit = permit;
                if let Ok(full_schema) = client.get_schema(&ns, &subject).await {
                    cache
                        .write()
                        .await
                        .insert(cache_key.clone(), full_schema.clone());
                    let _ = tx_clone.send(Msg::Applied {
                        generation,
                        key,
                        obj: full_schema,
                    });
                }
                flight.write().await.remove(&cache_key);
            }));
        }

        for h in handles {
            let _ = h.await;
        }
    });
}

/// Spawn a background poller task that watches for target changes or manual force-refresh
/// triggers and polls on a timer.
pub fn spawn_poller(
    mut target_rx: watch::Receiver<Option<PollTarget>>,
    mut force_rx: mpsc::UnboundedReceiver<()>,
    tx: mpsc::UnboundedSender<Msg>,
) -> tokio::task::JoinHandle<()> {
    let cancel_rx = target_rx.clone();
    tokio::spawn(async move {
        let mut current: Option<PollTarget> = target_rx.borrow_and_update().clone();
        let mut prev: HashMap<String, Value> = HashMap::new();
        let mut first_sync = true;
        let schema_cache: SchemaCache = Arc::new(RwLock::new(HashMap::new()));
        let in_flight: SchemaInFlight = Arc::new(RwLock::new(HashSet::new()));

        if let Some(t) = &current {
            poll_once_with_cache(
                t,
                &mut prev,
                &mut first_sync,
                &schema_cache,
                &in_flight,
                Some(cancel_rx.clone()),
                &tx,
            )
            .await;
        }

        loop {
            let wait_dur = current
                .as_ref()
                .map(|t| t.interval)
                .unwrap_or(Duration::from_secs(5));

            tokio::select! {
                changed = target_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    current = target_rx.borrow_and_update().clone();
                    in_flight.write().await.clear();
                    prev.clear();
                    first_sync = true;
                    if let Some(t) = &current {
                        poll_once_with_cache(
                            t,
                            &mut prev,
                            &mut first_sync,
                            &schema_cache,
                            &in_flight,
                            Some(cancel_rx.clone()),
                            &tx,
                        )
                        .await;
                    }
                }
                forced = force_rx.recv() => {
                    if forced.is_none() {
                        break;
                    }
                    schema_cache.write().await.clear();
                    in_flight.write().await.clear();
                    prev.clear();
                    first_sync = true;
                    if let Some(t) = &current {
                        poll_once_with_cache(
                            t,
                            &mut prev,
                            &mut first_sync,
                            &schema_cache,
                            &in_flight,
                            Some(cancel_rx.clone()),
                            &tx,
                        )
                        .await;
                    }
                }
                _ = tokio::time::sleep(wait_dur) => {
                    if let Some(t) = &current {
                        poll_once_with_cache(
                            t,
                            &mut prev,
                            &mut first_sync,
                            &schema_cache,
                            &in_flight,
                            Some(cancel_rx.clone()),
                            &tx,
                        )
                        .await;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::client::FakeClient;
    use crate::api::discovery::DiscoveryRegistry;

    #[tokio::test]
    async fn poller_emits_applied_deleted_and_synced_with_generation() {
        let client = Arc::new(FakeClient::demo(None));
        let reg = DiscoveryRegistry::builtin();
        let topic_def = reg.find_by_kind("Topic").unwrap().clone();
        let target = PollTarget {
            generation: 7,
            client: client.clone(),
            namespace: Some("team-orders".into()),
            resource: topic_def.clone(),
            interval: Duration::from_secs(5),
            cursor_hint: Arc::new(AtomicUsize::new(0)),
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut prev = HashMap::new();
        let mut first_sync = true;

        poll_once(&target, &mut prev, &mut first_sync, &tx).await;

        let mut applied_count = 0;
        let mut synced = false;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                Msg::Applied { generation, .. } => {
                    assert_eq!(generation, 7);
                    applied_count += 1;
                }
                Msg::Synced { generation } => {
                    assert_eq!(generation, 7);
                    synced = true;
                }
                other => panic!("unexpected msg: {other:?}"),
            }
        }
        assert_eq!(applied_count, 3);
        assert!(synced);

        // Delete one topic and poll again -> emits Msg::Deleted
        client
            .delete_resource(Some("team-orders"), &topic_def, "orders.dlq.v1", false)
            .await
            .unwrap();
        poll_once(&target, &mut prev, &mut first_sync, &tx).await;

        let mut deleted_keys = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let Msg::Deleted { generation, key } = msg {
                assert_eq!(generation, 7);
                deleted_keys.push(key);
            }
        }
        assert_eq!(deleted_keys, vec!["team-orders/orders.dlq.v1"]);
    }

    #[tokio::test]
    async fn poller_hydrates_schema_specs_in_background_and_caches_them() {
        let client = Arc::new(FakeClient::demo(None));
        let reg = DiscoveryRegistry::builtin();
        let schema_def = reg.find_by_kind("Schema").unwrap().clone();
        let target = PollTarget {
            generation: 11,
            client,
            namespace: Some("team-orders".into()),
            resource: schema_def,
            interval: Duration::from_secs(5),
            cursor_hint: Arc::new(AtomicUsize::new(0)),
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut prev = HashMap::new();
        let mut first_sync = true;
        let cache: SchemaCache = Arc::new(RwLock::new(HashMap::new()));
        let in_flight: SchemaInFlight = Arc::new(RwLock::new(HashSet::new()));

        poll_once_with_cache(
            &target,
            &mut prev,
            &mut first_sync,
            &cache,
            &in_flight,
            None,
            &tx,
        )
        .await;
        // Let background hydrator complete
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut hydrated_specs = 0;
        while let Ok(msg) = rx.try_recv() {
            if let Msg::Applied { obj, .. } = msg
                && obj.pointer("/spec/id").is_some()
            {
                hydrated_specs += 1;
            }
        }
        assert_eq!(hydrated_specs, 2);
        assert_eq!(cache.read().await.len(), 2);
    }
}
