//! In-memory store of the currently-polled resource set, plus the
//! generation-tagged `Msg` enum flowing from background tasks to the UI loop.
#![allow(dead_code)]

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use serde_json::Value;

use crate::api::discovery::ResourceDefinition;

pub type RowKey = Rc<str>;
pub type Items = HashMap<RowKey, Arc<Value>>;

/// What a failed poll or API request ran into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollFailure {
    /// 401 Unauthorized — credentials refused or expired.
    Unauthorized,
    /// 403 Forbidden — user lacks role binding for this namespace or admin endpoint.
    Forbidden,
    /// Connection or network error.
    Network,
    /// Server returned 4xx/5xx error response.
    Server,
}

/// Messages flowing from background pollers and action tasks to the UI loop.
/// Tagged with `generation` so stale responses after a namespace/context/view
/// switch are harmlessly dropped.
#[derive(Debug, Clone)]
pub enum Msg {
    Reset {
        generation: u64,
    },
    Applied {
        generation: u64,
        key: String,
        obj: Value,
    },
    Deleted {
        generation: u64,
        key: String,
    },
    Synced {
        generation: u64,
    },
    PollError {
        generation: u64,
        error: String,
        failure: PollFailure,
    },
    /// Discovery completed at connect or context switch.
    Discovered {
        context: String,
        resources: Vec<ResourceDefinition>,
        server_version: Option<String>,
        username: Option<String>,
    },
    /// Namespace list + authorization status for the namespace switcher (`n`).
    NamespacesLoaded {
        context: String,
        /// `(namespace_name, forbidden_403)`
        namespaces: Vec<(String, bool)>,
    },
    /// Server dry-run preview completed; opens Confirm or Diff view before executing.
    DryRunReady {
        generation: u64,
        title: String,
        summary: Vec<String>,
        diff_lines: Vec<String>,
        pending_action: PendingWriteAction,
    },
    /// A write or domain action completed in the background.
    ActionComplete {
        generation: u64,
        message: String,
        is_error: bool,
        /// Optional modal lines (e.g. connector offsets, reset-password one-time display, vault output).
        detail_popup: Option<(String, Vec<String>)>,
        /// Trigger an immediate re-poll of the active view.
        refresh: bool,
    },
}

/// A write operation that has passed dry-run preview and awaits user confirmation.
#[derive(Debug, Clone)]
pub enum PendingWriteAction {
    Apply {
        namespace: Option<String>,
        resource: ResourceDefinition,
        body: Value,
    },
    Delete {
        namespace: Option<String>,
        resource: ResourceDefinition,
        names: Vec<String>,
    },
    DeleteRecords {
        namespace: String,
        topic: String,
    },
    Import {
        namespace: String,
        resource: ResourceDefinition,
    },
    ResetConsumerGroup {
        namespace: String,
        group: String,
        topic: String,
        method: String,
        options: Option<String>,
    },
    DeleteConsumerGroup {
        namespace: String,
        group: String,
    },
    ResetConnectorOffsets {
        namespace: String,
        connector: String,
    },
    ChangeConnectorState {
        namespace: String,
        connectors: Vec<String>,
        action: String,
    },
    UpdateSchemaCompatibility {
        namespace: String,
        subject: String,
        compatibility: Option<String>,
    },
    ResetUserPassword {
        namespace: String,
        user: String,
    },
}

/// Stable identity for a resource row in the store.
pub fn row_key(obj: &Value) -> String {
    let ns = obj.pointer("/metadata/namespace").and_then(Value::as_str);
    let name = obj
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    let kind = obj.get("kind").and_then(Value::as_str).unwrap_or_default();

    // ConsumerGroupMember rows (or partition-level items) are per (group, topic, partition)
    if kind == "ConsumerGroupMember"
        || obj.pointer("/status/topic").is_some() && obj.pointer("/status/partition").is_some()
    {
        let topic = obj
            .pointer("/status/topic")
            .and_then(Value::as_str)
            .unwrap_or("*");
        let part = obj
            .pointer("/status/partition")
            .and_then(Value::as_i64)
            .map(|p| p.to_string())
            .unwrap_or_else(|| "0".into());
        return match ns {
            Some(ns) if !ns.is_empty() => format!("{ns}/{name}/{topic}/{part}"),
            _ => format!("{name}/{topic}/{part}"),
        };
    }

    // ResourceQuota expanded rows can carry `/_quotaKey`
    if let Some(qk) = obj.pointer("/_quotaKey").and_then(Value::as_str) {
        return match ns {
            Some(ns) if !ns.is_empty() => format!("{ns}/{name}/{qk}"),
            _ => format!("{name}/{qk}"),
        };
    }

    match ns {
        Some(ns) if !ns.is_empty() => format!("{ns}/{name}"),
        _ => name.to_string(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreMutation {
    Buffered,
    Inserted,
    Updated,
    Removed,
    Unchanged,
}

#[derive(Default)]
pub struct Store {
    items: Items,
    version: u64,
    pending: Option<Items>,
    pub synced: bool,
}

impl Store {
    pub fn clear(&mut self) {
        self.version += 1;
        self.items.clear();
        self.pending = None;
        self.synced = false;
    }

    pub fn seed(&mut self, items: Items) {
        self.version += 1;
        self.items = items;
        self.pending = None;
        self.synced = false;
    }

    pub fn take_items(&mut self) -> Items {
        self.version += 1;
        self.pending = None;
        self.synced = false;
        std::mem::take(&mut self.items)
    }

    pub fn begin_reset(&mut self) -> bool {
        self.version += 1;
        self.synced = false;
        if self.items.is_empty() {
            self.pending = None;
            true
        } else {
            self.pending = Some(Items::default());
            false
        }
    }

    pub fn finish_sync(&mut self) -> bool {
        self.version += 1;
        self.synced = true;
        match self.pending.take() {
            Some(fresh) => {
                self.items = fresh;
                true
            }
            None => false,
        }
    }

    pub fn apply(&mut self, key: String, obj: Value) -> StoreMutation {
        self.version += 1;
        let key: RowKey = key.into();
        let obj = Arc::new(obj);
        match &mut self.pending {
            Some(pending) => {
                pending.insert(key, obj);
                StoreMutation::Buffered
            }
            None => match self.items.insert(key, obj) {
                Some(_) => StoreMutation::Updated,
                None => StoreMutation::Inserted,
            },
        }
    }

    pub fn remove(&mut self, key: &str) -> StoreMutation {
        self.version += 1;
        match &mut self.pending {
            Some(pending) => {
                pending.remove(key);
                StoreMutation::Buffered
            }
            None => match self.items.remove(key) {
                Some(_) => StoreMutation::Removed,
                None => StoreMutation::Unchanged,
            },
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.items.get(key).map(AsRef::as_ref)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RowKey, &Value)> {
        self.items.iter().map(|(k, v)| (k, v.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn store_mutations_advance_version_and_swap_on_sync() {
        let mut store = Store::default();
        let v0 = store.version();
        let topic = json!({
            "apiVersion": "v1",
            "kind": "Topic",
            "metadata": { "name": "orders.v1", "namespace": "team-a" }
        });
        let key = row_key(&topic);
        assert_eq!(key, "team-a/orders.v1");

        store.apply(key.clone(), topic);
        assert!(store.version() > v0);
        assert_eq!(store.len(), 1);

        // Reset buffers when store is non-empty
        assert!(!store.begin_reset());
        let topic2 = json!({
            "apiVersion": "v1",
            "kind": "Topic",
            "metadata": { "name": "payments.v1", "namespace": "team-a" }
        });
        store.apply(row_key(&topic2), topic2);
        // Old item still visible until finish_sync
        assert!(store.get("team-a/orders.v1").is_some());
        assert!(store.finish_sync());
        assert!(store.get("team-a/orders.v1").is_none());
        assert!(store.get("team-a/payments.v1").is_some());
    }
}
