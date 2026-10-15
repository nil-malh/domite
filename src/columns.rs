//! Built-in column specifications per ns4kafka resource kind, plus JSON-Pointer
//! user-defined views (`[views.<kind>]` in `config.toml`) and a `NAME / AGE`
//! fallback for any unknown resource kind.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::api::config::ViewConfig;

/// How a column's value is extracted, formatted, colored, and sorted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnExtractor {
    /// RFC 6901 JSON Pointer (`/spec/partitions`, `/metadata/status/phase`, etc.)
    Pointer(String),
    /// Joined array elements (`/spec/tags`, `/spec/role/verbs`, `/spec/role/resourceTypes`)
    JoinedArray(String),
    /// Human-readable duration or ms config (`/spec/configs/retention.ms`)
    RetentionMs(String),
    /// Connector running/total tasks summary from `/status/tasks`
    ConnectorTasks,
    /// ResourceQuota `KEY`, `VALUE`, or `USED`
    QuotaField(QuotaColumn),
    /// Relative age from `/metadata/creationTimestamp` (or `/status/lastUpdateTime`)
    Age,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaColumn {
    Key,
    Value,
    Used,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColumnKind {
    #[default]
    Text,
    Number,
    Status,
    Time,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub header: String,
    pub extractor: ColumnExtractor,
    pub kind: ColumnKind,
    pub wide: bool,
}

/// Comparable sort key for a rendered cell.
#[derive(Debug, Clone, PartialEq)]
pub enum SortKey {
    Num(f64),
    Text(String),
}

impl Eq for SortKey {}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SortKey::Num(a), SortKey::Num(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (SortKey::Text(a), SortKey::Text(b)) => a.cmp(b),
            (SortKey::Num(_), SortKey::Text(_)) => std::cmp::Ordering::Less,
            (SortKey::Text(_), SortKey::Num(_)) => std::cmp::Ordering::Greater,
        }
    }
}

/// Return the column definitions for `kind`, honoring `all_namespaces` (which prepends
/// `NAMESPACE` for namespaced resources), `wide` mode, and any user view override in `config.toml`.
pub fn columns_for_kind(
    kind: &str,
    path: &str,
    namespaced: bool,
    all_namespaces: bool,
    wide: bool,
    user_views: &HashMap<String, ViewConfig>,
) -> Vec<ColumnSpec> {
    let mut cols = Vec::new();
    if namespaced && all_namespaces {
        cols.push(ColumnSpec {
            header: "NAMESPACE".into(),
            extractor: ColumnExtractor::Pointer("/metadata/namespace".into()),
            kind: ColumnKind::Text,
            wide: false,
        });
    }

    let user_view = user_views
        .get(&kind.to_ascii_lowercase())
        .or_else(|| user_views.get(path))
        .or_else(|| user_views.get(kind));

    if let Some(uv) = user_view
        && uv.replace
    {
        for c in &uv.columns {
            if !wide && c.wide {
                continue;
            }
            cols.push(compile_user_col(c));
        }
        return cols;
    }

    for spec in builtin_columns(kind) {
        if !wide && spec.wide {
            continue;
        }
        cols.push(spec);
    }

    if let Some(uv) = user_view {
        for c in &uv.columns {
            if !wide && c.wide {
                continue;
            }
            let spec = compile_user_col(c);
            if let Some(existing) = cols.iter_mut().find(|col| col.header == spec.header) {
                *existing = spec;
            } else {
                cols.push(spec);
            }
        }
    }

    cols
}

fn compile_user_col(c: &crate::api::config::UserColumnConfig) -> ColumnSpec {
    let kind = match c.kind.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("number") => ColumnKind::Number,
        Some("status") => ColumnKind::Status,
        Some("time") => ColumnKind::Time,
        _ => ColumnKind::Text,
    };
    let extractor = if kind == ColumnKind::Time && c.path == "/metadata/creationTimestamp" {
        ColumnExtractor::Age
    } else {
        ColumnExtractor::Pointer(c.path.clone())
    };
    ColumnSpec {
        header: c.name.trim().to_ascii_uppercase(),
        extractor,
        kind,
        wide: c.wide,
    }
}

/// Curated columns per ns4kafka kind (matching Section 4 of the domite design spec),
/// with a `NAME / AGE` fallback for unknown kinds.
pub fn builtin_columns(kind: &str) -> Vec<ColumnSpec> {
    match kind {
        "Topic" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_ptr("PARTITIONS", "/spec/partitions", ColumnKind::Number, false),
            col_ptr("RF", "/spec/replicationFactor", ColumnKind::Number, false),
            ColumnSpec {
                header: "TAGS".into(),
                extractor: ColumnExtractor::JoinedArray("/spec/tags".into()),
                kind: ColumnKind::Text,
                wide: false,
            },
            ColumnSpec {
                header: "RETENTION".into(),
                extractor: ColumnExtractor::RetentionMs("/spec/configs/retention.ms".into()),
                kind: ColumnKind::Number,
                wide: false,
            },
            col_ptr(
                "STATUS",
                "/metadata/status/phase",
                ColumnKind::Status,
                false,
            ),
            col_ptr(
                "CLEANUP",
                "/spec/configs/cleanup.policy",
                ColumnKind::Text,
                true,
            ),
            col_age(),
        ],
        "Connector" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_ptr(
                "CONNECT-CLUSTER",
                "/spec/connectCluster",
                ColumnKind::Text,
                false,
            ),
            col_ptr(
                "CLASS",
                "/spec/config/connector.class",
                ColumnKind::Text,
                false,
            ),
            col_ptr("STATE", "/status/state", ColumnKind::Status, false),
            ColumnSpec {
                header: "TASKS".into(),
                extractor: ColumnExtractor::ConnectorTasks,
                kind: ColumnKind::Text,
                wide: false,
            },
            col_ptr(
                "SYNC-STATUS",
                "/metadata/status/phase",
                ColumnKind::Status,
                true,
            ),
            col_age(),
        ],
        "Schema" => vec![
            col_ptr("SUBJECT", "/metadata/name", ColumnKind::Text, false),
            col_ptr("VERSION", "/spec/version", ColumnKind::Number, false),
            col_ptr("ID", "/spec/id", ColumnKind::Number, false),
            col_ptr(
                "COMPATIBILITY",
                "/spec/compatibility",
                ColumnKind::Text,
                false,
            ),
            col_ptr("TYPE", "/spec/schemaType", ColumnKind::Text, true),
        ],
        "AccessControlEntry" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_ptr("GRANTED-TO", "/spec/grantedTo", ColumnKind::Text, false),
            col_ptr(
                "RESOURCE-TYPE",
                "/spec/resourceType",
                ColumnKind::Text,
                false,
            ),
            col_ptr("RESOURCE", "/spec/resource", ColumnKind::Text, false),
            col_ptr(
                "PATTERN",
                "/spec/resourcePatternType",
                ColumnKind::Text,
                false,
            ),
            col_ptr("PERMISSION", "/spec/permission", ColumnKind::Text, false),
        ],
        "ConsumerGroup" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_ptr("STATE", "/status/state", ColumnKind::Status, false),
            col_ptr("TOPIC", "/status/topic", ColumnKind::Text, false),
            col_ptr("PARTITION", "/status/partition", ColumnKind::Number, false),
            col_ptr(
                "CURRENT-OFFSET",
                "/status/currentOffset",
                ColumnKind::Number,
                false,
            ),
            col_ptr(
                "END-OFFSET",
                "/status/logEndOffset",
                ColumnKind::Number,
                false,
            ),
            col_ptr("LAG", "/status/lag", ColumnKind::Number, false),
        ],
        "KafkaStream" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_age(),
        ],
        "ResourceQuota" | "ResourceQuotaResponse" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            ColumnSpec {
                header: "KEY".into(),
                extractor: ColumnExtractor::QuotaField(QuotaColumn::Key),
                kind: ColumnKind::Text,
                wide: false,
            },
            ColumnSpec {
                header: "VALUE".into(),
                extractor: ColumnExtractor::QuotaField(QuotaColumn::Value),
                kind: ColumnKind::Text,
                wide: false,
            },
            ColumnSpec {
                header: "USED".into(),
                extractor: ColumnExtractor::QuotaField(QuotaColumn::Used),
                kind: ColumnKind::Text,
                wide: false,
            },
            col_age(),
        ],
        "ConnectCluster" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_ptr("URL", "/spec/url", ColumnKind::Text, false),
            col_ptr("STATUS", "/spec/status", ColumnKind::Status, true),
            col_age(),
        ],
        "RoleBinding" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            ColumnSpec {
                header: "RESOURCE-TYPES".into(),
                extractor: ColumnExtractor::JoinedArray("/spec/role/resourceTypes".into()),
                kind: ColumnKind::Text,
                wide: false,
            },
            ColumnSpec {
                header: "VERBS".into(),
                extractor: ColumnExtractor::JoinedArray("/spec/role/verbs".into()),
                kind: ColumnKind::Text,
                wide: false,
            },
            col_ptr(
                "SUBJECT-TYPE",
                "/spec/subject/subjectType",
                ColumnKind::Text,
                false,
            ),
            col_ptr(
                "SUBJECT",
                "/spec/subject/subjectName",
                ColumnKind::Text,
                false,
            ),
        ],
        "Namespace" => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_ptr("CLUSTER", "/metadata/cluster", ColumnKind::Text, false),
            col_ptr("KAFKA-USER", "/spec/kafkaUser", ColumnKind::Text, false),
            col_age(),
        ],
        // Generic fallback for any unknown or future resource kind
        _ => vec![
            col_ptr("NAME", "/metadata/name", ColumnKind::Text, false),
            col_age(),
        ],
    }
}

fn col_ptr(header: &str, pointer: &str, kind: ColumnKind, wide: bool) -> ColumnSpec {
    ColumnSpec {
        header: header.into(),
        extractor: ColumnExtractor::Pointer(pointer.into()),
        kind,
        wide,
    }
}

fn col_age() -> ColumnSpec {
    ColumnSpec {
        header: "AGE".into(),
        extractor: ColumnExtractor::Age,
        kind: ColumnKind::Time,
        wide: false,
    }
}

/// Render a single cell and compute its typed sort key for `obj`.
pub fn eval_column(spec: &ColumnSpec, obj: &Value, now: DateTime<Utc>) -> (String, SortKey) {
    match &spec.extractor {
        ColumnExtractor::Pointer(ptr) => {
            let val = obj.pointer(ptr);
            let text = match val {
                Some(Value::String(s)) => {
                    // Shorten package-qualified Java connector classes (`io.confluent.connect.jdbc.JdbcSourceConnector` -> `JdbcSourceConnector`) unless wide
                    if ptr == "/spec/config/connector.class" && !spec.wide {
                        s.rsplit('.').next().unwrap_or(s).to_string()
                    } else {
                        s.clone()
                    }
                }
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(Value::Array(arr)) => arr
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
                    .join(","),
                _ => {
                    if obj.get("_hydrating").and_then(Value::as_bool) == Some(true)
                        && ptr.starts_with("/spec/")
                    {
                        "…".into()
                    } else {
                        "-".into()
                    }
                }
            };
            let sort = match spec.kind {
                ColumnKind::Number => val
                    .and_then(Value::as_f64)
                    .or_else(|| text.parse::<f64>().ok())
                    .map(SortKey::Num)
                    .unwrap_or_else(|| SortKey::Num(-1.0)),
                _ => SortKey::Text(text.to_ascii_lowercase()),
            };
            (text, sort)
        }
        ColumnExtractor::JoinedArray(ptr) => {
            let text = obj
                .pointer(ptr)
                .and_then(Value::as_array)
                .map(|arr| {
                    let joined = arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    if joined.is_empty() {
                        "-".into()
                    } else {
                        joined
                    }
                })
                .unwrap_or_else(|| "-".into());
            let sort = SortKey::Text(text.to_ascii_lowercase());
            (text, sort)
        }
        ColumnExtractor::RetentionMs(ptr) => {
            let raw = obj.pointer(ptr).and_then(|v| {
                v.as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| v.as_i64())
            });
            match raw {
                Some(ms) if ms < 0 => ("infinite".into(), SortKey::Num(f64::MAX)),
                Some(ms) => (format_retention_ms(ms), SortKey::Num(ms as f64)),
                None => ("-".into(), SortKey::Num(-1.0)),
            }
        }
        ColumnExtractor::ConnectorTasks => {
            let tasks = obj.pointer("/status/tasks").and_then(Value::as_array);
            match tasks {
                Some(arr) => {
                    let total = arr.len();
                    let running = arr
                        .iter()
                        .filter(|t| {
                            t.get("state")
                                .and_then(Value::as_str)
                                .is_some_and(|s| s.eq_ignore_ascii_case("RUNNING"))
                        })
                        .count();
                    let text = format!("{running}/{total}");
                    let ratio = if total == 0 {
                        0.0
                    } else {
                        running as f64 / total as f64
                    };
                    (text, SortKey::Num(ratio))
                }
                None => ("0/0".into(), SortKey::Num(0.0)),
            }
        }
        ColumnExtractor::QuotaField(qf) => {
            let text = extract_quota_field(obj, *qf);
            let sort = SortKey::Text(text.to_ascii_lowercase());
            (text, sort)
        }
        ColumnExtractor::Age => match age_seconds(obj, now) {
            Some(secs) => (format_age(secs), SortKey::Num(secs as f64)),
            None => ("-".into(), SortKey::Num(f64::MAX)),
        },
    }
}

/// Extract row status string for `theme::row_color` and `theme::status_color`.
pub fn row_status(obj: &Value) -> String {
    // Check connector or consumer group `/status/state` first
    if let Some(state) = obj.pointer("/status/state").and_then(Value::as_str) {
        // Also check if any connector task is FAILED even when connector state is RUNNING
        if let Some(tasks) = obj.pointer("/status/tasks").and_then(Value::as_array)
            && tasks.iter().any(|t| {
                t.get("state")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s.eq_ignore_ascii_case("FAILED"))
            })
        {
            return "FAILED".into();
        }
        return state.to_string();
    }
    if let Some(phase) = obj
        .pointer("/metadata/status/phase")
        .and_then(Value::as_str)
    {
        return phase.to_string();
    }
    if let Some(cc_status) = obj.pointer("/spec/status").and_then(Value::as_str) {
        return cc_status.to_string();
    }
    String::new()
}

/// Compute elapsed seconds since `/metadata/creationTimestamp` (or `/metadata/updateTimestamp`).
pub fn age_seconds(obj: &Value, now: DateTime<Utc>) -> Option<i64> {
    let ts_str = obj
        .pointer("/metadata/creationTimestamp")
        .or_else(|| obj.pointer("/metadata/updateTimestamp"))
        .or_else(|| obj.pointer("/status/lastUpdateTime"))
        .and_then(Value::as_str)?;
    let parsed = DateTime::parse_from_rfc3339(ts_str)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()?;
    Some((now - parsed).num_seconds().max(0))
}

pub fn format_age(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if h < 10 && m > 0 {
            format!("{h}h{m}m")
        } else {
            format!("{h}h")
        }
    } else {
        let d = secs / 86_400;
        let h = (secs % 86_400) / 3600;
        if d < 10 && h > 0 {
            format!("{d}d{h}h")
        } else {
            format!("{d}d")
        }
    }
}

pub fn format_retention_ms(ms: i64) -> String {
    let secs = ms / 1000;
    if secs <= 0 {
        return format!("{ms}ms");
    }
    if secs >= 86_400 {
        let d = secs / 86_400;
        let h = (secs % 86_400) / 3600;
        if d < 10 && h > 0 {
            format!("{d}d{h}h")
        } else {
            format!("{d}d")
        }
    } else if secs >= 3600 {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if h < 10 && m > 0 {
            format!("{h}h{m}m")
        } else {
            format!("{h}h")
        }
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn extract_quota_field(obj: &Value, qf: QuotaColumn) -> String {
    // Support expanded quota rows (`/_quotaKey`, `/_quotaValue`, `/_quotaUsed`)
    match qf {
        QuotaColumn::Key => {
            if let Some(k) = obj.pointer("/_quotaKey").and_then(Value::as_str) {
                return k.to_string();
            }
        }
        QuotaColumn::Value => {
            if let Some(v) = obj.pointer("/_quotaValue").and_then(Value::as_str) {
                return v.to_string();
            }
        }
        QuotaColumn::Used => {
            if let Some(u) = obj.pointer("/_quotaUsed").and_then(Value::as_str) {
                return u.to_string();
            }
        }
    }

    // Also support raw `ResourceQuotaResponse` (`spec.countTopic = "3/10"`, etc.)
    // or raw `ResourceQuota` (`spec: { "count/topics": "10", ... }`)
    let response_fields = [
        ("count/topics", "/spec/countTopic"),
        ("count/partitions", "/spec/countPartition"),
        ("disk/topics", "/spec/diskTopic"),
        ("count/connectors", "/spec/countConnector"),
        ("user/producer_byte_rate", "/spec/producerByteRate"),
        ("user/consumer_byte_rate", "/spec/consumerByteRate"),
    ];
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    let mut useds = Vec::new();

    for (label, ptr) in response_fields {
        if let Some(raw) = obj.pointer(ptr).and_then(Value::as_str)
            && !raw.is_empty()
        {
            keys.push(label.to_string());
            if let Some((u, v)) = raw.split_once('/') {
                useds.push(u.to_string());
                vals.push(v.to_string());
            } else {
                useds.push("-".to_string());
                vals.push(raw.to_string());
            }
        }
    }

    if keys.is_empty()
        && let Some(spec_map) = obj.get("spec").and_then(Value::as_object)
    {
        for (k, v) in spec_map {
            if let Some(s) = v.as_str() {
                keys.push(k.clone());
                vals.push(s.to_string());
                useds.push("-".to_string());
            }
        }
    }

    let list = match qf {
        QuotaColumn::Key => keys,
        QuotaColumn::Value => vals,
        QuotaColumn::Used => useds,
    };
    if list.is_empty() {
        "-".into()
    } else {
        list.join(", ")
    }
}

/// Extract `metadata.labels` as a `HashMap<String, String>` for `label:` filtering.
pub fn extract_labels(obj: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(map) = obj.pointer("/metadata/labels").and_then(Value::as_object) {
        for (k, v) in map {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    if let Some(tags) = obj.pointer("/spec/tags").and_then(Value::as_array) {
        for tag in tags.iter().filter_map(Value::as_str) {
            out.insert("tag".into(), tag.to_string());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn evaluates_curated_topic_and_connector_columns() {
        let now = DateTime::parse_from_rfc3339("2026-10-14T22:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let topic = json!({
            "apiVersion": "v1",
            "kind": "Topic",
            "metadata": {
                "name": "team.orders.v1",
                "namespace": "team-a",
                "creationTimestamp": "2026-10-14T20:00:00Z",
                "status": { "phase": "Success" }
            },
            "spec": {
                "partitions": 6,
                "replicationFactor": 3,
                "tags": ["PCI", "CRITICAL"],
                "configs": {
                    "cleanup.policy": "delete",
                    "retention.ms": "604800000"
                }
            }
        });

        let cols = columns_for_kind("Topic", "topics", true, false, false, &HashMap::new());
        let rendered: Vec<String> = cols.iter().map(|c| eval_column(c, &topic, now).0).collect();
        assert_eq!(
            rendered,
            vec![
                "team.orders.v1",
                "6",
                "3",
                "PCI,CRITICAL",
                "7d",
                "Success",
                "2h"
            ]
        );

        let connector = json!({
            "apiVersion": "v1",
            "kind": "Connector",
            "metadata": {
                "name": "team.jdbc-sink",
                "namespace": "team-a",
                "creationTimestamp": "2026-10-13T22:00:00Z",
                "status": { "phase": "Success" }
            },
            "spec": {
                "connectCluster": "connect-main",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"
                }
            },
            "status": {
                "state": "RUNNING",
                "tasks": [
                    { "id": "0", "state": "RUNNING", "workerId": "w1:8083" },
                    { "id": "1", "state": "FAILED", "trace": "java.sql.SQLException: Timeout", "workerId": "w2:8083" }
                ]
            }
        });
        let conn_cols = columns_for_kind(
            "Connector",
            "connectors",
            true,
            false,
            false,
            &HashMap::new(),
        );
        let conn_rendered: Vec<String> = conn_cols
            .iter()
            .map(|c| eval_column(c, &connector, now).0)
            .collect();
        assert_eq!(
            conn_rendered,
            vec![
                "team.jdbc-sink",
                "connect-main",
                "JdbcSinkConnector",
                "RUNNING",
                "1/2",
                "1d"
            ]
        );
        assert_eq!(
            row_status(&connector),
            "FAILED",
            "connector with a failed task should highlight as FAILED"
        );
    }
}
