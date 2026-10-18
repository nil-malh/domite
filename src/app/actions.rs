//! Kind-specific actions, dry-run diff previews, and background task dispatchers.

use std::sync::Arc;

use serde_json::Value;
use similar::TextDiff;
use tokio::sync::mpsc;

use crate::api::client::Ns4KafkaClient;
use crate::store::{Msg, PendingWriteAction};

/// Entries shown in the `t` resource action menu (`Mode::ActionMenu`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MenuEntry {
    pub label: String,
    pub description: String,
    pub mutating: bool,
    pub op: MenuOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MenuOp {
    // Topic actions
    TopicDeleteRecords {
        topic: String,
    },
    TopicImportUnsynced,
    TopicShowConfigs {
        topic: String,
    },
    TopicJumpConsumerGroups {
        topic: String,
    },
    TopicJumpAcls {
        topic: String,
    },
    TopicJumpSchemas {
        topic: String,
    },
    // Connector actions
    ConnectorChangeState {
        connectors: Vec<String>,
        action: String,
    },
    ConnectorViewOffsets {
        connector: String,
    },
    ConnectorResetOffsets {
        connector: String,
    },
    ConnectorImportUnsynced,
    // ConsumerGroup actions
    ConsumerGroupDrillPartitions {
        group: String,
        ownership: String,
    },
    ConsumerGroupJumpTopic {
        topic: String,
    },
    ConsumerGroupReset {
        group: String,
        topic: String,
        method: String,
        needs_input: Option<String>,
    },
    ConsumerGroupDelete {
        group: String,
    },
    // Schema actions
    SchemaSetCompatibility {
        subject: String,
        compatibility: Option<String>,
    },
    // ConnectCluster actions
    ConnectClusterVault {
        connect_cluster: String,
    },
    ConnectClusterJumpConnectors {
        connect_cluster: String,
    },
    // Namespace actions
    NamespaceResetPassword {
        namespace: String,
        kafka_user: String,
    },
    NamespaceEnter {
        namespace: String,
    },
}

/// Build the `t` action menu entries for the currently selected resource(s).
pub fn menu_for_resource(
    kind: &str,
    selected_obj: Option<&Value>,
    marked_names: &[String],
) -> Vec<MenuEntry> {
    let name = selected_obj
        .and_then(|v| v.pointer("/metadata/name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let targets = if !marked_names.is_empty() {
        marked_names.to_vec()
    } else if !name.is_empty() {
        vec![name.clone()]
    } else {
        Vec::new()
    };

    match kind {
        "Topic" => {
            let mut items = Vec::new();
            if !name.is_empty() {
                items.push(MenuEntry {
                    label: "Jump to Consumer Groups".into(),
                    description: format!("View consumer groups reading '{name}'"),
                    mutating: false,
                    op: MenuOp::TopicJumpConsumerGroups {
                        topic: name.clone(),
                    },
                });
                items.push(MenuEntry {
                    label: "Jump to ACLs".into(),
                    description: format!("View ACLs matching '{name}'"),
                    mutating: false,
                    op: MenuOp::TopicJumpAcls {
                        topic: name.clone(),
                    },
                });
                items.push(MenuEntry {
                    label: "Jump to Schemas".into(),
                    description: format!("View schemas for subject prefix '{name}'"),
                    mutating: false,
                    op: MenuOp::TopicJumpSchemas {
                        topic: name.clone(),
                    },
                });
                items.push(MenuEntry {
                    label: "Show Topic Configs".into(),
                    description: format!("Inspect /spec/configs for '{name}'"),
                    mutating: false,
                    op: MenuOp::TopicShowConfigs {
                        topic: name.clone(),
                    },
                });
                items.push(MenuEntry {
                    label: "Delete Records (truncate)".into(),
                    description: format!(
                        "POST /topics/{name}/delete-records (with dry-run preview)"
                    ),
                    mutating: true,
                    op: MenuOp::TopicDeleteRecords {
                        topic: name.clone(),
                    },
                });
            }
            items.push(MenuEntry {
                label: "Import Unsynchronized Topics".into(),
                description: "POST /topics/_/import (with dry-run preview)".into(),
                mutating: true,
                op: MenuOp::TopicImportUnsynced,
            });
            items
        }
        "Connector" => {
            let mut items = Vec::new();
            if !targets.is_empty() {
                let target_desc = if targets.len() == 1 {
                    format!("'{}'", targets[0])
                } else {
                    format!("{} selected connectors", targets.len())
                };
                for (action, verb) in [
                    ("RESTART", "Restart"),
                    ("PAUSE", "Pause"),
                    ("RESUME", "Resume"),
                    ("STOP", "Stop"),
                ] {
                    items.push(MenuEntry {
                        label: format!("{verb} Connector"),
                        description: format!("POST change-state {action} on {target_desc}"),
                        mutating: true,
                        op: MenuOp::ConnectorChangeState {
                            connectors: targets.clone(),
                            action: action.into(),
                        },
                    });
                }
            }
            if !name.is_empty() {
                items.push(MenuEntry {
                    label: "View Offsets".into(),
                    description: format!("GET /connectors/{name}/offsets"),
                    mutating: false,
                    op: MenuOp::ConnectorViewOffsets {
                        connector: name.clone(),
                    },
                });
                items.push(MenuEntry {
                    label: "Reset Offsets".into(),
                    description: format!("DELETE /connectors/{name}/offsets (with confirmation)"),
                    mutating: true,
                    op: MenuOp::ConnectorResetOffsets {
                        connector: name.clone(),
                    },
                });
            }
            items.push(MenuEntry {
                label: "Import Unsynchronized Connectors".into(),
                description: "POST /connectors/_/import (with dry-run preview)".into(),
                mutating: true,
                op: MenuOp::ConnectorImportUnsynced,
            });
            items
        }
        "ConsumerGroup" | "ExternalConsumerGroup" | "ConsumerGroupMember" => {
            if name.is_empty() {
                return Vec::new();
            }
            let ownership = selected_obj
                .and_then(|v| v.get("_cgOwnership"))
                .and_then(Value::as_str)
                .unwrap_or("Owned")
                .to_string();

            // In ConsumerGroupMember view, the user can reset either the exact `topic:partition`,
            // the whole `topic`, or all topics `*`. In the summary ConsumerGroup view, default to
            // the single topic if only 1 topic is consumed, or `*` otherwise.
            let status_topic = selected_obj
                .and_then(|v| v.pointer("/status/topic"))
                .and_then(Value::as_str);
            let status_part = selected_obj
                .and_then(|v| v.pointer("/status/partition"))
                .and_then(Value::as_i64);
            let cg_topics = selected_obj
                .and_then(|v| v.get("_cgTopics"))
                .and_then(Value::as_str)
                .unwrap_or("*");

            let default_topic = if let (Some(t), Some(p)) = (status_topic, status_part) {
                format!("{t}:{p}")
            } else if let Some(t) = status_topic {
                t.to_string()
            } else if !cg_topics.is_empty() && cg_topics != "-" && !cg_topics.contains(',') {
                cg_topics.to_string()
            } else {
                "*".to_string()
            };

            let mut entries = Vec::new();
            if kind != "ConsumerGroupMember" {
                entries.push(MenuEntry {
                    label: "View Partition Offsets & Lag".into(),
                    description: format!("Drill down into partition offsets for '{name}'"),
                    mutating: false,
                    op: MenuOp::ConsumerGroupDrillPartitions {
                        group: name.clone(),
                        ownership: ownership.clone(),
                    },
                });
            }
            if let Some(t) = status_topic.or_else(|| {
                (!cg_topics.is_empty() && cg_topics != "-" && !cg_topics.contains(','))
                    .then_some(cg_topics)
            }) {
                entries.push(MenuEntry {
                    label: format!("Jump to Topic ({t})"),
                    description: format!("Inspect topic '{t}'"),
                    mutating: false,
                    op: MenuOp::ConsumerGroupJumpTopic {
                        topic: t.to_string(),
                    },
                });
            }

            entries.extend([
                MenuEntry {
                    label: "Reset Offsets → TO_EARLIEST".into(),
                    description: format!(
                        "Reset '{name}' on '{default_topic}' to earliest offset (dry-run preview)"
                    ),
                    mutating: true,
                    op: MenuOp::ConsumerGroupReset {
                        group: name.clone(),
                        topic: default_topic.clone(),
                        method: "TO_EARLIEST".into(),
                        needs_input: None,
                    },
                },
                MenuEntry {
                    label: "Reset Offsets → TO_LATEST".into(),
                    description: format!(
                        "Reset '{name}' on '{default_topic}' to latest offset (dry-run preview)"
                    ),
                    mutating: true,
                    op: MenuOp::ConsumerGroupReset {
                        group: name.clone(),
                        topic: default_topic.clone(),
                        method: "TO_LATEST".into(),
                        needs_input: None,
                    },
                },
                MenuEntry {
                    label: "Reset Offsets → TO_DATETIME".into(),
                    description: format!(
                        "Reset '{default_topic}' to ISO-8601 timestamp (yyyy-MM-ddTHH:mm:ss.SSS)"
                    ),
                    mutating: true,
                    op: MenuOp::ConsumerGroupReset {
                        group: name.clone(),
                        topic: default_topic.clone(),
                        method: "TO_DATETIME".into(),
                        needs_input: Some(
                            "Enter ISO-8601 datetime (e.g. 2026-10-14T12:00:00.000Z):".into(),
                        ),
                    },
                },
                MenuEntry {
                    label: "Reset Offsets → BY_DURATION".into(),
                    description: format!(
                        "Rewind '{default_topic}' by ISO-8601 duration (e.g. PT2H, PT30M, P1D)"
                    ),
                    mutating: true,
                    op: MenuOp::ConsumerGroupReset {
                        group: name.clone(),
                        topic: default_topic.clone(),
                        method: "BY_DURATION".into(),
                        needs_input: Some(
                            "Enter ISO-8601 duration (e.g. PT2H, PT30M, P1D):".into(),
                        ),
                    },
                },
                MenuEntry {
                    label: "Reset Offsets → TO_OFFSET".into(),
                    description: format!("Reset '{default_topic}' to an explicit numeric offset"),
                    mutating: true,
                    op: MenuOp::ConsumerGroupReset {
                        group: name.clone(),
                        topic: default_topic.clone(),
                        method: "TO_OFFSET".into(),
                        needs_input: Some("Enter target offset number (>= 0):".into()),
                    },
                },
                MenuEntry {
                    label: "Reset Offsets → SHIFT_BY".into(),
                    description: format!("Shift '{default_topic}' offset by +/- N records"),
                    mutating: true,
                    op: MenuOp::ConsumerGroupReset {
                        group: name.clone(),
                        topic: default_topic,
                        method: "SHIFT_BY".into(),
                        needs_input: Some("Enter offset shift (+/- integer):".into()),
                    },
                },
                MenuEntry {
                    label: "Delete Consumer Group".into(),
                    description: format!("DELETE /consumer-groups/{name} (with dry-run preview)"),
                    mutating: true,
                    op: MenuOp::ConsumerGroupDelete { group: name },
                },
            ]);
            entries
        }
        "Schema" => {
            if name.is_empty() {
                return Vec::new();
            }
            let mut items = Vec::new();
            for compat in [
                "BACKWARD",
                "BACKWARD_TRANSITIVE",
                "FORWARD",
                "FORWARD_TRANSITIVE",
                "FULL",
                "FULL_TRANSITIVE",
                "NONE",
            ] {
                items.push(MenuEntry {
                    label: format!("Set Compatibility → {compat}"),
                    description: format!("POST /schemas/{name}/config ({compat})"),
                    mutating: true,
                    op: MenuOp::SchemaSetCompatibility {
                        subject: name.clone(),
                        compatibility: Some(compat.into()),
                    },
                });
            }
            items.push(MenuEntry {
                label: "Reset Compatibility → GLOBAL".into(),
                description: format!("DELETE /schemas/{name}/config"),
                mutating: true,
                op: MenuOp::SchemaSetCompatibility {
                    subject: name,
                    compatibility: None,
                },
            });
            items
        }
        "ConnectCluster" => {
            if name.is_empty() {
                return Vec::new();
            }
            vec![
                MenuEntry {
                    label: "Jump to Connectors".into(),
                    description: format!("View connectors deployed on '{name}'"),
                    mutating: false,
                    op: MenuOp::ConnectClusterJumpConnectors {
                        connect_cluster: name.clone(),
                    },
                },
                MenuEntry {
                    label: "Vault Encrypt Secret".into(),
                    description: format!("POST /connect-clusters/{name}/vaults"),
                    mutating: true,
                    op: MenuOp::ConnectClusterVault {
                        connect_cluster: name,
                    },
                },
            ]
        }
        "Namespace" => {
            if name.is_empty() {
                return Vec::new();
            }
            let kafka_user = selected_obj
                .and_then(|v| v.pointer("/spec/kafkaUser"))
                .and_then(Value::as_str)
                .unwrap_or(&name)
                .to_string();
            vec![
                MenuEntry {
                    label: "Switch to Namespace & View Topics".into(),
                    description: format!("Set active namespace to '{name}'"),
                    mutating: false,
                    op: MenuOp::NamespaceEnter {
                        namespace: name.clone(),
                    },
                },
                MenuEntry {
                    label: format!("Reset Kafka User Password ({kafka_user})"),
                    description: format!(
                        "POST /namespaces/{name}/users/{kafka_user}/reset-password"
                    ),
                    mutating: true,
                    op: MenuOp::NamespaceResetPassword {
                        namespace: name,
                        kafka_user,
                    },
                },
            ]
        }
        _ => Vec::new(),
    }
}

/// Compute a unified diff between two YAML strings for the dry-run preview screen.
pub fn compute_yaml_diff(old_yaml: &str, new_yaml: &str) -> Vec<String> {
    let diff = TextDiff::from_lines(old_yaml, new_yaml);
    let mut lines = Vec::new();
    for change in diff.iter_all_changes() {
        let prefix = match change.tag() {
            similar::ChangeTag::Delete => "-",
            similar::ChangeTag::Insert => "+",
            similar::ChangeTag::Equal => " ",
        };
        lines.push(format!("{prefix}{}", change.value().trim_end_matches('\n')));
    }
    lines
}

/// Spawn a dry-run preview task for a `PendingWriteAction`.
pub fn spawn_dry_run(
    generation: u64,
    client: Arc<dyn Ns4KafkaClient>,
    original_obj: Option<Value>,
    action: PendingWriteAction,
    tx: mpsc::UnboundedSender<Msg>,
) {
    tokio::spawn(async move {
        match &action {
            PendingWriteAction::Apply {
                namespace,
                resource,
                body,
            } => {
                match client
                    .apply_resource(namespace.as_deref(), resource, body, true)
                    .await
                {
                    Ok(res) => {
                        let old_yaml = original_obj
                            .as_ref()
                            .and_then(|v| serde_yaml::to_string(v).ok())
                            .unwrap_or_default();
                        let new_yaml = serde_yaml::to_string(&res.resource).unwrap_or_default();
                        let diff_lines = compute_yaml_diff(&old_yaml, &new_yaml);
                        let name = body
                            .pointer("/metadata/name")
                            .and_then(Value::as_str)
                            .unwrap_or("<resource>");
                        let _ = tx.send(Msg::DryRunReady {
                            generation,
                            title: format!(
                                "Dry-Run Apply {}: {} ({})",
                                resource.kind, name, res.status
                            ),
                            summary: vec![format!(
                                "Server dry-run validation succeeded ({} {} -> {}). Press 'y' or Enter to apply, Esc to cancel.",
                                resource.kind, name, res.status
                            )],
                            diff_lines,
                            pending_action: action,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(Msg::ActionComplete {
                            generation,
                            message: format!("Dry-run apply failed: {e}"),
                            is_error: true,
                            detail_popup: None,
                            refresh: false,
                        });
                    }
                }
            }
            PendingWriteAction::Delete {
                namespace,
                resource,
                names,
            } => {
                let mut preview_items = Vec::new();
                for name in names {
                    match client
                        .delete_resource(namespace.as_deref(), resource, name, true)
                        .await
                    {
                        Ok(items) => preview_items.extend(items),
                        Err(e) => {
                            let _ = tx.send(Msg::ActionComplete {
                                generation,
                                message: format!("Dry-run delete failed for '{name}': {e}"),
                                is_error: true,
                                detail_popup: None,
                                refresh: false,
                            });
                            return;
                        }
                    }
                }
                let mut summary = vec![format!(
                    "Dry-run validated deleting {} {}(s):",
                    names.len(),
                    resource.kind
                )];
                for n in names {
                    summary.push(format!("  - {n}"));
                }
                summary.push(String::new());
                summary.push("Press 'y' or Enter to permanently delete, Esc/n to cancel.".into());
                let diff_lines = serde_yaml::to_string(&preview_items)
                    .unwrap_or_default()
                    .lines()
                    .map(|l| format!("-{l}"))
                    .collect();
                let _ = tx.send(Msg::DryRunReady {
                    generation,
                    title: format!("Confirm Delete {} ({})", resource.kind, names.join(", ")),
                    summary,
                    diff_lines,
                    pending_action: action,
                });
            }
            PendingWriteAction::DeleteRecords { namespace, topic } => {
                match client.delete_records(namespace, topic, true).await {
                    Ok(partitions) => {
                        let mut summary = vec![format!(
                            "Dry-run delete-records on topic '{topic}' in namespace '{namespace}':"
                        )];
                        for p in &partitions {
                            let part = p
                                .pointer("/spec/partition")
                                .and_then(Value::as_i64)
                                .unwrap_or(0);
                            let off = p
                                .pointer("/spec/offset")
                                .and_then(Value::as_i64)
                                .unwrap_or(0);
                            summary.push(format!(
                                "  - partition {part} -> truncate up to offset {off}"
                            ));
                        }
                        summary.push(String::new());
                        summary
                            .push("Press 'y' or Enter to delete records, Esc/n to cancel.".into());
                        let _ = tx.send(Msg::DryRunReady {
                            generation,
                            title: format!("Confirm Delete Records: {topic}"),
                            summary,
                            diff_lines: Vec::new(),
                            pending_action: action,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(Msg::ActionComplete {
                            generation,
                            message: format!("Dry-run delete-records failed: {e}"),
                            is_error: true,
                            detail_popup: None,
                            refresh: false,
                        });
                    }
                }
            }
            PendingWriteAction::Import {
                namespace,
                resource,
            } => {
                match client
                    .import_resources(namespace, &resource.path, "*", true)
                    .await
                {
                    Ok(unsynced) => {
                        if unsynced.is_empty() {
                            let _ = tx.send(Msg::ActionComplete {
                                generation,
                                message: format!(
                                    "No unsynchronized {} found in namespace '{namespace}'",
                                    resource.path
                                ),
                                is_error: false,
                                detail_popup: None,
                                refresh: false,
                            });
                            return;
                        }
                        let mut summary = vec![format!(
                            "Dry-run found {} unsynchronized {}(s) to import into '{namespace}':",
                            unsynced.len(),
                            resource.kind
                        )];
                        for item in &unsynced {
                            let n = item
                                .pointer("/metadata/name")
                                .and_then(Value::as_str)
                                .unwrap_or("<unnamed>");
                            summary.push(format!("  + {n}"));
                        }
                        summary.push(String::new());
                        summary.push("Press 'y' or Enter to import, Esc/n to cancel.".into());
                        let diff_lines = serde_yaml::to_string(&unsynced)
                            .unwrap_or_default()
                            .lines()
                            .map(|l| format!("+{l}"))
                            .collect();
                        let _ = tx.send(Msg::DryRunReady {
                            generation,
                            title: format!("Confirm Import {}", resource.kind),
                            summary,
                            diff_lines,
                            pending_action: action,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(Msg::ActionComplete {
                            generation,
                            message: format!("Dry-run import failed: {e}"),
                            is_error: true,
                            detail_popup: None,
                            refresh: false,
                        });
                    }
                }
            }
            PendingWriteAction::ResetConsumerGroup {
                namespace,
                group,
                topic,
                method,
                options,
            } => {
                match client
                    .reset_consumer_group_offsets(
                        namespace,
                        group,
                        topic,
                        method,
                        options.as_deref(),
                        true,
                    )
                    .await
                {
                    Ok(preview) => {
                        let mut summary = vec![format!(
                            "Dry-run offset reset for group '{group}' on topic '{topic}' ({method}):"
                        )];
                        for p in &preview {
                            let t = p
                                .pointer("/spec/topic")
                                .and_then(Value::as_str)
                                .unwrap_or(topic);
                            let part = p
                                .pointer("/spec/partition")
                                .and_then(Value::as_i64)
                                .unwrap_or(0);
                            let off = p
                                .pointer("/spec/offset")
                                .and_then(Value::as_i64)
                                .unwrap_or(0);
                            summary.push(format!("  - {t}:{part} -> new offset {off}"));
                        }
                        summary.push(String::new());
                        summary.push(
                            "Press 'y' or Enter to execute offset reset, Esc/n to cancel.".into(),
                        );
                        let _ = tx.send(Msg::DryRunReady {
                            generation,
                            title: format!("Confirm Offset Reset: {group}"),
                            summary,
                            diff_lines: Vec::new(),
                            pending_action: action,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(Msg::ActionComplete {
                            generation,
                            message: format!("Dry-run offset reset failed: {e}"),
                            is_error: true,
                            detail_popup: None,
                            refresh: false,
                        });
                    }
                }
            }
            PendingWriteAction::DeleteConsumerGroup { namespace, group } => {
                match client.delete_consumer_group(namespace, group, true).await {
                    Ok(()) => {
                        let summary = vec![
                            format!(
                                "Dry-run validated deleting consumer group '{group}' in namespace '{namespace}'."
                            ),
                            String::new(),
                            "Press 'y' or Enter to delete, Esc/n to cancel.".into(),
                        ];
                        let _ = tx.send(Msg::DryRunReady {
                            generation,
                            title: format!("Confirm Delete ConsumerGroup: {group}"),
                            summary,
                            diff_lines: Vec::new(),
                            pending_action: action,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(Msg::ActionComplete {
                            generation,
                            message: format!("Dry-run delete consumer group failed: {e}"),
                            is_error: true,
                            detail_popup: None,
                            refresh: false,
                        });
                    }
                }
            }
            PendingWriteAction::ResetConnectorOffsets {
                namespace,
                connector,
            } => {
                let summary = vec![
                    format!(
                        "Reset offsets for connector '{connector}' in namespace '{namespace}'?"
                    ),
                    "Note: the connector should be STOPPED before resetting its offsets.".into(),
                    String::new(),
                    "Press 'y' or Enter to reset offsets, Esc/n to cancel.".into(),
                ];
                let _ = tx.send(Msg::DryRunReady {
                    generation,
                    title: format!("Confirm Reset Connector Offsets: {connector}"),
                    summary,
                    diff_lines: Vec::new(),
                    pending_action: action,
                });
            }
            PendingWriteAction::ChangeConnectorState {
                namespace,
                connectors,
                action: state_action,
            } => {
                let summary = vec![
                    format!(
                        "Change state to {state_action} for {} connector(s) in '{namespace}':",
                        connectors.len()
                    ),
                    format!("  {}", connectors.join(", ")),
                    String::new(),
                    "Press 'y' or Enter to confirm, Esc/n to cancel.".into(),
                ];
                let _ = tx.send(Msg::DryRunReady {
                    generation,
                    title: format!("Confirm Connector {state_action}"),
                    summary,
                    diff_lines: Vec::new(),
                    pending_action: action,
                });
            }
            PendingWriteAction::UpdateSchemaCompatibility {
                namespace,
                subject,
                compatibility,
            } => {
                let label = compatibility.as_deref().unwrap_or("GLOBAL (reset)");
                let summary = vec![
                    format!(
                        "Set schema compatibility for subject '{subject}' in '{namespace}' to {label}?"
                    ),
                    String::new(),
                    "Press 'y' or Enter to confirm, Esc/n to cancel.".into(),
                ];
                let _ = tx.send(Msg::DryRunReady {
                    generation,
                    title: format!("Confirm Schema Compatibility: {subject}"),
                    summary,
                    diff_lines: Vec::new(),
                    pending_action: action,
                });
            }
            PendingWriteAction::ResetUserPassword { namespace, user } => {
                let summary = vec![
                    format!(
                        "Reset Kafka password for user '{user}' in namespace '{namespace}'?"
                    ),
                    "The new password will be shown ONCE on screen and will never be written to logs."
                        .into(),
                    String::new(),
                    "Press 'y' or Enter to reset password, Esc/n to cancel.".into(),
                ];
                let _ = tx.send(Msg::DryRunReady {
                    generation,
                    title: format!("Confirm Reset Kafka Password: {user}"),
                    summary,
                    diff_lines: Vec::new(),
                    pending_action: action,
                });
            }
        }
    });
}

type ActionOutcome = Result<(String, Option<(String, Vec<String>)>), String>;

/// Execute a confirmed `PendingWriteAction` against the server (`dryrun = false`).
pub fn spawn_execute_action(
    generation: u64,
    client: Arc<dyn Ns4KafkaClient>,
    action: PendingWriteAction,
    tx: mpsc::UnboundedSender<Msg>,
) {
    tokio::spawn(async move {
        let outcome: ActionOutcome = match action {
            PendingWriteAction::Apply {
                namespace,
                resource,
                body,
            } => {
                let name = body
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or("<resource>")
                    .to_string();
                client
                    .apply_resource(namespace.as_deref(), &resource, &body, false)
                    .await
                    .map(|res| (format!("Applied {} '{}' ({})", resource.kind, name, res.status), None))
                    .map_err(|e| e.to_string())
            }
            PendingWriteAction::Delete {
                namespace,
                resource,
                names,
            } => {
                let mut deleted = 0usize;
                let mut err_msg = None;
                for name in &names {
                    match client
                        .delete_resource(namespace.as_deref(), &resource, name, false)
                        .await
                    {
                        Ok(_) => deleted += 1,
                        Err(e) => {
                            err_msg = Some(format!("Failed deleting '{name}': {e}"));
                            break;
                        }
                    }
                }
                match err_msg {
                    Some(e) => Err(e),
                    None => Ok((
                        format!("Deleted {deleted} {}(s)", resource.kind),
                        None,
                    )),
                }
            }
            PendingWriteAction::DeleteRecords { namespace, topic } => client
                .delete_records(&namespace, &topic, false)
                .await
                .map(|res| {
                    (
                        format!(
                            "Deleted records across {} partition(s) of topic '{topic}'",
                            res.len()
                        ),
                        None,
                    )
                })
                .map_err(|e| e.to_string()),
            PendingWriteAction::Import {
                namespace,
                resource,
            } => client
                .import_resources(&namespace, &resource.path, "*", false)
                .await
                .map(|res| {
                    (
                        format!(
                            "Imported {} unsynchronized {}(s) into '{namespace}'",
                            res.len(),
                            resource.kind
                        ),
                        None,
                    )
                })
                .map_err(|e| e.to_string()),
            PendingWriteAction::ResetConsumerGroup {
                namespace,
                group,
                topic,
                method,
                options,
            } => client
                .reset_consumer_group_offsets(
                    &namespace,
                    &group,
                    &topic,
                    &method,
                    options.as_deref(),
                    false,
                )
                .await
                .map(|res| {
                    (
                        format!(
                            "Reset offsets for group '{group}' ({} partition(s) updated)",
                            res.len()
                        ),
                        None,
                    )
                })
                .map_err(|e| e.to_string()),
            PendingWriteAction::DeleteConsumerGroup { namespace, group } => client
                .delete_consumer_group(&namespace, &group, false)
                .await
                .map(|()| (format!("Deleted consumer group '{group}'"), None))
                .map_err(|e| e.to_string()),
            PendingWriteAction::ResetConnectorOffsets {
                namespace,
                connector,
            } => client
                .reset_connector_offsets(&namespace, &connector)
                .await
                .map(|_| {
                    (
                        format!("Reset offsets for connector '{connector}'"),
                        None,
                    )
                })
                .map_err(|e| e.to_string()),
            PendingWriteAction::ChangeConnectorState {
                namespace,
                connectors,
                action,
            } => {
                let mut ok = 0usize;
                let mut last_err = None;
                for c in &connectors {
                    match client.change_connector_state(&namespace, c, &action).await {
                        Ok(_) => ok += 1,
                        Err(e) => last_err = Some(e.to_string()),
                    }
                }
                match last_err {
                    Some(e) if ok == 0 => Err(e),
                    Some(e) => Err(format!("{action} succeeded on {ok}, failed: {e}")),
                    None => Ok((
                        format!("{action} sent to {ok} connector(s)"),
                        None,
                    )),
                }
            }
            PendingWriteAction::UpdateSchemaCompatibility {
                namespace,
                subject,
                compatibility,
            } => client
                .update_schema_compatibility(&namespace, &subject, compatibility.as_deref())
                .await
                .map(|_| {
                    let c = compatibility.as_deref().unwrap_or("GLOBAL");
                    (
                        format!("Updated schema '{subject}' compatibility to {c}"),
                        None,
                    )
                })
                .map_err(|e| e.to_string()),
            PendingWriteAction::ResetUserPassword { namespace, user } => client
                .reset_user_password(&namespace, &user)
                .await
                .map(|resp| {
                    let pwd = resp
                        .pointer("/spec/newPassword")
                        .and_then(Value::as_str)
                        .unwrap_or("<empty>");
                    let popup_lines = vec![
                        format!("Kafka User: {user}"),
                        format!("Namespace:  {namespace}"),
                        String::new(),
                        format!("New Password: {pwd}"),
                        String::new(),
                        "IMPORTANT: Copy this password now. It is shown only once and is never logged."
                            .into(),
                    ];
                    (
                        format!("Reset password for Kafka user '{user}' (shown once)"),
                        Some((format!("New Password for {user}"), popup_lines)),
                    )
                })
                .map_err(|e| e.to_string()),
        };

        match outcome {
            Ok((msg, popup)) => {
                crate::applog::info("action_complete", &[("msg", &msg)]);
                let _ = tx.send(Msg::ActionComplete {
                    generation,
                    message: msg,
                    is_error: false,
                    detail_popup: popup,
                    refresh: true,
                });
            }
            Err(err) => {
                crate::applog::warn("action_error", &[("err", &err)]);
                let _ = tx.send(Msg::ActionComplete {
                    generation,
                    message: err,
                    is_error: true,
                    detail_popup: None,
                    refresh: false,
                });
            }
        }
    });
}
