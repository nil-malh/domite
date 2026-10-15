//! End-to-end state-machine and UI snapshot tests driven through `FakeClient`
//! and `App::handle_key` / `App::handle_msg` without requiring a live cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde_json::json;
use tokio::sync::{mpsc, watch};

use crate::api::client::{FakeClient, Ns4KafkaClient};
use crate::api::config::{KafkaContext, LoadedConfig};
use crate::api::poller::{PollTarget, poll_once};
use crate::app::{App, EditorRequest, FindingSeverity, Mode};
use crate::store::Msg;
use crate::ui::render_snapshot;

struct TestHarness {
    app: App,
    msg_tx: mpsc::UnboundedSender<Msg>,
    msg_rx: mpsc::UnboundedReceiver<Msg>,
    poll_rx: watch::Receiver<Option<PollTarget>>,
    prev_poll: HashMap<String, serde_json::Value>,
    poll_gen: u64,
}

impl TestHarness {
    async fn new(resource: Option<&str>) -> Self {
        let client: Arc<dyn Ns4KafkaClient> = Arc::new(FakeClient::demo(None));
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        let (poll_tx, poll_rx) = watch::channel(None);
        let (force_tx, _force_rx) = mpsc::unbounded_channel();
        let app = App::new(
            LoadedConfig::default(),
            client,
            resource,
            None,
            None,
            msg_tx.clone(),
            poll_tx,
            force_tx,
        );
        let mut h = Self {
            app,
            msg_tx,
            msg_rx,
            poll_rx,
            prev_poll: HashMap::new(),
            poll_gen: 0,
        };
        h.sync_poll().await;
        h
    }

    async fn sync_poll(&mut self) {
        if let Some(target) = self.poll_rx.borrow().clone() {
            let mut first = if target.generation != self.poll_gen {
                self.prev_poll.clear();
                self.poll_gen = target.generation;
                true
            } else {
                false
            };
            poll_once(&target, &mut self.prev_poll, &mut first, &self.msg_tx).await;
        }
        self.drain_msgs().await;
    }

    async fn drain_msgs(&mut self) {
        // Yield briefly to let spawned dry-run/action tasks complete
        tokio::time::sleep(Duration::from_millis(15)).await;
        let mut batch = Vec::new();
        while let Ok(msg) = self.msg_rx.try_recv() {
            batch.push(msg);
        }
        self.app.handle_msgs(batch);
    }

    fn press(&mut self, code: KeyCode) {
        self.app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn press_ctrl(&mut self, c: char) {
        self.app
            .handle_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL));
    }

    fn type_str(&mut self, s: &str) {
        for c in s.chars() {
            self.press(KeyCode::Char(c));
        }
    }
}

#[tokio::test]
async fn renders_curated_columns_for_all_ns4kafka_kinds() {
    for (alias, expected_kind, expected_header) in [
        ("topics", "Topic", "PARTITIONS"),
        ("co", "Connector", "CONNECT-CLUSTER"),
        ("cg", "ConsumerGroup", "LAG"),
        ("sc", "Schema", "COMPATIBILITY"),
        ("ac", "AccessControlEntry", "PERMISSION"),
        ("st", "KafkaStream", "NAME"),
        ("cc", "ConnectCluster", "URL"),
        ("qu", "ResourceQuota", "USED"),
        ("rb", "RoleBinding", "VERBS"),
        ("ns", "Namespace", "KAFKA-USER"),
    ] {
        let h = TestHarness::new(Some(alias)).await;
        assert_eq!(h.app.active_resource.kind, expected_kind);
        assert!(
            !h.app.rows.is_empty(),
            "expected demo rows for kind {expected_kind}"
        );
        let snap = render_snapshot(&h.app, 120, 28);
        assert!(
            snap.contains(expected_header),
            "snapshot for {expected_kind} missing header {expected_header}:\n{snap}"
        );
    }
}

#[tokio::test]
async fn command_palette_and_filter_mode_narrow_rows() {
    let mut h = TestHarness::new(Some("topics")).await;
    assert_eq!(h.app.rows.len(), 3);
    assert_eq!(h.app.rows[0].name, "orders.changelog.v1");

    // Open `/` filter and filter by `status=Fail`
    h.press(KeyCode::Char('/'));
    assert!(matches!(h.app.mode, Mode::Filter { .. }));
    h.type_str("status=Fail");
    h.press(KeyCode::Enter);
    assert!(matches!(h.app.mode, Mode::Table));
    assert_eq!(h.app.rows.len(), 1);
    assert_eq!(h.app.rows[0].name, "orders.dlq.v1");

    // Esc clears filter
    h.press(KeyCode::Esc);
    assert_eq!(h.app.rows.len(), 3);

    // Open `:` command palette and switch to connectors (`co`)
    h.press(KeyCode::Char(':'));
    assert!(matches!(h.app.mode, Mode::Command { .. }));
    h.type_str("co");
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "Connector");
    assert_eq!(h.app.rows.len(), 2);
}

#[tokio::test]
async fn drill_down_and_history_navigation_restore_cursor() {
    let mut h = TestHarness::new(Some("topics")).await;
    // Move down to `orders.created.v1` (index 1)
    h.press(KeyCode::Char('j'));
    assert_eq!(h.app.selected_row().unwrap().name, "orders.created.v1");

    // Press Enter -> drills down to ConsumerGroup filtered by `topic=orders.created.v1`
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroup");
    assert_eq!(h.app.filter_text, "topic=orders.created.v1");
    assert_eq!(h.app.rows.len(), 2);

    // Press `[` -> navigates back to Topic with cursor restored on `orders.created.v1`
    h.press(KeyCode::Char('['));
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "Topic");
    assert_eq!(h.app.selected_row().unwrap().name, "orders.created.v1");

    // Press `]` -> navigates forward to ConsumerGroup
    h.press(KeyCode::Char(']'));
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroup");
}

#[tokio::test]
async fn explain_view_surfaces_connector_task_traces_and_failed_topics() {
    let mut h = TestHarness::new(Some("connectors")).await;
    // Select `orders.s3-archive` (index 1, FAILED)
    h.press(KeyCode::Char('j'));
    assert_eq!(h.app.selected_row().unwrap().name, "orders.s3-archive");

    // Press `X` -> opens Explain view with failed task stack trace
    h.press(KeyCode::Char('X'));
    match &h.app.mode {
        Mode::Explain {
            title, findings, ..
        } => {
            assert!(title.contains("orders.s3-archive"));
            assert!(findings.iter().any(|f| f.severity == FindingSeverity::Error
                && f.detail.iter().any(|d| d.contains("403 AccessDenied"))));
        }
        other => panic!("expected Mode::Explain, got {other:?}"),
    }
    let snap = render_snapshot(&h.app, 120, 28);
    assert!(snap.contains("[FAIL]"));
    assert!(snap.contains("403 AccessDenied"));
}

#[tokio::test]
async fn schema_enter_opens_pretty_printed_definition_and_y_opens_yaml() {
    let mut h = TestHarness::new(Some("schemas")).await;
    // Wait briefly for background schema hydration task to complete
    h.drain_msgs().await;
    assert_eq!(h.app.active_resource.kind, "Schema");
    assert_eq!(h.app.rows.len(), 2);
    assert_eq!(h.app.rows[0].cells[1], "3"); // VERSION
    assert_eq!(h.app.rows[0].cells[2], "1042"); // ID
    assert_eq!(h.app.rows[0].cells[3], "BACKWARD"); // COMPATIBILITY

    // Press Enter -> opens pretty-printed schema definition
    h.press(KeyCode::Enter);
    match &h.app.mode {
        Mode::Detail { title, lines, .. } => {
            assert!(title.contains("Schema Definition (AVRO v3): orders.created.v1-value"));
            let text = lines.join("\n");
            assert!(text.contains("# Schema ID:     1042"));
            assert!(text.contains("\"name\": \"OrderCreated\""));
        }
        other => panic!("expected Mode::Detail on Schema Enter, got {other:?}"),
    }

    // Back to table, press `y` -> opens full ns4kafka YAML resource
    h.press(KeyCode::Esc);
    h.press(KeyCode::Char('y'));
    match &h.app.mode {
        Mode::Detail { title, lines, .. } => {
            assert!(title.contains("Schema YAML: orders.created.v1-value"));
            let text = lines.join("\n");
            assert!(text.contains("kind: Schema"));
            assert!(text.contains("compatibility: BACKWARD"));
        }
        other => panic!("expected Mode::Detail on Schema y, got {other:?}"),
    }
}

#[tokio::test]
async fn yaml_view_redacts_connect_cluster_secrets() {
    let mut h = TestHarness::new(Some("cc")).await;
    assert_eq!(h.app.active_resource.kind, "ConnectCluster");
    h.press(KeyCode::Char('y'));
    match &h.app.mode {
        Mode::Detail { lines, .. } => {
            let text = lines.join("\n");
            assert!(text.contains("https://connect-eu-west.internal:8083"));
            assert!(!text.contains("demo-key"));
            assert!(text.contains(crate::redact::REDACTED));
        }
        other => panic!("expected Mode::Detail, got {other:?}"),
    }
}

#[tokio::test]
async fn readonly_guardrail_blocks_writes_until_write_command() {
    let mut h = TestHarness::new(Some("topics")).await;
    // Enable readonly via `:readonly`
    h.app.execute_command("readonly");
    assert!(h.app.is_readonly());

    // Attempting delete (`Ctrl-d`) or edit (`e`) is blocked with flash warning
    h.press_ctrl('d');
    assert!(matches!(h.app.mode, Mode::Table));
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(msg, is_err)| *is_err && msg.contains("read-only"))
    );

    // Enable write via `:write`
    h.app.execute_command("write");
    assert!(!h.app.is_readonly());

    // Now `Ctrl-d` runs dry-run preview and opens Diff/Confirm modal
    h.press_ctrl('d');
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Diff { .. }));

    // Confirm with `y` -> deletes the topic
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    h.sync_poll().await;
    assert_eq!(h.app.rows.len(), 2);
}

#[tokio::test]
async fn edit_yaml_produces_dry_run_diff_and_applies_on_confirm() {
    let mut h = TestHarness::new(Some("topics")).await;
    let orig = h.app.selected_object().unwrap().clone();
    let req = EditorRequest {
        resource: h.app.active_resource.clone(),
        namespace: h.app.active_namespace.clone(),
        original: orig.clone(),
        yaml: serde_yaml::to_string(&orig).unwrap(),
    };

    let mut modified = orig;
    modified["spec"]["partitions"] = json!(24);
    let new_yaml = serde_yaml::to_string(&modified).unwrap();

    h.app.submit_edited_yaml(req, &new_yaml);
    h.drain_msgs().await;

    match &h.app.mode {
        Mode::Diff { diff_lines, .. } => {
            let joined = diff_lines.join("\n");
            assert!(joined.contains("+  partitions: 24"));
        }
        other => panic!("expected Mode::Diff after dry-run edit, got {other:?}"),
    }

    // Confirm with Enter
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    h.sync_poll().await;

    let updated = h.app.selected_object().unwrap();
    assert_eq!(updated.pointer("/spec/partitions"), Some(&json!(24)));
}

#[tokio::test]
async fn bulk_select_and_action_menu_on_connectors_and_imports() {
    let mut h = TestHarness::new(Some("connectors")).await;
    assert_eq!(h.app.rows.len(), 2);

    // Mark both connectors with Space
    h.press(KeyCode::Char(' '));
    h.press(KeyCode::Char(' '));
    assert_eq!(h.app.marked.len(), 2);

    // Open `t` action menu -> first option is "Restart Connector" for 2 selected connectors
    h.press(KeyCode::Char('t'));
    assert!(matches!(h.app.mode, Mode::ActionMenu { .. }));
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));

    // Confirm restart -> both connectors become RUNNING
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    h.sync_poll().await;
    for r in &h.app.rows {
        assert_eq!(r.status, "RUNNING");
    }

    // Switch to topics and import unsynchronized topics (`i`)
    h.app.execute_command("topics");
    h.sync_poll().await;
    assert_eq!(h.app.rows.len(), 3);
    h.press(KeyCode::Char('i'));
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Diff { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    h.sync_poll().await;
    assert_eq!(h.app.rows.len(), 4);
}

#[tokio::test]
async fn graceful_403_handling_for_unauthorized_namespace() {
    let mut h = TestHarness::new(Some("topics")).await;
    let restricted = KafkaContext {
        name: "clermont-dev".into(),
        api: "https://ns4kafka.clermont.example.com".into(),
        user_token: "glpat-demo".into(),
        username: "michelin-dev".into(),
        namespace: "restricted-audit".into(),
        readonly: false,
    };
    h.app
        .switch_context(Arc::new(FakeClient::demo(Some(restricted))));
    h.sync_poll().await;

    assert!(h.app.forbidden_namespaces.contains("restricted-audit"));
    let snap = render_snapshot(&h.app, 120, 28);
    assert!(snap.contains("403 Forbidden"));
}
