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
            crate::api::cache::PersistentCache::in_memory(3600),
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

    // Press Enter -> drills down to ConsumerGroup filtered by `topics=orders.created.v1`
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    h.drain_msgs().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroup");
    assert_eq!(h.app.filter_text, "topics=orders.created.v1");
    // Both `orders.billing-worker` (Owned) and `payments.order-reader` (External) consume `orders.created.v1`
    assert_eq!(h.app.rows.len(), 2);

    // Press Enter on `orders.billing-worker` -> drills down into ConsumerGroupMember (Partition Lag view)
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroupMember");
    assert_eq!(h.app.rows.len(), 2); // partition 0 and partition 1

    // Press `[` -> navigates back to ConsumerGroup
    h.press(KeyCode::Char('['));
    h.sync_poll().await;
    h.drain_msgs().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroup");

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
async fn persistent_cache_seeds_table_on_restart_and_revalidates_in_background() {
    let dir = std::env::temp_dir().join(format!("domite-app-cache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let cache = crate::api::cache::PersistentCache::new(Some(dir.clone()), 3600);
    let client: Arc<dyn Ns4KafkaClient> = Arc::new(FakeClient::demo(None));

    // Session 1: poll schemas and hydrate them
    {
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
        let (poll_tx, poll_rx) = watch::channel(None);
        let (force_tx, _force_rx) = mpsc::unbounded_channel();
        let mut app1 = App::new(
            LoadedConfig::default(),
            client.clone(),
            cache.clone(),
            Some("schemas"),
            None,
            None,
            msg_tx.clone(),
            poll_tx,
            force_tx,
        );
        if let Some(target) = poll_rx.borrow().clone() {
            let mut prev = HashMap::new();
            let mut first = true;
            poll_once(&target, &mut prev, &mut first, &msg_tx).await;
        }
        tokio::time::sleep(Duration::from_millis(35)).await;
        let mut batch = Vec::new();
        while let Ok(m) = msg_rx.try_recv() {
            batch.push(m);
        }
        app1.handle_msgs(batch);
        assert_eq!(app1.rows.len(), 2);
        assert_eq!(app1.rows[0].cells[2], "1042");
    }

    // Allow background disk writer thread to flush JSON files
    tokio::time::sleep(Duration::from_millis(40)).await;

    // Session 2: cold restart with a fresh PersistentCache reading from the same directory;
    // BEFORE any network poll runs, App::new immediately renders the cached hydrated schemas!
    let cache2 = crate::api::cache::PersistentCache::new(Some(dir.clone()), 3600);
    let (msg_tx2, _msg_rx2) = mpsc::unbounded_channel();
    let (poll_tx2, _poll_rx2) = watch::channel(None);
    let (force_tx2, _force_rx2) = mpsc::unbounded_channel();
    let app2 = App::new(
        LoadedConfig::default(),
        client,
        cache2,
        Some("schemas"),
        None,
        None,
        msg_tx2,
        poll_tx2,
        force_tx2,
    );
    assert!(
        !app2.store.synced,
        "seeded store is marked unsynced until live poll completes"
    );
    assert_eq!(
        app2.rows.len(),
        2,
        "cached rows are immediately visible on startup"
    );
    assert_eq!(app2.rows[0].cells[1], "3");
    assert_eq!(app2.rows[0].cells[2], "1042");
    assert_eq!(app2.rows[0].cells[3], "BACKWARD");

    let _ = std::fs::remove_dir_all(&dir);
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

#[tokio::test]
async fn sort_picker_invert_sort_age_sort_and_wide_mode_toggle() {
    let mut h = TestHarness::new(Some("topics")).await;
    assert_eq!(h.app.rows.len(), 3);
    assert!(!h.app.wide);

    // Toggle wide mode (`w`) -> adds CLEANUP column
    h.press(KeyCode::Char('w'));
    assert!(h.app.wide);
    assert!(h.app.columns.iter().any(|c| c.header == "CLEANUP"));

    // Open SortPicker (`S`), navigate down to PARTITIONS (column index 1), press Enter
    h.press(KeyCode::Char('S'));
    assert!(matches!(h.app.mode, Mode::SortPicker { selected: 0 }));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    assert!(matches!(h.app.mode, Mode::Table));
    assert_eq!(h.app.sort_col, Some((1, false)));
    // Ascending by PARTITIONS: 3 (dlq), 6 (changelog), 12 (created)
    assert_eq!(h.app.rows[0].name, "orders.dlq.v1");
    assert_eq!(h.app.rows[2].name, "orders.created.v1");

    // Invert sort (`I`) -> descending by PARTITIONS: 12, 6, 3
    h.press(KeyCode::Char('I'));
    assert_eq!(h.app.sort_col, Some((1, true)));
    assert_eq!(h.app.rows[0].name, "orders.created.v1");
    assert_eq!(h.app.rows[2].name, "orders.dlq.v1");

    // Sort by AGE (`A`) -> ascending then descending on repeat
    h.press(KeyCode::Char('A'));
    let age_idx = h
        .app
        .columns
        .iter()
        .position(|c| c.header == "AGE")
        .unwrap();
    assert_eq!(h.app.sort_col, Some((age_idx, false)));
    h.press(KeyCode::Char('A'));
    assert_eq!(h.app.sort_col, Some((age_idx, true)));
}

#[tokio::test]
async fn namespace_switcher_all_namespaces_and_forbidden_guard() {
    let mut h = TestHarness::new(Some("topics")).await;
    h.app.spawn_discovery();
    h.drain_msgs().await;

    // Press `0` -> switches to all namespaces (`*`), showing all 4 topics across team-orders & team-payments
    h.press(KeyCode::Char('0'));
    h.sync_poll().await;
    assert_eq!(h.app.active_namespace.as_deref(), Some("*"));
    assert_eq!(h.app.rows.len(), 4);
    assert_eq!(h.app.columns[0].header, "NAMESPACE");

    // Open namespace switcher (`n`), filter for `team-payments`, press Enter
    h.press(KeyCode::Char('n'));
    assert!(matches!(h.app.mode, Mode::Namespaces { .. }));
    let snap = render_snapshot(&h.app, 120, 28);
    assert!(snap.contains("Switch Namespace"));
    h.type_str("payments");
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_namespace.as_deref(), Some("team-payments"));
    assert_eq!(h.app.rows.len(), 1);
    assert_eq!(h.app.rows[0].name, "payments.settled.v1");

    // Open namespace switcher (`n`), filter for `restricted-audit` (403 forbidden), press Enter -> stays blocked
    h.press(KeyCode::Char('n'));
    h.type_str("restricted");
    h.press(KeyCode::Enter);
    assert_eq!(h.app.active_namespace.as_deref(), Some("team-payments"));
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(msg, is_err)| *is_err && msg.contains("403"))
    );
}

#[tokio::test]
async fn topic_action_menu_jumps_configs_and_delete_records() {
    let mut h = TestHarness::new(Some("topics")).await;
    // Select `orders.created.v1` (index 1)
    h.press(KeyCode::Char('j'));
    assert_eq!(h.app.selected_row().unwrap().name, "orders.created.v1");

    // 1. `t` -> "Jump to ACLs" (index 1)
    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "AccessControlEntry");
    assert_eq!(h.app.filter_text, "orders.created.v1");
    assert_eq!(h.app.rows.len(), 1);

    // Navigate back (`[`) to Topic
    h.press(KeyCode::Char('['));
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "Topic");

    // 2. `t` -> "Jump to Schemas" (index 2)
    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "Schema");
    assert_eq!(h.app.filter_text, "orders.created.v1");

    // Navigate back (`[`) to Topic
    h.press(KeyCode::Char('['));
    h.sync_poll().await;

    // 3. `t` -> "Show Topic Configs" (index 3)
    h.press(KeyCode::Char('t'));
    for _ in 0..3 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    match &h.app.mode {
        Mode::Detail { title, lines, .. } => {
            assert!(title.contains("Topic Configs: orders.created.v1"));
            assert!(lines.iter().any(|l| l.contains("min.insync.replicas")));
        }
        other => panic!("expected Mode::Detail for topic configs, got {other:?}"),
    }
    h.press(KeyCode::Esc);

    // 4. `t` -> "Delete Records (truncate)" (index 4) -> Confirm modal -> execute
    h.press(KeyCode::Char('t'));
    for _ in 0..4 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    let snap = render_snapshot(&h.app, 120, 28);
    assert!(snap.contains("Confirm Delete Records"));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, err)| !*err && m.contains("Deleted records"))
    );
}

#[tokio::test]
async fn consumer_group_reset_offsets_and_delete_workflows() {
    let mut h = TestHarness::new(Some("cg")).await;
    // Let background ConsumerGroup hydrator complete so TOPICS / PARTITIONS / TOTAL-LAG / MAX-LAG are populated
    h.drain_msgs().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroup");
    // 2 Owned groups (`orders.billing-worker`, `orders.legacy-consumer`) + 1 External group (`payments.order-reader`)
    assert_eq!(h.app.rows.len(), 3);
    // `orders.billing-worker` (index 0): 2 partitions, total lag 450, max lag 450
    assert_eq!(h.app.rows[0].cells[1], "Owned");
    assert_eq!(h.app.rows[0].cells[3], "orders.created.v1");
    assert_eq!(h.app.rows[0].cells[4], "2");
    assert_eq!(h.app.rows[0].cells[5], "450");
    assert_eq!(h.app.rows[0].cells[6], "450");
    // `payments.order-reader` (index 2): External group
    assert_eq!(h.app.rows[2].cells[1], "External");
    assert_eq!(h.app.rows[2].cells[5], "20");

    // 1. Drill down (`Enter`) on `orders.billing-worker` -> opens ConsumerGroupMember partition table
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroupMember");
    assert_eq!(h.app.rows.len(), 2);
    assert_eq!(h.app.rows[1].cells[3], "1"); // PARTITION 1
    assert_eq!(h.app.rows[1].cells[6], "450"); // LAG 450

    // In ConsumerGroupMember, `t` -> "Reset Offsets -> TO_EARLIEST" (index 1, after "Jump to Topic")
    // resets `orders.created.v1:0`
    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    h.sync_poll().await;

    // Navigate back (`[`) to ConsumerGroup summary table
    h.press(KeyCode::Char('['));
    h.sync_poll().await;
    h.drain_msgs().await;
    assert_eq!(h.app.active_resource.kind, "ConsumerGroup");

    // 2. Test `BY_DURATION` offset reset (`t` -> index 5: "Reset Offsets -> BY_DURATION")
    h.press(KeyCode::Char('t'));
    for _ in 0..5 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    assert!(matches!(h.app.mode, Mode::Prompt { .. }));
    let snap = render_snapshot(&h.app, 120, 28);
    assert!(snap.contains("BY_DURATION"));
    h.type_str("PT2H");
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, err)| !*err && m.contains("Reset offsets"))
    );

    // 3. Test `X` Explain view on ConsumerGroup -> shows state, ownership, total/max lag, and per-partition breakdown
    h.press(KeyCode::Char('X'));
    match &h.app.mode {
        Mode::Explain { findings, .. } => {
            assert!(findings.len() >= 3);
            assert!(findings[0].heading.contains("Consumer Group State: STABLE"));
        }
        other => panic!("expected Mode::Explain on ConsumerGroup, got {other:?}"),
    }
    h.press(KeyCode::Esc);

    // 4. Test `:cg-ext` command -> shows only External consumer groups
    h.app.execute_command("cg-ext");
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "ExternalConsumerGroup");
    assert_eq!(h.app.rows.len(), 1);
    assert_eq!(h.app.rows[0].name, "payments.order-reader");

    // Back to `:cg` and delete `orders.billing-worker` (`t` -> last item "Delete Consumer Group")
    h.app.execute_command("cg");
    h.sync_poll().await;
    h.drain_msgs().await;
    h.press(KeyCode::Char('t'));
    for _ in 0..8 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    h.sync_poll().await;
    // `orders.billing-worker` deleted, leaving 2 groups (`orders.legacy-consumer` + `payments.order-reader`)
    assert_eq!(h.app.rows.len(), 2);
}

#[tokio::test]
async fn connector_offsets_schema_compatibility_connect_vault_and_namespace_password_reset() {
    // 1. Connector view offsets & reset offsets
    let mut h = TestHarness::new(Some("connectors")).await;
    // `t` -> "View Offsets" (index 4)
    h.press(KeyCode::Char('t'));
    for _ in 0..4 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    match &h.app.mode {
        Mode::Detail { title, lines, .. } => {
            assert!(title.contains("Connector Offsets"));
            assert!(lines.iter().any(|l| l.contains("14820")));
        }
        other => panic!("expected Mode::Detail for connector offsets, got {other:?}"),
    }
    h.press(KeyCode::Esc);

    // `t` -> "Reset Offsets" (index 5) -> Confirm -> execute
    h.press(KeyCode::Char('t'));
    for _ in 0..5 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, err)| !*err && m.contains("Reset offsets for connector"))
    );

    // 2. Schema compatibility update (`t` -> "Set Compatibility -> FULL", index 4)
    h.app.execute_command("schemas");
    h.sync_poll().await;
    h.drain_msgs().await;
    h.press(KeyCode::Char('t'));
    for _ in 0..4 {
        h.press(KeyCode::Char('j'));
    }
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, err)| !*err && m.contains("FULL"))
    );

    // 3. ConnectCluster drill-down & Vault secret encryption
    h.app.execute_command("cc");
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "ConnectCluster");
    // `t` -> "Vault Encrypt Secret" (index 1) -> Prompt -> Detail modal with encrypted payload
    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    assert!(matches!(h.app.mode, Mode::Prompt { .. }));
    h.type_str("my-super-secret-db-password");
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    match &h.app.mode {
        Mode::Detail { title, lines, .. } => {
            assert!(title.contains("Vault Secret"));
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("${aes256:connect-eu-west:"))
            );
        }
        other => panic!("expected Mode::Detail with encrypted vault output, got {other:?}"),
    }
    h.press(KeyCode::Esc);

    // Drill down (`Enter`) on ConnectCluster -> jumps to Connector filtered by `connect-cluster=connect-eu-west`
    h.press(KeyCode::Enter);
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "Connector");
    assert_eq!(h.app.filter_text, "connect-cluster=connect-eu-west");

    // 4. Namespace Kafka user password reset (`t` -> "Reset Kafka User Password", index 1)
    h.app.execute_command("namespaces");
    h.sync_poll().await;
    assert_eq!(h.app.active_resource.kind, "Namespace");
    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Confirm { .. }));
    h.press(KeyCode::Char('y'));
    h.drain_msgs().await;
    match &h.app.mode {
        Mode::Detail { title, lines, .. } => {
            assert!(title.contains("New Password"));
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("generated-kafka-scram-pwd"))
            );
        }
        other => panic!("expected Mode::Detail showing one-time password, got {other:?}"),
    }
}

#[tokio::test]
async fn skin_picker_help_overlay_and_context_switcher_modals() {
    let mut h = TestHarness::new(Some("topics")).await;

    // 1. Help overlay (`?`) -> scroll down/up -> close with `q`
    h.press(KeyCode::Char('?'));
    assert!(matches!(h.app.mode, Mode::Help { scroll: 0 }));
    let help_snap = render_snapshot(&h.app, 120, 28);
    assert!(help_snap.contains("Navigation & Views"));
    h.press(KeyCode::Char('j'));
    assert!(matches!(h.app.mode, Mode::Help { scroll: 1 }));
    h.press(KeyCode::Char('q'));
    assert!(matches!(h.app.mode, Mode::Table));

    // 2. Skin picker (`K`) -> live preview on `j`, revert on `Esc`, apply on `Enter`
    let initial_skin = h.app.active_skin.clone();
    h.press(KeyCode::Char('K'));
    assert!(matches!(h.app.mode, Mode::Skins { .. }));
    h.press(KeyCode::Char('j'));
    assert_ne!(h.app.active_skin, initial_skin);
    // Cancel (`Esc`) restores previous skin
    h.press(KeyCode::Esc);
    assert_eq!(h.app.active_skin, initial_skin);

    // Open again (`K`), move down, and accept (`Enter`)
    h.press(KeyCode::Char('K'));
    h.press(KeyCode::Char('j'));
    h.press(KeyCode::Enter);
    assert!(matches!(h.app.mode, Mode::Table));
    assert_ne!(h.app.active_skin, initial_skin);

    // Also test `:skin dracula` command
    h.app.execute_command("skin dracula");
    assert_eq!(h.app.active_skin, "dracula");

    // 3. Context switcher (`C`)
    h.press(KeyCode::Char('C'));
    assert!(matches!(h.app.mode, Mode::Contexts { .. }));
    let ctx_snap = render_snapshot(&h.app, 120, 28);
    assert!(ctx_snap.contains("Switch Context"));
    h.press(KeyCode::Esc);
    assert!(matches!(h.app.mode, Mode::Table));
}

#[tokio::test]
async fn stale_generation_messages_are_ignored_and_version_drift_warns() {
    let mut h = TestHarness::new(Some("topics")).await;
    assert_eq!(h.app.rows.len(), 3);
    let current_gen = h.app.generation;

    // 1. Send Applied, Deleted, and PollError with an older generation -> all ignored
    h.app.handle_msgs(vec![
        Msg::Applied {
            generation: current_gen - 1,
            key: "team-orders/stale.topic".into(),
            obj: json!({
                "apiVersion": "v1",
                "kind": "Topic",
                "metadata": { "name": "stale.topic", "namespace": "team-orders" }
            }),
        },
        Msg::Deleted {
            generation: current_gen - 1,
            key: "team-orders/orders.created.v1".into(),
        },
        Msg::PollError {
            generation: current_gen - 1,
            error: "stale error".into(),
            failure: crate::store::PollFailure::Network,
        },
    ]);
    assert_eq!(h.app.rows.len(), 3);
    assert!(h.app.poll_error.is_none());

    // 2. Configure min_server_version = "2.0.0" and send Discovered with server_version = "1.22.6"
    h.app.config.domite.min_server_version = Some("2.0.0".into());
    h.app.handle_msgs(vec![Msg::Discovered {
        context: h.app.active_context.clone(),
        resources: h.app.registry.resources().to_vec(),
        server_version: Some("1.22.6".into()),
        username: Some("alice@michelin.com".into()),
    }]);
    assert_eq!(h.app.server_version.as_deref(), Some("1.22.6"));
    assert_eq!(h.app.username.as_deref(), Some("alice@michelin.com"));
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, is_err)| *is_err && m.contains("older than min_server_version"))
    );
}

#[tokio::test]
async fn detail_and_diff_scrolling_line_editing_and_cancel_paths() {
    let mut h = TestHarness::new(Some("topics")).await;

    // 1. Open YAML Detail view (`y`), test scrolling (`j`, `k`, `G`, `g`, `l`, `h`, `w`) and `e` edit handoff
    h.press(KeyCode::Char('y'));
    assert!(matches!(h.app.mode, Mode::Detail { scroll: 0, .. }));
    h.press(KeyCode::Char('j'));
    assert!(matches!(h.app.mode, Mode::Detail { scroll: 1, .. }));
    h.press(KeyCode::Char('k'));
    assert!(matches!(h.app.mode, Mode::Detail { scroll: 0, .. }));
    h.press(KeyCode::Char('G'));
    h.press(KeyCode::Char('g'));
    h.press(KeyCode::Char('l'));
    h.press(KeyCode::Char('h'));
    h.press(KeyCode::Char('w'));
    assert!(matches!(
        h.app.mode,
        Mode::Detail {
            scroll: 0,
            hscroll: 0,
            wrap: true,
            ..
        }
    ));

    // Pressing `e` inside Detail view queues `pending_editor` and returns to Table
    h.press(KeyCode::Char('e'));
    assert!(matches!(h.app.mode, Mode::Table));
    let req = h
        .app
        .pending_editor
        .take()
        .expect("pending_editor should be populated");

    // 2. Submitting unchanged YAML cancels cleanly
    h.app.submit_edited_yaml(req.clone(), &req.yaml);
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, err)| !*err && m.contains("no changes"))
    );

    // 3. Submitting invalid YAML syntax surfaces an error flash
    h.app.submit_edited_yaml(req, "invalid: [yaml: {");
    assert!(
        h.app
            .flash
            .as_ref()
            .is_some_and(|(m, err)| *err && m.contains("Invalid YAML"))
    );

    // 4. Trigger delete dry-run (`Ctrl-d`) -> scroll Diff view (`j`/`k`) -> cancel with `n` (topic is NOT deleted)
    h.press_ctrl('d');
    h.drain_msgs().await;
    assert!(matches!(h.app.mode, Mode::Diff { scroll: 0, .. }));
    h.press(KeyCode::Char('j'));
    assert!(matches!(h.app.mode, Mode::Diff { scroll: 1, .. }));
    h.press(KeyCode::Char('k'));
    h.press(KeyCode::Char('n'));
    assert!(matches!(h.app.mode, Mode::Table));
    assert_eq!(
        h.app.rows.len(),
        3,
        "cancelled delete must preserve all 3 topics"
    );

    // 5. Command palette line-editing keys: Tab completion, Ctrl-w (delete word), Ctrl-u (clear line)
    h.press(KeyCode::Char(':'));
    h.type_str("conn extra");
    h.press_ctrl('w'); // deletes "extra"
    match &h.app.mode {
        Mode::Command { input, .. } => assert_eq!(input, "conn "),
        other => panic!("expected Mode::Command, got {other:?}"),
    }
    h.press_ctrl('u'); // clears line
    match &h.app.mode {
        Mode::Command { input, .. } => assert!(input.is_empty()),
        other => panic!("expected Mode::Command, got {other:?}"),
    }
    h.press(KeyCode::Right); // completes first suggestion ("topics")
    match &h.app.mode {
        Mode::Command { input, .. } => assert_eq!(input, "topics"),
        other => panic!("expected Mode::Command, got {other:?}"),
    }
    h.press(KeyCode::Esc);
}
