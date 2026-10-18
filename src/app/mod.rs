//! Application state machine (`App`), `Mode` transitions, key handling,
//! row rebuilds (filtering + typed sorting), drill-downs, and action dispatch.

pub mod actions;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use crossterm::event::{KeyCode, KeyEvent};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::api::cache::PersistentCache;
use crate::api::client::Ns4KafkaClient;
use crate::api::config::{KafkaContext, LoadedConfig};
use crate::api::discovery::{DiscoveryRegistry, ResourceDefinition};
use crate::api::poller::PollTarget;
use crate::app::actions::{
    MenuEntry, MenuOp, menu_for_resource, spawn_dry_run, spawn_execute_action,
};
use crate::columns::{
    ColumnSpec, SortKey, age_seconds, columns_for_kind, eval_column, extract_labels, row_status,
};
use crate::filter::{self, Structured};
use crate::fuzzy::Fuzzy;
use crate::keymap::{Action, KeyInput, Keymap};
use crate::store::{Msg, PendingWriteAction, PollFailure, Store};
use crate::theme;

/// Active UI mode driving both key handling and rendering.
#[derive(Debug, Clone)]
pub enum Mode {
    Table,
    Command {
        input: String,
        selected: usize,
    },
    Filter {
        input: String,
    },
    Detail {
        title: String,
        lines: Vec<String>,
        scroll: usize,
        hscroll: usize,
        wrap: bool,
        editable: Option<Value>,
    },
    Diff {
        title: String,
        summary: Vec<String>,
        diff_lines: Vec<String>,
        scroll: usize,
        pending_action: PendingWriteAction,
    },
    Confirm {
        title: String,
        summary: Vec<String>,
        pending_action: PendingWriteAction,
    },
    Prompt {
        title: String,
        prompt: String,
        input: String,
        followup: PromptFollowup,
    },
    Explain {
        title: String,
        findings: Vec<ExplainFinding>,
        scroll: usize,
        wrap: bool,
    },
    Namespaces {
        filter: String,
        selected: usize,
    },
    Contexts {
        filter: String,
        selected: usize,
    },
    SortPicker {
        selected: usize,
    },
    ActionMenu {
        title: String,
        entries: Vec<MenuEntry>,
        selected: usize,
    },
    Skins {
        selected: usize,
        previous_skin: String,
    },
    Help {
        scroll: usize,
    },
}

#[derive(Debug, Clone)]
pub enum PromptFollowup {
    ConsumerGroupReset {
        namespace: String,
        group: String,
        topic: String,
        method: String,
    },
    ConnectClusterVault {
        namespace: String,
        connect_cluster: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FindingSeverity {
    Error,
    Warning,
    Ok,
}

#[derive(Debug, Clone)]
pub struct ExplainFinding {
    pub severity: FindingSeverity,
    pub heading: String,
    pub detail: Vec<String>,
}

/// One rendered row in the table after filtering and sorting.
#[derive(Debug, Clone)]
pub struct RenderedRow {
    pub key: String,
    pub name: String,
    pub namespace: Option<String>,
    pub cells: Vec<String>,
    pub sort_keys: Vec<SortKey>,
    pub status: String,
}

/// Saved view entry on the drill-down history stack (`Enter` / `Esc` / `[` / `]`).
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub resource: ResourceDefinition,
    pub namespace: Option<String>,
    pub filter: String,
    pub selected_key: Option<String>,
    pub sort: Option<(usize, bool)>,
}

/// Request for `main.rs` to suspend the TUI and launch `$EDITOR` on `yaml`,
/// then feed the edited document back into `App::submit_edited_yaml`.
#[derive(Debug, Clone)]
pub struct EditorRequest {
    pub resource: ResourceDefinition,
    pub namespace: Option<String>,
    pub original: Value,
    pub yaml: String,
}

pub struct App {
    pub config: LoadedConfig,
    pub client: Arc<dyn Ns4KafkaClient>,
    pub cache: PersistentCache,
    pub registry: DiscoveryRegistry,
    pub store: Store,
    pub generation: u64,
    pub active_context: String,
    pub active_namespace: Option<String>,
    pub active_resource: ResourceDefinition,
    pub mode: Mode,
    pub keymap: Keymap,
    pub fuzzy: Fuzzy,
    pub filter_text: String,
    pub filter_parsed: Structured,
    pub wide: bool,
    pub sort_col: Option<(usize, bool)>,
    pub columns: Vec<ColumnSpec>,
    pub rows: Vec<RenderedRow>,
    pub cursor: usize,
    pub marked: HashSet<String>,
    /// `(namespace_name, is_forbidden_403)`
    pub namespaces: Vec<(String, bool)>,
    pub forbidden_namespaces: HashSet<String>,
    pub server_version: Option<String>,
    pub username: Option<String>,
    pub readonly_override: Option<bool>,
    pub active_skin: String,
    pub flash: Option<(String, bool)>,
    pub poll_error: Option<(String, PollFailure)>,
    pub history_back: Vec<HistoryEntry>,
    pub history_forward: Vec<HistoryEntry>,
    pub should_quit: bool,
    pub pending_editor: Option<EditorRequest>,
    pub desired_selected_key: Option<String>,
    pub cursor_hint: Arc<AtomicUsize>,
    msg_tx: mpsc::UnboundedSender<Msg>,
    poll_target_tx: watch::Sender<Option<PollTarget>>,
    force_poll_tx: mpsc::UnboundedSender<()>,
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: LoadedConfig,
        client: Arc<dyn Ns4KafkaClient>,
        cache: PersistentCache,
        initial_resource: Option<&str>,
        namespace_override: Option<String>,
        readonly_override: Option<bool>,
        msg_tx: mpsc::UnboundedSender<Msg>,
        poll_target_tx: watch::Sender<Option<PollTarget>>,
        force_poll_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        let registry = DiscoveryRegistry::builtin();
        let active_resource = initial_resource
            .and_then(|r| registry.resolve(r))
            .or_else(|| registry.find_by_kind("Topic"))
            .unwrap()
            .clone();

        let ctx = client.context();
        let active_context = ctx.name.clone();
        let active_namespace = namespace_override.or_else(|| {
            if ctx.namespace.is_empty() {
                Some("default".into())
            } else {
                Some(ctx.namespace.clone())
            }
        });
        let username = Some(ctx.username.clone());
        let ctx_skin = config
            .domite
            .contexts
            .get(&active_context)
            .and_then(|o| o.skin.as_deref());
        let active_skin = ctx_skin
            .or(config.domite.skin.name.as_deref())
            .map(theme::canonical_skin_name)
            .unwrap_or("catppuccin-mocha")
            .to_string();
        if let Some(s) = ctx_skin
            && let Some(p) = theme::builtin(&s.to_ascii_lowercase())
        {
            theme::set(p);
        }

        let initial_flash = if !config.warnings.is_empty() {
            Some((config.warnings.join("; "), true))
        } else {
            None
        };

        let mut namespaces = cache.load_namespaces(&active_context).unwrap_or_default();
        if let Some(ns) = &active_namespace
            && ns != "*"
            && !namespaces.iter().any(|(n, _)| n == ns)
        {
            namespaces.push((ns.clone(), false));
        }

        let mut app = Self {
            config,
            client,
            cache,
            registry,
            store: Store::default(),
            generation: 1,
            active_context,
            active_namespace,
            active_resource,
            mode: Mode::Table,
            keymap: Keymap::default(),
            fuzzy: Fuzzy::new(),
            filter_text: String::new(),
            filter_parsed: Structured::default(),
            wide: false,
            sort_col: None,
            columns: Vec::new(),
            rows: Vec::new(),
            cursor: 0,
            marked: HashSet::new(),
            namespaces,
            forbidden_namespaces: HashSet::new(),
            server_version: None,
            username,
            readonly_override,
            active_skin,
            flash: initial_flash,
            poll_error: None,
            history_back: Vec::new(),
            history_forward: Vec::new(),
            should_quit: false,
            pending_editor: None,
            desired_selected_key: None,
            cursor_hint: Arc::new(AtomicUsize::new(0)),
            msg_tx,
            poll_target_tx,
            force_poll_tx,
        };
        app.seed_store_from_cache();
        app.apply_default_view_sort();
        app.rebuild_rows();
        app.update_poller_target();
        app
    }

    /// Seed `self.store` from the persistent cache for the active `(context, namespace, resource)`
    /// so rows appear immediately while the background poller revalidates.
    fn seed_store_from_cache(&mut self) {
        if let Some(seeded) = self.cache.load_view_items(
            &self.active_context,
            self.active_namespace.as_deref(),
            &self.active_resource.path,
        ) {
            self.store.seed(seeded);
        } else {
            self.store.clear();
        }
    }

    /// Effective read-only status for the active context.
    pub fn is_readonly(&self) -> bool {
        if let Some(ro) = self.readonly_override {
            return ro;
        }
        self.client.context().readonly
    }

    pub fn all_namespaces(&self) -> bool {
        self.active_namespace.as_deref().is_none_or(|ns| ns == "*")
    }

    pub fn demo_or_fast_hydrate(&self) -> bool {
        matches!(
            self.active_resource.kind.as_str(),
            "Schema" | "ConsumerGroup"
        )
    }

    /// Spawn initial discovery (`/api-resources`, `/api/app-info/version`, `/token_info`, `/api/namespaces`).
    pub fn spawn_discovery(&self) {
        let client = self.client.clone();
        let tx = self.msg_tx.clone();
        let ctx_name = self.active_context.clone();
        tokio::spawn(async move {
            let version = client.server_version().await.ok();
            let username = client.token_info().await.ok().map(|t| t.username);
            if let Ok(resources) = client.api_resources().await {
                let _ = tx.send(Msg::Discovered {
                    context: ctx_name.clone(),
                    resources,
                    server_version: version,
                    username,
                });
            }
            if let Ok(ns_list) = client.list_namespaces().await {
                let _ = tx.send(Msg::NamespacesLoaded {
                    context: ctx_name,
                    namespaces: ns_list,
                });
            }
        });
    }

    pub fn update_poller_target(&self) {
        self.cursor_hint.store(self.cursor, Ordering::Relaxed);
        let interval = Duration::from_secs(self.config.domite.poll_interval_secs.max(1));
        let target = PollTarget {
            generation: self.generation,
            client: self.client.clone(),
            namespace: self.active_namespace.clone(),
            resource: self.active_resource.clone(),
            interval,
            cursor_hint: self.cursor_hint.clone(),
            cache: self.cache.clone(),
        };
        let _ = self.poll_target_tx.send(Some(target));
    }

    pub fn force_refresh(&mut self) {
        self.flash = Some(("Refreshing…".into(), false));
        let _ = self.force_poll_tx.send(());
    }

    /// Process a batch of background `Msg`s, rebuilding the table at most once.
    pub fn handle_msgs(&mut self, msgs: impl IntoIterator<Item = Msg>) {
        let mut dirty = false;
        for msg in msgs {
            dirty |= self.apply_msg(msg);
        }
        if dirty {
            self.rebuild_rows();
        }
    }

    fn apply_msg(&mut self, msg: Msg) -> bool {
        match msg {
            Msg::Reset { generation } => {
                if generation == self.generation {
                    self.store.begin_reset();
                    return true;
                }
            }
            Msg::Applied {
                generation,
                key,
                obj,
            } => {
                if generation == self.generation {
                    self.store.apply(key, obj);
                    self.poll_error = None;
                    return true;
                }
            }
            Msg::Deleted { generation, key } => {
                if generation == self.generation {
                    self.store.remove(&key);
                    self.marked.remove(&key);
                    return true;
                }
            }
            Msg::Synced { generation } => {
                if generation == self.generation {
                    self.store.finish_sync();
                    self.poll_error = None;
                    return true;
                }
            }
            Msg::PollError {
                generation,
                error,
                failure,
            } => {
                if generation == self.generation {
                    if failure == PollFailure::Forbidden
                        && let Some(ns) = &self.active_namespace
                    {
                        self.forbidden_namespaces.insert(ns.clone());
                        for (n, is_forb) in &mut self.namespaces {
                            if n == ns {
                                *is_forb = true;
                            }
                        }
                    }
                    self.poll_error = Some((error, failure));
                }
            }
            Msg::Discovered {
                context,
                resources,
                server_version,
                username,
            } => {
                if context == self.active_context {
                    self.registry = DiscoveryRegistry::from_server_list(resources);
                    if let Some(v) = server_version {
                        self.check_version_drift(&v);
                        self.server_version = Some(v);
                    }
                    if let Some(u) = username {
                        self.username = Some(u);
                    }
                }
            }
            Msg::NamespacesLoaded {
                context,
                namespaces,
            } => {
                if context == self.active_context {
                    let mut merged = namespaces;
                    if let Some(cur) = &self.active_namespace
                        && cur != "*"
                        && !merged.iter().any(|(n, _)| n == cur)
                    {
                        merged.push((cur.clone(), self.forbidden_namespaces.contains(cur)));
                    }
                    for (n, is_forb) in &mut merged {
                        if self.forbidden_namespaces.contains(n) {
                            *is_forb = true;
                        }
                    }
                    merged.sort_by(|a, b| a.0.cmp(&b.0));
                    self.cache.save_namespaces(&self.active_context, &merged);
                    self.namespaces = merged;
                }
            }
            Msg::DryRunReady {
                generation,
                title,
                summary,
                diff_lines,
                pending_action,
            } => {
                if generation == self.generation {
                    if diff_lines.is_empty() {
                        self.mode = Mode::Confirm {
                            title,
                            summary,
                            pending_action,
                        };
                    } else {
                        self.mode = Mode::Diff {
                            title,
                            summary,
                            diff_lines,
                            scroll: 0,
                            pending_action,
                        };
                    }
                }
            }
            Msg::ActionComplete {
                generation,
                message,
                is_error,
                detail_popup,
                refresh,
            } => {
                if generation == self.generation {
                    self.flash = Some((message, is_error));
                    if let Some((title, lines)) = detail_popup {
                        self.mode = Mode::Detail {
                            title,
                            lines,
                            scroll: 0,
                            hscroll: 0,
                            wrap: true,
                            editable: None,
                        };
                    } else if matches!(self.mode, Mode::Confirm { .. } | Mode::Diff { .. }) {
                        self.mode = Mode::Table;
                    }
                    if refresh {
                        let _ = self.force_poll_tx.send(());
                    }
                }
            }
        }
        false
    }

    fn check_version_drift(&mut self, server_ver: &str) {
        if let Some(min_ver) = &self.config.domite.min_server_version
            && let (Ok(actual), Ok(min)) = (
                semver::Version::parse(server_ver.trim_start_matches('v')),
                semver::Version::parse(min_ver.trim_start_matches('v')),
            )
            && actual < min
        {
            self.flash = Some((
                format!(
                    "Warning: ns4kafka server version {actual} is older than min_server_version {min}"
                ),
                true,
            ));
        }
    }

    /// Rebuild `self.columns` and `self.rows` from `self.store`, applying the active
    /// `/` filter (`Structured`) and column sort (`self.sort_col`).
    pub fn rebuild_rows(&mut self) {
        let selected_key = self.desired_selected_key.clone().or_else(|| {
            if self.store.synced {
                self.rows.get(self.cursor).map(|r| r.key.clone())
            } else {
                None
            }
        });

        self.columns = columns_for_kind(
            &self.active_resource.kind,
            &self.active_resource.path,
            self.active_resource.namespaced,
            self.all_namespaces(),
            self.wide,
            &self.config.domite.views,
        );

        let headers: Vec<String> = self.columns.iter().map(|c| c.header.clone()).collect();
        let now = Utc::now();
        let mut out = Vec::with_capacity(self.store.len());

        for (key, obj) in self.store.iter() {
            let name = obj
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>")
                .to_string();
            let ns = obj
                .pointer("/metadata/namespace")
                .and_then(Value::as_str)
                .map(str::to_string);

            let mut cells = Vec::with_capacity(self.columns.len());
            let mut sort_keys = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                let (cell, sk) = eval_column(col, obj, now);
                cells.push(cell);
                sort_keys.push(sk);
            }

            if !self.filter_parsed.terms.is_empty() {
                let labels = extract_labels(obj);
                let age = age_seconds(obj, now);
                if !self.filter_parsed.matches_row(
                    ns.as_deref(),
                    &name,
                    &headers,
                    &cells,
                    &labels,
                    age,
                    &self.fuzzy,
                ) {
                    continue;
                }
            }

            out.push(RenderedRow {
                key: key.to_string(),
                name,
                namespace: ns,
                cells,
                sort_keys,
                status: row_status(obj),
            });
        }

        if let Some((col_idx, desc)) = self.sort_col
            && col_idx < self.columns.len()
        {
            out.sort_by(|a, b| {
                let ord = a.sort_keys[col_idx]
                    .cmp(&b.sort_keys[col_idx])
                    .then_with(|| a.name.cmp(&b.name));
                if desc { ord.reverse() } else { ord }
            });
        } else {
            out.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.key.cmp(&b.key)));
        }

        self.rows = out;
        if let Some(prev_k) = selected_key
            && let Some(pos) = self.rows.iter().position(|r| r.key == prev_k)
        {
            self.cursor = pos;
            if self.store.synced {
                self.desired_selected_key = None;
            }
        } else if self.rows.is_empty() {
            self.cursor = 0;
        } else if self.cursor >= self.rows.len() {
            self.cursor = self.rows.len() - 1;
        }
        self.cursor_hint.store(self.cursor, Ordering::Relaxed);
    }

    fn apply_default_view_sort(&mut self) {
        let cols = columns_for_kind(
            &self.active_resource.kind,
            &self.active_resource.path,
            self.active_resource.namespaced,
            self.all_namespaces(),
            self.wide,
            &self.config.domite.views,
        );
        let view_cfg = self
            .config
            .domite
            .views
            .get(&self.active_resource.kind.to_ascii_lowercase())
            .or_else(|| self.config.domite.views.get(&self.active_resource.path))
            .or_else(|| self.config.domite.views.get(&self.active_resource.kind));
        if let Some(vc) = view_cfg
            && let Some(sort_col_name) = &vc.sort
            && let Some(idx) = cols
                .iter()
                .position(|c| c.header.eq_ignore_ascii_case(sort_col_name.trim()))
        {
            self.sort_col = Some((idx, vc.sort_desc));
        }
    }

    pub fn selected_row(&self) -> Option<&RenderedRow> {
        self.rows.get(self.cursor)
    }

    pub fn selected_object(&self) -> Option<&Value> {
        self.selected_row().and_then(|r| self.store.get(&r.key))
    }

    fn active_scope(&self) -> &'static str {
        match &self.mode {
            Mode::Table => "table",
            Mode::Command { .. } => "command",
            Mode::Filter { .. } => "filter",
            Mode::Detail { .. } => "detail",
            Mode::Diff { .. } => "diff",
            Mode::Confirm { .. } => "confirm",
            Mode::Prompt { .. } => "prompt",
            Mode::Explain { .. } => "explain",
            Mode::Namespaces { .. } => "namespaces",
            Mode::Contexts { .. } => "contexts",
            Mode::SortPicker { .. } => "sort_picker",
            Mode::ActionMenu { .. } => "action_menu",
            Mode::Skins { .. } => "skins",
            Mode::Help { .. } => "help",
        }
    }

    /// Handle a terminal key event.
    pub fn handle_key(&mut self, event: KeyEvent) {
        let scope = self.active_scope();
        let input = self.keymap.resolve(scope, event);
        if input.action == Some(Action::Quit) {
            self.should_quit = true;
            return;
        }

        match self.mode.clone() {
            Mode::Table => self.handle_table_key(input),
            Mode::Command {
                mut input,
                mut selected,
            } => {
                self.handle_command_key(event, &mut input, &mut selected);
            }
            Mode::Filter { mut input } => {
                self.handle_filter_key(event, &mut input);
            }
            Mode::Detail {
                title,
                lines,
                mut scroll,
                mut hscroll,
                mut wrap,
                editable,
            } => {
                self.handle_detail_key(
                    input,
                    title,
                    lines,
                    &mut scroll,
                    &mut hscroll,
                    &mut wrap,
                    editable,
                );
            }
            Mode::Diff {
                title,
                summary,
                diff_lines,
                mut scroll,
                pending_action,
            } => {
                self.handle_diff_key(
                    input,
                    title,
                    summary,
                    diff_lines,
                    &mut scroll,
                    pending_action,
                );
            }
            Mode::Confirm { pending_action, .. } => {
                self.handle_confirm_key(input, pending_action);
            }
            Mode::Prompt {
                title,
                prompt,
                mut input,
                followup,
            } => {
                self.handle_prompt_key(event, title, prompt, &mut input, followup);
            }
            Mode::Explain {
                title,
                findings,
                mut scroll,
                mut wrap,
            } => {
                self.handle_explain_key(input, title, findings, &mut scroll, &mut wrap);
            }
            Mode::Namespaces {
                mut filter,
                mut selected,
            } => {
                self.handle_namespaces_key(event, &mut filter, &mut selected);
            }
            Mode::Contexts {
                mut filter,
                mut selected,
            } => {
                self.handle_contexts_key(event, &mut filter, &mut selected);
            }
            Mode::SortPicker { mut selected } => {
                self.handle_sort_picker_key(input, &mut selected);
            }
            Mode::ActionMenu {
                title,
                entries,
                mut selected,
            } => {
                self.handle_action_menu_key(input, title, entries, &mut selected);
            }
            Mode::Skins {
                mut selected,
                previous_skin,
            } => {
                self.handle_skins_key(input, &mut selected, previous_skin);
            }
            Mode::Help { mut scroll } => {
                self.handle_help_key(input, &mut scroll);
            }
        }
    }

    fn handle_table_key(&mut self, input: KeyInput) {
        match input.action {
            Some(Action::Exit) => self.should_quit = true,
            Some(Action::Down) => {
                if !self.rows.is_empty() {
                    self.cursor = (self.cursor + 1).min(self.rows.len() - 1);
                    self.cursor_hint.store(self.cursor, Ordering::Relaxed);
                }
            }
            Some(Action::Up) => {
                self.cursor = self.cursor.saturating_sub(1);
                self.cursor_hint.store(self.cursor, Ordering::Relaxed);
            }
            Some(Action::First) => {
                self.cursor = 0;
                self.cursor_hint.store(0, Ordering::Relaxed);
            }
            Some(Action::Last) => {
                if !self.rows.is_empty() {
                    self.cursor = self.rows.len() - 1;
                    self.cursor_hint.store(self.cursor, Ordering::Relaxed);
                }
            }
            Some(Action::PageDown) => {
                if !self.rows.is_empty() {
                    self.cursor = (self.cursor + 15).min(self.rows.len() - 1);
                    self.cursor_hint.store(self.cursor, Ordering::Relaxed);
                }
            }
            Some(Action::PageUp) => {
                self.cursor = self.cursor.saturating_sub(15);
                self.cursor_hint.store(self.cursor, Ordering::Relaxed);
            }
            Some(Action::Command) => {
                self.mode = Mode::Command {
                    input: String::new(),
                    selected: 0,
                };
            }
            Some(Action::Filter) => {
                self.mode = Mode::Filter {
                    input: self.filter_text.clone(),
                };
            }
            Some(Action::Help) => {
                self.mode = Mode::Help { scroll: 0 };
            }
            Some(Action::Namespaces) => {
                self.mode = Mode::Namespaces {
                    filter: String::new(),
                    selected: 0,
                };
            }
            Some(Action::Contexts) => {
                self.mode = Mode::Contexts {
                    filter: String::new(),
                    selected: 0,
                };
            }
            Some(Action::AllNamespaces) => {
                self.switch_namespace(Some("*".into()));
            }
            Some(Action::Wide) => {
                self.wide = !self.wide;
                self.rebuild_rows();
            }
            Some(Action::Mark) => {
                if let Some(row) = self.rows.get(self.cursor) {
                    let key = row.key.clone();
                    if !self.marked.remove(&key) {
                        self.marked.insert(key);
                    }
                    if self.cursor + 1 < self.rows.len() {
                        self.cursor += 1;
                    }
                }
            }
            Some(Action::Sort) => {
                let initial = self.sort_col.map(|(i, _)| i).unwrap_or(0);
                self.mode = Mode::SortPicker { selected: initial };
            }
            Some(Action::InvertSort) => {
                if let Some((col, desc)) = self.sort_col {
                    self.sort_col = Some((col, !desc));
                } else {
                    self.sort_col = Some((0, true));
                }
                self.rebuild_rows();
            }
            Some(Action::SortAge) => {
                if let Some(age_idx) = self.columns.iter().position(|c| c.header == "AGE") {
                    let desc = match self.sort_col {
                        Some((idx, d)) if idx == age_idx => !d,
                        _ => false,
                    };
                    self.sort_col = Some((age_idx, desc));
                    self.rebuild_rows();
                }
            }
            Some(Action::Refresh) => {
                self.force_refresh();
            }
            Some(Action::Yaml | Action::Describe) => {
                self.open_yaml_view();
            }
            Some(Action::Edit) => {
                self.request_edit();
            }
            Some(Action::Delete) => {
                self.initiate_delete();
            }
            Some(Action::Import) => {
                self.initiate_import();
            }
            Some(Action::ActionMenu) => {
                self.open_action_menu();
            }
            Some(Action::Explain) => {
                self.open_explain_view();
            }
            Some(Action::Open) => {
                self.drill_down();
            }
            Some(Action::Back | Action::HistoryBack) => {
                if !self.filter_text.is_empty() && input.action == Some(Action::Back) {
                    self.set_filter(String::new());
                } else if !self.marked.is_empty() && input.action == Some(Action::Back) {
                    self.marked.clear();
                } else {
                    self.navigate_back();
                }
            }
            Some(Action::HistoryForward) => {
                self.navigate_forward();
            }
            Some(Action::Skins) => {
                let idx = theme::BUILTIN_NAMES
                    .iter()
                    .position(|&n| n == self.active_skin)
                    .unwrap_or(0);
                self.mode = Mode::Skins {
                    selected: idx,
                    previous_skin: self.active_skin.clone(),
                };
            }
            Some(Action::CopyName) => {
                if let Some(row) = self.selected_row() {
                    self.flash = Some((format!("Selected: {}", row.name), false));
                }
            }
            _ => {}
        }
    }

    /// Returns `(hydrated_count, total_count)` when background schema or consumer-group hydration is in flight.
    pub fn hydrating_progress(&self) -> Option<(usize, usize)> {
        if !matches!(
            self.active_resource.kind.as_str(),
            "Schema" | "ConsumerGroup"
        ) || self.store.is_empty()
        {
            return None;
        }
        let total = self.store.len();
        let unhydrated = self
            .store
            .iter()
            .filter(|(_, obj)| obj.get("_hydrating").and_then(Value::as_bool) == Some(true))
            .count();
        (unhydrated > 0).then_some((total.saturating_sub(unhydrated), total))
    }

    pub fn open_yaml_view(&mut self) {
        let Some(obj) = self.selected_object().cloned() else {
            return;
        };
        if self.active_resource.kind == "Schema" && obj.get("spec").is_none_or(Value::is_null) {
            self.fetch_schema_and_open(&obj, false);
            return;
        }
        let mut display_obj = obj.clone();
        if let Some(map) = display_obj.as_object_mut() {
            map.remove("_quotaKey");
            map.remove("_quotaValue");
            map.remove("_quotaUsed");
            map.remove("_hydrating");
            map.remove("_cgOwnership");
            map.remove("_cgTopics");
            map.remove("_cgPartitions");
            map.remove("_cgTotalLag");
            map.remove("_cgMaxLag");
        }
        // Redact sensitive fields in ConnectCluster specs (`password`, `aes256Key`, `aes256Salt`)
        crate::redact::redact_json(&mut display_obj);
        let name = display_obj
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("<resource>");
        let yaml = serde_yaml::to_string(&display_obj).unwrap_or_default();
        let lines = yaml.lines().map(str::to_string).collect();
        self.mode = Mode::Detail {
            title: format!("{} YAML: {}", self.active_resource.kind, name),
            lines,
            scroll: 0,
            hscroll: 0,
            wrap: false,
            editable: self.active_resource.is_declarative().then_some(obj),
        };
    }

    pub fn open_schema_pretty_view(&mut self) {
        let Some(obj) = self.selected_object().cloned() else {
            return;
        };
        if obj.get("spec").is_none_or(Value::is_null) {
            self.fetch_schema_and_open(&obj, true);
            return;
        }
        let (title, lines) = format_schema_pretty(&obj);
        self.mode = Mode::Detail {
            title,
            lines,
            scroll: 0,
            hscroll: 0,
            wrap: false,
            editable: Some(obj),
        };
    }

    fn fetch_schema_and_open(&mut self, obj: &Value, pretty: bool) {
        let subject = obj
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if subject.is_empty() {
            return;
        }
        let ns = obj
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .or(self.active_namespace.as_deref())
            .unwrap_or(&self.client.context().namespace)
            .to_string();
        let key = crate::store::row_key(obj);
        let client = self.client.clone();
        let cache = self.cache.clone();
        let ctx_name = self.active_context.clone();
        let tx = self.msg_tx.clone();
        let r#gen = self.generation;
        self.flash = Some((
            format!("Fetching schema '{subject}' from Schema Registry…"),
            false,
        ));
        tokio::spawn(async move {
            match client.get_schema(&ns, &subject).await {
                Ok(full) => {
                    cache.put_schema(&ctx_name, key.clone(), full.clone(), true);
                    let _ = tx.send(Msg::Applied {
                        generation: r#gen,
                        key,
                        obj: full.clone(),
                    });
                    let (title, lines) = if pretty {
                        format_schema_pretty(&full)
                    } else {
                        let yaml = serde_yaml::to_string(&full).unwrap_or_default();
                        (
                            format!("Schema YAML: {subject}"),
                            yaml.lines().map(str::to_string).collect(),
                        )
                    };
                    let _ = tx.send(Msg::ActionComplete {
                        generation: r#gen,
                        message: format!("Loaded schema '{subject}'"),
                        is_error: false,
                        detail_popup: Some((title, lines)),
                        refresh: false,
                    });
                }
                Err(e) => {
                    let _ = tx.send(Msg::ActionComplete {
                        generation: r#gen,
                        message: format!("Failed to fetch schema '{subject}': {e}"),
                        is_error: true,
                        detail_popup: None,
                        refresh: false,
                    });
                }
            }
        });
    }

    pub fn request_edit(&mut self) {
        if self.guard_readonly() {
            return;
        }
        if !self.active_resource.is_declarative() {
            self.flash = Some((
                format!(
                    "{} is not a declarative resource and cannot be edited via apply",
                    self.active_resource.kind
                ),
                true,
            ));
            return;
        }
        let Some(obj) = self.selected_object().cloned() else {
            return;
        };
        let mut clean = obj.clone();
        if let Some(map) = clean.as_object_mut() {
            map.remove("_quotaKey");
            map.remove("_quotaValue");
            map.remove("_quotaUsed");
        }
        let yaml = serde_yaml::to_string(&clean).unwrap_or_default();
        self.pending_editor = Some(EditorRequest {
            resource: self.active_resource.clone(),
            namespace: self.active_namespace.clone(),
            original: clean,
            yaml,
        });
    }

    /// Called by `main.rs` after `$EDITOR` exits with updated YAML content.
    pub fn submit_edited_yaml(&mut self, req: EditorRequest, new_yaml: &str) {
        if new_yaml.trim() == req.yaml.trim() {
            self.flash = Some(("Edit cancelled (no changes)".into(), false));
            return;
        }
        match serde_yaml::from_str::<Value>(new_yaml) {
            Ok(new_val) => {
                self.flash = Some(("Running server dry-run validation…".into(), false));
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    Some(req.original),
                    PendingWriteAction::Apply {
                        namespace: req.namespace,
                        resource: req.resource,
                        body: new_val,
                    },
                    self.msg_tx.clone(),
                );
            }
            Err(e) => {
                self.flash = Some((format!("Invalid YAML: {e}"), true));
            }
        }
    }

    pub fn guard_readonly(&mut self) -> bool {
        if self.is_readonly() {
            self.flash = Some((
                format!(
                    "Context '{}' is read-only (use :write or --write to enable mutations)",
                    self.active_context
                ),
                true,
            ));
            true
        } else {
            false
        }
    }

    fn marked_or_selected_names(&self) -> Vec<String> {
        if !self.marked.is_empty() {
            let mut names: Vec<String> = self
                .rows
                .iter()
                .filter(|r| self.marked.contains(&r.key))
                .map(|r| r.name.clone())
                .collect();
            names.dedup();
            if !names.is_empty() {
                return names;
            }
        }
        self.selected_row()
            .map(|r| vec![r.name.clone()])
            .unwrap_or_default()
    }

    pub fn initiate_delete(&mut self) {
        if self.guard_readonly() {
            return;
        }
        let names = self.marked_or_selected_names();
        if names.is_empty() {
            return;
        }
        let ns = self
            .selected_row()
            .and_then(|r| r.namespace.clone())
            .or_else(|| self.active_namespace.clone());
        self.flash = Some(("Running delete dry-run preview…".into(), false));
        spawn_dry_run(
            self.generation,
            self.client.clone(),
            None,
            PendingWriteAction::Delete {
                namespace: ns,
                resource: self.active_resource.clone(),
                names,
            },
            self.msg_tx.clone(),
        );
    }

    pub fn initiate_import(&mut self) {
        if self.guard_readonly() {
            return;
        }
        if !self.active_resource.synchronizable {
            self.flash = Some((
                format!(
                    "{} does not support unsynchronized import (only Topic and Connector do)",
                    self.active_resource.kind
                ),
                true,
            ));
            return;
        }
        let Some(ns) = self.active_namespace.clone().filter(|n| n != "*") else {
            self.flash = Some(("Select a specific namespace before importing".into(), true));
            return;
        };
        self.flash = Some(("Checking unsynchronized resources (dry-run)…".into(), false));
        spawn_dry_run(
            self.generation,
            self.client.clone(),
            None,
            PendingWriteAction::Import {
                namespace: ns,
                resource: self.active_resource.clone(),
            },
            self.msg_tx.clone(),
        );
    }

    pub fn open_action_menu(&mut self) {
        let marked_names = if !self.marked.is_empty() {
            self.marked_or_selected_names()
        } else {
            Vec::new()
        };
        let entries = menu_for_resource(
            &self.active_resource.kind,
            self.selected_object(),
            &marked_names,
        );
        if entries.is_empty() {
            self.flash = Some((
                format!("No special actions for {}", self.active_resource.kind),
                false,
            ));
            return;
        }
        let target_name = self
            .selected_row()
            .map(|r| r.name.as_str())
            .unwrap_or(&self.active_resource.kind);
        self.mode = Mode::ActionMenu {
            title: format!("{} Actions: {}", self.active_resource.kind, target_name),
            entries,
            selected: 0,
        };
    }

    pub fn open_explain_view(&mut self) {
        let Some(obj) = self.selected_object().cloned() else {
            return;
        };
        let name = obj
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("<resource>");
        let kind = &self.active_resource.kind;
        let mut findings = Vec::new();

        // 1. Metadata status phase & message
        if let Some(phase) = obj
            .pointer("/metadata/status/phase")
            .and_then(Value::as_str)
        {
            let msg = obj
                .pointer("/metadata/status/message")
                .and_then(Value::as_str)
                .unwrap_or("No status message reported by ns4kafka.");
            let sev = match phase {
                "Fail" => FindingSeverity::Error,
                "Pending" | "Deleting" => FindingSeverity::Warning,
                _ => FindingSeverity::Ok,
            };
            findings.push(ExplainFinding {
                severity: sev,
                heading: format!("ns4kafka Metadata Status: {phase}"),
                detail: vec![msg.to_string()],
            });
        }

        // 2. Connector state and task traces
        if kind == "Connector" {
            let state = obj
                .pointer("/status/state")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN");
            let worker = obj
                .pointer("/status/workerId")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let sev = match state {
                "FAILED" => FindingSeverity::Error,
                "PAUSED" | "UNASSIGNED" => FindingSeverity::Warning,
                _ => FindingSeverity::Ok,
            };
            findings.push(ExplainFinding {
                severity: sev,
                heading: format!("Connector State: {state} (worker: {worker})"),
                detail: Vec::new(),
            });

            if let Some(tasks) = obj.pointer("/status/tasks").and_then(Value::as_array) {
                for task in tasks {
                    let id = task.get("id").and_then(Value::as_str).unwrap_or("?");
                    let tstate = task
                        .get("state")
                        .and_then(Value::as_str)
                        .unwrap_or("UNKNOWN");
                    let tworker = task.get("workerId").and_then(Value::as_str).unwrap_or("-");
                    let trace = task.get("trace").and_then(Value::as_str);
                    let tsev = match tstate {
                        "FAILED" => FindingSeverity::Error,
                        "PAUSED" | "UNASSIGNED" => FindingSeverity::Warning,
                        _ => FindingSeverity::Ok,
                    };
                    let detail = trace
                        .map(|t| t.lines().map(str::to_string).collect())
                        .unwrap_or_else(|| vec!["Task is operating normally.".into()]);
                    findings.push(ExplainFinding {
                        severity: tsev,
                        heading: format!("Task #{id}: {tstate} (worker: {tworker})"),
                        detail,
                    });
                }
            }
        }

        // 3. ConsumerGroup / ExternalConsumerGroup / ConsumerGroupMember lag and state
        if matches!(
            kind.as_str(),
            "ConsumerGroup" | "ExternalConsumerGroup" | "ConsumerGroupMember"
        ) {
            let state = obj
                .pointer("/status/state")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN");
            let ownership = obj
                .get("_cgOwnership")
                .and_then(Value::as_str)
                .unwrap_or("Owned");
            let total_lag = obj
                .get("_cgTotalLag")
                .or_else(|| obj.pointer("/status/lag"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let max_lag = obj
                .get("_cgMaxLag")
                .or_else(|| obj.pointer("/status/lag"))
                .and_then(Value::as_i64)
                .unwrap_or(0);

            let sev = if state == "DEAD" {
                FindingSeverity::Error
            } else if total_lag > 0 || state != "STABLE" {
                FindingSeverity::Warning
            } else {
                FindingSeverity::Ok
            };

            let reset_note = if state == "EMPTY" || state == "DEAD" {
                "Group is inactive (EMPTY/DEAD): offset reset and group deletion are allowed."
            } else {
                "Group has active consumers (STABLE/REBALANCING): stop consumers until state is EMPTY before resetting offsets or deleting."
            };

            findings.push(ExplainFinding {
                severity: sev,
                heading: format!(
                    "Consumer Group State: {state} ({ownership}, total lag: {total_lag}, max partition lag: {max_lag})"
                ),
                detail: vec![reset_note.to_string()],
            });

            if let Some(items) = obj.get("_cgItems").and_then(Value::as_array) {
                for item in items {
                    let topic = item
                        .pointer("/status/topic")
                        .and_then(Value::as_str)
                        .unwrap_or("-");
                    let part = item
                        .pointer("/status/partition")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let cur = item
                        .pointer("/status/currentOffset")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let end = item
                        .pointer("/status/logEndOffset")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let lag = item
                        .pointer("/status/lag")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let psev = if lag > 0 {
                        FindingSeverity::Warning
                    } else {
                        FindingSeverity::Ok
                    };
                    findings.push(ExplainFinding {
                        severity: psev,
                        heading: format!("Partition {topic}:{part} — Lag: {lag}"),
                        detail: vec![format!("Current Offset: {cur}  |  Log End Offset: {end}")],
                    });
                }
            } else if let Some(topic) = obj.pointer("/status/topic").and_then(Value::as_str) {
                let part = obj
                    .pointer("/status/partition")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let cur = obj
                    .pointer("/status/currentOffset")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let end = obj
                    .pointer("/status/logEndOffset")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                findings.push(ExplainFinding {
                    severity: if total_lag > 0 {
                        FindingSeverity::Warning
                    } else {
                        FindingSeverity::Ok
                    },
                    heading: format!("Partition {topic}:{part} — Lag: {total_lag}"),
                    detail: vec![format!("Current Offset: {cur}  |  Log End Offset: {end}")],
                });
            }
        }

        if findings.is_empty() {
            findings.push(ExplainFinding {
                severity: FindingSeverity::Ok,
                heading: format!("{kind} '{name}' has no reported errors"),
                detail: vec!["Resource is healthy.".into()],
            });
        }

        self.mode = Mode::Explain {
            title: format!("Explain {kind}: {name}"),
            findings,
            scroll: 0,
            wrap: true,
        };
    }

    /// Drill down from the selected row (`Enter` in `Mode::Table`).
    pub fn drill_down(&mut self) {
        let Some(row) = self.selected_row().cloned() else {
            return;
        };
        match self.active_resource.kind.as_str() {
            "Namespace" => {
                let target_ns = row.name.clone();
                if self.forbidden_namespaces.contains(&target_ns) {
                    self.flash =
                        Some((format!("Namespace '{target_ns}' is forbidden (403)"), true));
                    return;
                }
                if let Some(topic_def) = self.registry.find_by_kind("Topic").cloned() {
                    self.navigate_to(topic_def, Some(target_ns), String::new(), true);
                }
            }
            "Topic" => {
                // Drill down from Topic -> ConsumerGroup filtered by topic name
                if let Some(cg_def) = self.registry.find_by_kind("ConsumerGroup").cloned() {
                    let filter = format!("topics={}", row.name);
                    self.navigate_to(cg_def, self.active_namespace.clone(), filter, true);
                }
            }
            "ConsumerGroup" | "ExternalConsumerGroup" => {
                let ownership = self
                    .selected_object()
                    .and_then(|v| v.get("_cgOwnership"))
                    .and_then(Value::as_str)
                    .unwrap_or("Owned")
                    .to_string();
                let member_def =
                    crate::api::discovery::consumer_group_member_definition(&row.name, &ownership);
                self.navigate_to(
                    member_def,
                    self.active_namespace.clone(),
                    String::new(),
                    true,
                );
            }
            "ConsumerGroupMember" => {
                self.open_explain_view();
            }
            "ConnectCluster" => {
                // Drill down from ConnectCluster -> Connector filtered by connect-cluster
                if let Some(conn_def) = self.registry.find_by_kind("Connector").cloned() {
                    let filter = format!("connect-cluster={}", row.name);
                    self.navigate_to(conn_def, self.active_namespace.clone(), filter, true);
                }
            }
            "Connector" => {
                if row.status.eq_ignore_ascii_case("FAILED")
                    || row.status.eq_ignore_ascii_case("Fail")
                {
                    self.open_explain_view();
                } else {
                    self.open_yaml_view();
                }
            }
            "Schema" => {
                self.open_schema_pretty_view();
            }
            _ => {
                self.open_yaml_view();
            }
        }
    }

    pub fn navigate_to(
        &mut self,
        resource: ResourceDefinition,
        namespace: Option<String>,
        filter: String,
        record_history: bool,
    ) {
        if record_history {
            self.history_back.push(HistoryEntry {
                resource: self.active_resource.clone(),
                namespace: self.active_namespace.clone(),
                filter: self.filter_text.clone(),
                selected_key: self.selected_row().map(|r| r.key.clone()),
                sort: self.sort_col,
            });
            self.history_forward.clear();
        }
        self.active_resource = resource;
        self.active_namespace = namespace;
        self.filter_text = filter.clone();
        self.filter_parsed = filter::parse(&filter);
        self.sort_col = None;
        self.apply_default_view_sort();
        self.cursor = 0;
        self.desired_selected_key = None;
        self.marked.clear();
        self.poll_error = None;
        self.generation += 1;
        self.seed_store_from_cache();
        self.rebuild_rows();
        self.update_poller_target();
    }

    pub fn navigate_back(&mut self) {
        let Some(entry) = self.history_back.pop() else {
            return;
        };
        self.history_forward.push(HistoryEntry {
            resource: self.active_resource.clone(),
            namespace: self.active_namespace.clone(),
            filter: self.filter_text.clone(),
            selected_key: self.selected_row().map(|r| r.key.clone()),
            sort: self.sort_col,
        });
        self.restore_history_entry(entry);
    }

    pub fn navigate_forward(&mut self) {
        let Some(entry) = self.history_forward.pop() else {
            return;
        };
        self.history_back.push(HistoryEntry {
            resource: self.active_resource.clone(),
            namespace: self.active_namespace.clone(),
            filter: self.filter_text.clone(),
            selected_key: self.selected_row().map(|r| r.key.clone()),
            sort: self.sort_col,
        });
        self.restore_history_entry(entry);
    }

    fn restore_history_entry(&mut self, entry: HistoryEntry) {
        self.active_resource = entry.resource;
        self.active_namespace = entry.namespace;
        self.filter_text = entry.filter.clone();
        self.filter_parsed = filter::parse(&entry.filter);
        self.sort_col = entry.sort;
        self.cursor = 0;
        self.desired_selected_key = entry.selected_key;
        self.marked.clear();
        self.poll_error = None;
        self.generation += 1;
        self.seed_store_from_cache();
        self.rebuild_rows();
        self.update_poller_target();
    }

    pub fn switch_namespace(&mut self, ns: Option<String>) {
        if let Some(target) = &ns
            && self.forbidden_namespaces.contains(target)
        {
            self.flash = Some((
                format!("Access to namespace '{target}' is forbidden (403)"),
                true,
            ));
            return;
        }
        let def = self.active_resource.clone();
        let filt = self.filter_text.clone();
        self.navigate_to(def, ns, filt, true);
    }

    pub fn switch_context(&mut self, new_client: Arc<dyn Ns4KafkaClient>) {
        let ctx = new_client.context().clone();
        self.client = new_client;
        self.active_context = ctx.name.clone();
        if let Some(ov_skin) = self
            .config
            .domite
            .contexts
            .get(&ctx.name)
            .and_then(|o| o.skin.as_deref())
            .or(self.config.domite.skin.name.as_deref())
            && let Some(p) = theme::builtin(&ov_skin.to_ascii_lowercase())
        {
            theme::set(p);
            self.active_skin = theme::canonical_skin_name(ov_skin).to_string();
        }
        self.active_namespace = Some(ctx.namespace.clone());
        self.username = Some(ctx.username.clone());
        self.forbidden_namespaces.clear();
        let mut cached_ns = self.cache.load_namespaces(&ctx.name).unwrap_or_default();
        if !cached_ns.iter().any(|(n, _)| n == &ctx.namespace) {
            cached_ns.push((ctx.namespace.clone(), false));
        }
        self.namespaces = cached_ns;
        self.history_back.clear();
        self.history_forward.clear();
        self.marked.clear();
        self.poll_error = None;
        self.generation += 1;
        self.seed_store_from_cache();
        self.mode = Mode::Table;
        self.flash = Some((
            format!(
                "Switched to context '{}' ({})",
                ctx.name,
                if self.is_readonly() {
                    "read-only"
                } else {
                    "writable"
                }
            ),
            false,
        ));
        self.rebuild_rows();
        self.spawn_discovery();
        self.update_poller_target();
    }

    pub fn set_filter(&mut self, input: String) {
        self.filter_text = input.clone();
        self.filter_parsed = filter::parse(&input);
        self.rebuild_rows();
    }

    // -----------------------------------------------------------------------
    // Command palette (`:`)
    // -----------------------------------------------------------------------

    /// Suggestions for the `:` command palette matching `query`.
    pub fn command_suggestions(&self, query: &str) -> Vec<(String, String)> {
        let q = query.trim();
        let mut all: Vec<(String, String, i64)> = Vec::new();

        for res in self.registry.resources() {
            let aliases = res.names.join(", ");
            let label = res.path.clone();
            let desc = format!("{} ({})", res.kind, aliases);
            if q.is_empty() {
                all.push((label, desc, 0));
            } else if let Some(score) = self
                .fuzzy
                .score(&label, q)
                .or_else(|| self.fuzzy.score(&res.kind, q))
                .or_else(|| res.names.iter().find_map(|n| self.fuzzy.score(n, q)))
            {
                all.push((label, desc, score));
            }
        }

        let builtins = [
            ("ctx", "Switch kafkactl context (:ctx [name])"),
            ("ns", "Switch namespace (:ns [name])"),
            ("skin", "Switch color theme (:skin [name])"),
            ("readonly", "Enable read-only safety guardrail"),
            ("write", "Enable write mode for current session"),
            ("import", "Import unsynchronized topics/connectors"),
            ("help", "Open keyboard & filter help"),
            ("q", "Quit domite"),
        ];
        for (cmd, desc) in builtins {
            if q.is_empty() {
                all.push((cmd.into(), desc.into(), -1));
            } else if let Some(score) = self.fuzzy.score(cmd, q) {
                all.push((cmd.into(), desc.into(), score));
            }
        }

        if !q.is_empty() {
            all.sort_by_key(|a| std::cmp::Reverse(a.2));
        }
        all.into_iter().map(|(l, d, _)| (l, d)).collect()
    }

    fn handle_command_key(&mut self, event: KeyEvent, input: &mut String, selected: &mut usize) {
        let ki = self.keymap.resolve("command", event);
        let suggestions = self.command_suggestions(input);
        match ki.action {
            Some(Action::Back) => {
                self.mode = Mode::Table;
            }
            Some(Action::Down) => {
                if !suggestions.is_empty() {
                    *selected = (*selected + 1) % suggestions.len();
                }
                self.mode = Mode::Command {
                    input: input.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Up) => {
                if !suggestions.is_empty() {
                    *selected = selected.checked_sub(1).unwrap_or(suggestions.len() - 1);
                }
                self.mode = Mode::Command {
                    input: input.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Complete) => {
                if let Some((cmd, _)) = suggestions.get(*selected) {
                    *input = cmd.clone();
                    *selected = 0;
                }
                self.mode = Mode::Command {
                    input: input.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Accept) => {
                let raw = input.trim().to_string();
                let chosen = if !raw.contains(' ')
                    && let Some((sug, _)) = suggestions.get(*selected)
                {
                    if self.registry.resolve(&raw).is_some() {
                        raw
                    } else {
                        sug.clone()
                    }
                } else {
                    raw
                };
                self.mode = Mode::Table;
                self.execute_command(&chosen);
            }
            Some(Action::Backspace) => {
                input.pop();
                *selected = 0;
                self.mode = Mode::Command {
                    input: input.clone(),
                    selected: 0,
                };
            }
            Some(Action::ClearLine) => {
                input.clear();
                *selected = 0;
                self.mode = Mode::Command {
                    input: String::new(),
                    selected: 0,
                };
            }
            Some(Action::DeleteWord) => {
                delete_last_word(input);
                *selected = 0;
                self.mode = Mode::Command {
                    input: input.clone(),
                    selected: 0,
                };
            }
            _ => {
                if let KeyCode::Char(c) = event.code
                    && ki.is_text()
                {
                    input.push(c);
                    *selected = 0;
                    self.mode = Mode::Command {
                        input: input.clone(),
                        selected: 0,
                    };
                }
            }
        }
    }

    pub fn execute_command(&mut self, raw: &str) {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return;
        }
        let mut parts = trimmed.split_whitespace();
        let head = parts.next().unwrap_or_default();
        let arg = parts.next();

        match head.to_ascii_lowercase().as_str() {
            "q" | "quit" | "exit" => {
                self.should_quit = true;
            }
            "help" | "?" => {
                self.mode = Mode::Help { scroll: 0 };
            }
            "ctx" | "context" | "contexts" => {
                if let Some(ctx_name) = arg {
                    self.switch_context_by_name(ctx_name);
                } else {
                    self.mode = Mode::Contexts {
                        filter: String::new(),
                        selected: 0,
                    };
                }
            }
            "ns" | "namespace" => {
                if let Some(ns_name) = arg {
                    self.switch_namespace(Some(ns_name.to_string()));
                } else {
                    self.mode = Mode::Namespaces {
                        filter: String::new(),
                        selected: 0,
                    };
                }
            }
            "skin" | "skins" | "theme" => {
                if let Some(skin_name) = arg {
                    if let Some(p) = theme::builtin(&skin_name.to_ascii_lowercase()) {
                        theme::set(p);
                        self.active_skin = theme::canonical_skin_name(skin_name).to_string();
                        self.flash = Some((format!("Skin set to '{}'", self.active_skin), false));
                    } else {
                        self.flash = Some((format!("Unknown skin '{skin_name}'"), true));
                    }
                } else {
                    let idx = theme::BUILTIN_NAMES
                        .iter()
                        .position(|&n| n == self.active_skin)
                        .unwrap_or(0);
                    self.mode = Mode::Skins {
                        selected: idx,
                        previous_skin: self.active_skin.clone(),
                    };
                }
            }
            "readonly" | "ro" => {
                self.readonly_override = Some(true);
                self.flash = Some(("Read-only mode enabled".into(), false));
            }
            "write" | "rw" => {
                self.readonly_override = Some(false);
                self.flash = Some(("Write mode enabled for this session".into(), false));
            }
            "import" => {
                self.initiate_import();
            }
            _ => {
                if let Some(def) = self.registry.resolve(head).cloned() {
                    let ns = arg
                        .map(str::to_string)
                        .or_else(|| self.active_namespace.clone());
                    self.navigate_to(def, ns, String::new(), true);
                } else {
                    self.flash = Some((format!("Unknown command or resource '{head}'"), true));
                }
            }
        }
    }

    pub fn switch_context_by_name(&mut self, name: &str) {
        let Some(ctx) = self.config.find_context(name).cloned() else {
            self.flash = Some((format!("Context '{name}' not found in config"), true));
            return;
        };
        if let Ok(http_client) = crate::api::client::HttpClient::new(ctx) {
            self.switch_context(Arc::new(http_client));
        }
    }

    // -----------------------------------------------------------------------
    // Filter mode (`/`)
    // -----------------------------------------------------------------------

    fn handle_filter_key(&mut self, event: KeyEvent, input: &mut String) {
        let ki = self.keymap.resolve("filter", event);
        match ki.action {
            Some(Action::Back) => {
                self.set_filter(String::new());
                self.mode = Mode::Table;
            }
            Some(Action::Accept) => {
                self.set_filter(input.clone());
                if self.filter_parsed.error().is_none() {
                    self.mode = Mode::Table;
                }
            }
            Some(Action::Backspace) => {
                input.pop();
                self.set_filter(input.clone());
                self.mode = Mode::Filter {
                    input: input.clone(),
                };
            }
            Some(Action::ClearLine) => {
                input.clear();
                self.set_filter(String::new());
                self.mode = Mode::Filter {
                    input: String::new(),
                };
            }
            Some(Action::DeleteWord) => {
                delete_last_word(input);
                self.set_filter(input.clone());
                self.mode = Mode::Filter {
                    input: input.clone(),
                };
            }
            _ => {
                if let KeyCode::Char(c) = event.code
                    && ki.is_text()
                {
                    input.push(c);
                    self.set_filter(input.clone());
                    self.mode = Mode::Filter {
                        input: input.clone(),
                    };
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Detail, Diff, Confirm, Prompt, Explain, Pickers, Help
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn handle_detail_key(
        &mut self,
        input: KeyInput,
        title: String,
        lines: Vec<String>,
        scroll: &mut usize,
        hscroll: &mut usize,
        wrap: &mut bool,
        editable: Option<Value>,
    ) {
        let max_scroll = lines.len().saturating_sub(1);
        match input.action {
            Some(Action::Back | Action::Close) => {
                self.mode = Mode::Table;
                return;
            }
            Some(Action::Down) => *scroll = (*scroll + 1).min(max_scroll),
            Some(Action::Up) => *scroll = scroll.saturating_sub(1),
            Some(Action::First) => *scroll = 0,
            Some(Action::Last) => *scroll = max_scroll,
            Some(Action::PageDown) => *scroll = (*scroll + 20).min(max_scroll),
            Some(Action::PageUp) => *scroll = scroll.saturating_sub(20),
            Some(Action::Left) => *hscroll = hscroll.saturating_sub(4),
            Some(Action::Right) => *hscroll += 4,
            Some(Action::Wrap) => *wrap = !*wrap,
            Some(Action::Edit) if editable.is_some() => {
                self.mode = Mode::Table;
                self.request_edit();
                return;
            }
            _ => {}
        }
        self.mode = Mode::Detail {
            title,
            lines,
            scroll: *scroll,
            hscroll: *hscroll,
            wrap: *wrap,
            editable,
        };
    }

    fn handle_diff_key(
        &mut self,
        input: KeyInput,
        title: String,
        summary: Vec<String>,
        diff_lines: Vec<String>,
        scroll: &mut usize,
        pending_action: PendingWriteAction,
    ) {
        let max_scroll = diff_lines.len().saturating_sub(1);
        match input.action {
            Some(Action::Accept) => {
                self.mode = Mode::Table;
                self.flash = Some(("Executing confirmed action…".into(), false));
                spawn_execute_action(
                    self.generation,
                    self.client.clone(),
                    pending_action,
                    self.msg_tx.clone(),
                );
            }
            Some(Action::Back) => {
                self.mode = Mode::Table;
                self.flash = Some(("Cancelled".into(), false));
            }
            Some(Action::Down) => {
                *scroll = (*scroll + 1).min(max_scroll);
                self.mode = Mode::Diff {
                    title,
                    summary,
                    diff_lines,
                    scroll: *scroll,
                    pending_action,
                };
            }
            Some(Action::Up) => {
                *scroll = scroll.saturating_sub(1);
                self.mode = Mode::Diff {
                    title,
                    summary,
                    diff_lines,
                    scroll: *scroll,
                    pending_action,
                };
            }
            Some(Action::PageDown) => {
                *scroll = (*scroll + 15).min(max_scroll);
                self.mode = Mode::Diff {
                    title,
                    summary,
                    diff_lines,
                    scroll: *scroll,
                    pending_action,
                };
            }
            Some(Action::PageUp) => {
                *scroll = scroll.saturating_sub(15);
                self.mode = Mode::Diff {
                    title,
                    summary,
                    diff_lines,
                    scroll: *scroll,
                    pending_action,
                };
            }
            _ => {}
        }
    }

    fn handle_confirm_key(&mut self, input: KeyInput, pending_action: PendingWriteAction) {
        match input.action {
            Some(Action::Accept) => {
                self.mode = Mode::Table;
                self.flash = Some(("Executing confirmed action…".into(), false));
                spawn_execute_action(
                    self.generation,
                    self.client.clone(),
                    pending_action,
                    self.msg_tx.clone(),
                );
            }
            Some(Action::Back) => {
                self.mode = Mode::Table;
                self.flash = Some(("Cancelled".into(), false));
            }
            _ => {}
        }
    }

    fn handle_prompt_key(
        &mut self,
        event: KeyEvent,
        title: String,
        prompt: String,
        input: &mut String,
        followup: PromptFollowup,
    ) {
        let ki = self.keymap.resolve("prompt", event);
        match ki.action {
            Some(Action::Back) => {
                self.mode = Mode::Table;
            }
            Some(Action::Accept) => {
                let value = input.trim().to_string();
                if value.is_empty() {
                    self.flash = Some(("Input cannot be empty".into(), true));
                    return;
                }
                self.mode = Mode::Table;
                match followup {
                    PromptFollowup::ConsumerGroupReset {
                        namespace,
                        group,
                        topic,
                        method,
                    } => {
                        spawn_dry_run(
                            self.generation,
                            self.client.clone(),
                            None,
                            PendingWriteAction::ResetConsumerGroup {
                                namespace,
                                group,
                                topic,
                                method,
                                options: Some(value),
                            },
                            self.msg_tx.clone(),
                        );
                    }
                    PromptFollowup::ConnectClusterVault {
                        namespace,
                        connect_cluster,
                    } => {
                        let client = self.client.clone();
                        let tx = self.msg_tx.clone();
                        let r#gen = self.generation;
                        tokio::spawn(async move {
                            match client
                                .vault_passwords(&namespace, &connect_cluster, &[value])
                                .await
                            {
                                Ok(res) => {
                                    let encrypted = res
                                        .first()
                                        .and_then(|v| v.pointer("/spec/encrypted"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("<empty>")
                                        .to_string();
                                    let lines = vec![
                                        format!("Connect Cluster: {connect_cluster}"),
                                        format!("Namespace:       {namespace}"),
                                        String::new(),
                                        format!("Encrypted Vault Value: {encrypted}"),
                                    ];
                                    let _ = tx.send(Msg::ActionComplete {
                                        generation: r#gen,
                                        message: format!(
                                            "Encrypted secret with Connect Cluster '{connect_cluster}'"
                                        ),
                                        is_error: false,
                                        detail_popup: Some((
                                            format!("Vault Secret ({connect_cluster})"),
                                            lines,
                                        )),
                                        refresh: false,
                                    });
                                }
                                Err(e) => {
                                    let _ = tx.send(Msg::ActionComplete {
                                        generation: r#gen,
                                        message: format!("Vault encryption failed: {e}"),
                                        is_error: true,
                                        detail_popup: None,
                                        refresh: false,
                                    });
                                }
                            }
                        });
                    }
                }
            }
            Some(Action::Backspace) => {
                input.pop();
                self.mode = Mode::Prompt {
                    title,
                    prompt,
                    input: input.clone(),
                    followup,
                };
            }
            Some(Action::ClearLine) => {
                input.clear();
                self.mode = Mode::Prompt {
                    title,
                    prompt,
                    input: String::new(),
                    followup,
                };
            }
            _ => {
                if let KeyCode::Char(c) = event.code
                    && ki.is_text()
                {
                    input.push(c);
                    self.mode = Mode::Prompt {
                        title,
                        prompt,
                        input: input.clone(),
                        followup,
                    };
                }
            }
        }
    }

    fn handle_explain_key(
        &mut self,
        input: KeyInput,
        title: String,
        findings: Vec<ExplainFinding>,
        scroll: &mut usize,
        wrap: &mut bool,
    ) {
        let max_scroll = findings.len().saturating_sub(1);
        match input.action {
            Some(Action::Back | Action::Close) => {
                self.mode = Mode::Table;
                return;
            }
            Some(Action::Down) => *scroll = (*scroll + 1).min(max_scroll),
            Some(Action::Up) => *scroll = scroll.saturating_sub(1),
            Some(Action::First) => *scroll = 0,
            Some(Action::Last) => *scroll = max_scroll,
            Some(Action::Wrap) => *wrap = !*wrap,
            _ => {}
        }
        self.mode = Mode::Explain {
            title,
            findings,
            scroll: *scroll,
            wrap: *wrap,
        };
    }

    pub fn filtered_namespaces(&self, filter: &str) -> Vec<(String, bool)> {
        let mut all = vec![("*".to_string(), false)];
        all.extend(self.namespaces.iter().cloned());
        if filter.trim().is_empty() {
            return all;
        }
        all.into_iter()
            .filter(|(n, _)| self.fuzzy.score(n, filter.trim()).is_some())
            .collect()
    }

    fn handle_namespaces_key(
        &mut self,
        event: KeyEvent,
        filter: &mut String,
        selected: &mut usize,
    ) {
        let ki = self.keymap.resolve("namespaces", event);
        let list = self.filtered_namespaces(filter);
        match ki.action {
            Some(Action::Back) => {
                self.mode = Mode::Table;
            }
            Some(Action::Down) => {
                if !list.is_empty() {
                    *selected = (*selected + 1) % list.len();
                }
                self.mode = Mode::Namespaces {
                    filter: filter.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Up) => {
                if !list.is_empty() {
                    *selected = selected.checked_sub(1).unwrap_or(list.len() - 1);
                }
                self.mode = Mode::Namespaces {
                    filter: filter.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Accept) => {
                if let Some((ns, forbidden)) = list.get(*selected) {
                    if *forbidden {
                        self.flash = Some((
                            format!("Cannot switch to '{ns}': no role binding (403 Forbidden)"),
                            true,
                        ));
                    } else {
                        self.mode = Mode::Table;
                        self.switch_namespace(Some(ns.clone()));
                    }
                } else {
                    self.mode = Mode::Table;
                }
            }
            Some(Action::Backspace) => {
                filter.pop();
                *selected = 0;
                self.mode = Mode::Namespaces {
                    filter: filter.clone(),
                    selected: 0,
                };
            }
            _ => {
                if let KeyCode::Char(c) = event.code
                    && ki.is_text()
                {
                    filter.push(c);
                    *selected = 0;
                    self.mode = Mode::Namespaces {
                        filter: filter.clone(),
                        selected: 0,
                    };
                }
            }
        }
    }

    pub fn filtered_contexts(&self, filter: &str) -> Vec<KafkaContext> {
        let list = if self.config.contexts.is_empty() {
            vec![self.client.context().clone()]
        } else {
            self.config.contexts.clone()
        };
        if filter.trim().is_empty() {
            return list;
        }
        list.into_iter()
            .filter(|c| self.fuzzy.score(&c.name, filter.trim()).is_some())
            .collect()
    }

    fn handle_contexts_key(&mut self, event: KeyEvent, filter: &mut String, selected: &mut usize) {
        let ki = self.keymap.resolve("contexts", event);
        let list = self.filtered_contexts(filter);
        match ki.action {
            Some(Action::Back) => {
                self.mode = Mode::Table;
            }
            Some(Action::Down) => {
                if !list.is_empty() {
                    *selected = (*selected + 1) % list.len();
                }
                self.mode = Mode::Contexts {
                    filter: filter.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Up) => {
                if !list.is_empty() {
                    *selected = selected.checked_sub(1).unwrap_or(list.len() - 1);
                }
                self.mode = Mode::Contexts {
                    filter: filter.clone(),
                    selected: *selected,
                };
            }
            Some(Action::Accept) => {
                if let Some(ctx) = list.get(*selected) {
                    let name = ctx.name.clone();
                    self.mode = Mode::Table;
                    self.switch_context_by_name(&name);
                } else {
                    self.mode = Mode::Table;
                }
            }
            Some(Action::Backspace) => {
                filter.pop();
                *selected = 0;
                self.mode = Mode::Contexts {
                    filter: filter.clone(),
                    selected: 0,
                };
            }
            _ => {
                if let KeyCode::Char(c) = event.code
                    && ki.is_text()
                {
                    filter.push(c);
                    *selected = 0;
                    self.mode = Mode::Contexts {
                        filter: filter.clone(),
                        selected: 0,
                    };
                }
            }
        }
    }

    fn handle_sort_picker_key(&mut self, input: KeyInput, selected: &mut usize) {
        let count = self.columns.len();
        match input.action {
            Some(Action::Back | Action::Close) => self.mode = Mode::Table,
            Some(Action::Down) if count > 0 => {
                *selected = (*selected + 1) % count;
                self.mode = Mode::SortPicker {
                    selected: *selected,
                };
            }
            Some(Action::Up) if count > 0 => {
                *selected = selected.checked_sub(1).unwrap_or(count - 1);
                self.mode = Mode::SortPicker {
                    selected: *selected,
                };
            }
            Some(Action::Accept) => {
                let desc = match self.sort_col {
                    Some((col, d)) if col == *selected => !d,
                    _ => false,
                };
                self.sort_col = Some((*selected, desc));
                self.mode = Mode::Table;
                self.rebuild_rows();
            }
            _ => {}
        }
    }

    fn handle_action_menu_key(
        &mut self,
        input: KeyInput,
        title: String,
        entries: Vec<MenuEntry>,
        selected: &mut usize,
    ) {
        match input.action {
            Some(Action::Back | Action::Close) => {
                self.mode = Mode::Table;
            }
            Some(Action::Down) if !entries.is_empty() => {
                *selected = (*selected + 1) % entries.len();
                self.mode = Mode::ActionMenu {
                    title,
                    entries,
                    selected: *selected,
                };
            }
            Some(Action::Up) if !entries.is_empty() => {
                *selected = selected.checked_sub(1).unwrap_or(entries.len() - 1);
                self.mode = Mode::ActionMenu {
                    title,
                    entries,
                    selected: *selected,
                };
            }
            Some(Action::Accept) => {
                let Some(entry) = entries.get(*selected).cloned() else {
                    self.mode = Mode::Table;
                    return;
                };
                self.mode = Mode::Table;
                if entry.mutating && self.guard_readonly() {
                    return;
                }
                self.execute_menu_op(entry.op);
            }
            _ => {}
        }
    }

    fn execute_menu_op(&mut self, op: MenuOp) {
        let ns = self
            .selected_row()
            .and_then(|r| r.namespace.clone())
            .or_else(|| self.active_namespace.clone())
            .unwrap_or_else(|| self.client.context().namespace.clone());

        match op {
            MenuOp::TopicJumpConsumerGroups { topic } => {
                if let Some(def) = self.registry.find_by_kind("ConsumerGroup").cloned() {
                    self.navigate_to(def, Some(ns), format!("topic={topic}"), true);
                }
            }
            MenuOp::TopicJumpAcls { topic } => {
                if let Some(def) = self.registry.find_by_kind("AccessControlEntry").cloned() {
                    self.navigate_to(def, Some(ns), topic, true);
                }
            }
            MenuOp::TopicJumpSchemas { topic } => {
                if let Some(def) = self.registry.find_by_kind("Schema").cloned() {
                    self.navigate_to(def, Some(ns), topic, true);
                }
            }
            MenuOp::TopicShowConfigs { topic } => {
                if let Some(obj) = self.selected_object()
                    && let Some(configs) = obj.pointer("/spec/configs")
                {
                    let yaml = serde_yaml::to_string(configs).unwrap_or_default();
                    self.mode = Mode::Detail {
                        title: format!("Topic Configs: {topic}"),
                        lines: yaml.lines().map(str::to_string).collect(),
                        scroll: 0,
                        hscroll: 0,
                        wrap: false,
                        editable: Some(obj.clone()),
                    };
                }
            }
            MenuOp::TopicDeleteRecords { topic } => {
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    None,
                    PendingWriteAction::DeleteRecords {
                        namespace: ns,
                        topic,
                    },
                    self.msg_tx.clone(),
                );
            }
            MenuOp::TopicImportUnsynced | MenuOp::ConnectorImportUnsynced => {
                self.initiate_import();
            }
            MenuOp::ConnectorChangeState { connectors, action } => {
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    None,
                    PendingWriteAction::ChangeConnectorState {
                        namespace: ns,
                        connectors,
                        action,
                    },
                    self.msg_tx.clone(),
                );
            }
            MenuOp::ConnectorViewOffsets { connector } => {
                let client = self.client.clone();
                let tx = self.msg_tx.clone();
                let r#gen = self.generation;
                tokio::spawn(async move {
                    match client.list_connector_offsets(&ns, &connector).await {
                        Ok(offsets) => {
                            let yaml = serde_yaml::to_string(&offsets).unwrap_or_default();
                            let lines = yaml.lines().map(str::to_string).collect();
                            let _ = tx.send(Msg::ActionComplete {
                                generation: r#gen,
                                message: format!("Loaded offsets for '{connector}'"),
                                is_error: false,
                                detail_popup: Some((
                                    format!("Connector Offsets: {connector}"),
                                    lines,
                                )),
                                refresh: false,
                            });
                        }
                        Err(e) => {
                            let _ = tx.send(Msg::ActionComplete {
                                generation: r#gen,
                                message: format!("Failed to fetch offsets: {e}"),
                                is_error: true,
                                detail_popup: None,
                                refresh: false,
                            });
                        }
                    }
                });
            }
            MenuOp::ConnectorResetOffsets { connector } => {
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    None,
                    PendingWriteAction::ResetConnectorOffsets {
                        namespace: ns,
                        connector,
                    },
                    self.msg_tx.clone(),
                );
            }
            MenuOp::ConsumerGroupDrillPartitions { group, ownership } => {
                let member_def =
                    crate::api::discovery::consumer_group_member_definition(&group, &ownership);
                self.navigate_to(member_def, Some(ns), String::new(), true);
            }
            MenuOp::ConsumerGroupJumpTopic { topic } => {
                if let Some(def) = self.registry.find_by_kind("Topic").cloned() {
                    self.navigate_to(def, Some(ns), topic, true);
                }
            }
            MenuOp::ConsumerGroupReset {
                group,
                topic,
                method,
                needs_input,
            } => {
                if let Some(prompt_label) = needs_input {
                    self.mode = Mode::Prompt {
                        title: format!("Reset Consumer Group {group} ({method})"),
                        prompt: prompt_label,
                        input: String::new(),
                        followup: PromptFollowup::ConsumerGroupReset {
                            namespace: ns,
                            group,
                            topic,
                            method,
                        },
                    };
                } else {
                    spawn_dry_run(
                        self.generation,
                        self.client.clone(),
                        None,
                        PendingWriteAction::ResetConsumerGroup {
                            namespace: ns,
                            group,
                            topic,
                            method,
                            options: None,
                        },
                        self.msg_tx.clone(),
                    );
                }
            }
            MenuOp::ConsumerGroupDelete { group } => {
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    None,
                    PendingWriteAction::DeleteConsumerGroup {
                        namespace: ns,
                        group,
                    },
                    self.msg_tx.clone(),
                );
            }
            MenuOp::SchemaSetCompatibility {
                subject,
                compatibility,
            } => {
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    None,
                    PendingWriteAction::UpdateSchemaCompatibility {
                        namespace: ns,
                        subject,
                        compatibility,
                    },
                    self.msg_tx.clone(),
                );
            }
            MenuOp::ConnectClusterJumpConnectors { connect_cluster } => {
                if let Some(def) = self.registry.find_by_kind("Connector").cloned() {
                    self.navigate_to(
                        def,
                        Some(ns),
                        format!("connect-cluster={connect_cluster}"),
                        true,
                    );
                }
            }
            MenuOp::ConnectClusterVault { connect_cluster } => {
                self.mode = Mode::Prompt {
                    title: format!("Vault Encrypt Secret ({connect_cluster})"),
                    prompt: "Enter secret value to encrypt (will not be logged):".into(),
                    input: String::new(),
                    followup: PromptFollowup::ConnectClusterVault {
                        namespace: ns,
                        connect_cluster,
                    },
                };
            }
            MenuOp::NamespaceEnter { namespace } => {
                if let Some(def) = self.registry.find_by_kind("Topic").cloned() {
                    self.navigate_to(def, Some(namespace), String::new(), true);
                }
            }
            MenuOp::NamespaceResetPassword {
                namespace,
                kafka_user,
            } => {
                spawn_dry_run(
                    self.generation,
                    self.client.clone(),
                    None,
                    PendingWriteAction::ResetUserPassword {
                        namespace,
                        user: kafka_user,
                    },
                    self.msg_tx.clone(),
                );
            }
        }
    }

    fn handle_skins_key(&mut self, input: KeyInput, selected: &mut usize, previous_skin: String) {
        let count = theme::BUILTIN_NAMES.len();
        match input.action {
            Some(Action::Back | Action::Close) => {
                if let Some(p) = theme::builtin(&previous_skin) {
                    theme::set(p);
                    self.active_skin = previous_skin;
                }
                self.mode = Mode::Table;
            }
            Some(Action::Down) => {
                *selected = (*selected + 1) % count;
                let name = theme::BUILTIN_NAMES[*selected];
                if let Some(p) = theme::builtin(name) {
                    theme::set(p);
                    self.active_skin = name.to_string();
                }
                self.mode = Mode::Skins {
                    selected: *selected,
                    previous_skin,
                };
            }
            Some(Action::Up) => {
                *selected = selected.checked_sub(1).unwrap_or(count - 1);
                let name = theme::BUILTIN_NAMES[*selected];
                if let Some(p) = theme::builtin(name) {
                    theme::set(p);
                    self.active_skin = name.to_string();
                }
                self.mode = Mode::Skins {
                    selected: *selected,
                    previous_skin,
                };
            }
            Some(Action::Accept) => {
                let name = theme::BUILTIN_NAMES[*selected];
                if let Some(p) = theme::builtin(name) {
                    theme::set(p);
                    self.active_skin = name.to_string();
                }
                self.mode = Mode::Table;
                self.flash = Some((format!("Active skin: {name}"), false));
            }
            _ => {}
        }
    }

    fn handle_help_key(&mut self, input: KeyInput, scroll: &mut usize) {
        match input.action {
            Some(Action::Back | Action::Close) => self.mode = Mode::Table,
            Some(Action::Down) => {
                *scroll += 1;
                self.mode = Mode::Help { scroll: *scroll };
            }
            Some(Action::Up) => {
                *scroll = scroll.saturating_sub(1);
                self.mode = Mode::Help { scroll: *scroll };
            }
            Some(Action::PageDown) => {
                *scroll += 12;
                self.mode = Mode::Help { scroll: *scroll };
            }
            Some(Action::PageUp) => {
                *scroll = scroll.saturating_sub(12);
                self.mode = Mode::Help { scroll: *scroll };
            }
            Some(Action::First) => {
                *scroll = 0;
                self.mode = Mode::Help { scroll: 0 };
            }
            _ => {}
        }
    }
}

fn delete_last_word(input: &mut String) {
    let trimmed = input.trim_end();
    let new_len = trimmed
        .rfind(char::is_whitespace)
        .map(|i| i + 1)
        .unwrap_or(0);
    input.truncate(new_len);
}

/// Format a `Schema` resource into a pretty-printed schema definition view (`Enter` on Schema).
pub fn format_schema_pretty(obj: &Value) -> (String, Vec<String>) {
    let subject = obj
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("<subject>");
    let id = obj
        .pointer("/spec/id")
        .and_then(Value::as_i64)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "-".into());
    let version = obj
        .pointer("/spec/version")
        .and_then(Value::as_i64)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "-".into());
    let schema_type = obj
        .pointer("/spec/schemaType")
        .and_then(Value::as_str)
        .unwrap_or("AVRO");
    let compatibility = obj
        .pointer("/spec/compatibility")
        .and_then(Value::as_str)
        .unwrap_or("GLOBAL");

    let mut lines = vec![
        format!("# Subject:       {subject}"),
        format!("# Schema ID:     {id}"),
        format!("# Version:       v{version}"),
        format!("# Type:          {schema_type}"),
        format!("# Compatibility: {compatibility}"),
    ];

    if let Some(refs) = obj.pointer("/spec/references").and_then(Value::as_array)
        && !refs.is_empty()
    {
        for r in refs {
            let rname = r.get("name").and_then(Value::as_str).unwrap_or("?");
            let rsubj = r.get("subject").and_then(Value::as_str).unwrap_or("?");
            let rver = r.get("version").and_then(Value::as_i64).unwrap_or(1);
            lines.push(format!("# Reference:     {rname} -> {rsubj} (v{rver})"));
        }
    }
    lines.push(String::new());

    if let Some(raw_schema) = obj.pointer("/spec/schema").and_then(Value::as_str) {
        if let Ok(parsed_json) = serde_json::from_str::<Value>(raw_schema)
            && let Ok(pretty) = serde_json::to_string_pretty(&parsed_json)
        {
            lines.extend(pretty.lines().map(str::to_string));
        } else {
            lines.extend(raw_schema.lines().map(str::to_string));
        }
    } else {
        lines.push("# (No schema body returned)".into());
    }

    (
        format!("Schema Definition ({schema_type} v{version}): {subject}"),
        lines,
    )
}
