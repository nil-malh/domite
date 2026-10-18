//! `domite` — A terminal UI for ns4kafka, built the sofka way.

mod api;
mod app;
mod applog;
mod atomicfile;
mod columns;
mod filter;
mod fuzzy;
mod keymap;
mod keys;
mod redact;
mod store;
mod theme;
mod ui;

use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{CommandFactory, Parser};
use clap_complete::Shell;
use crossterm::event::{Event, EventStream, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::{mpsc, watch};

use crate::api::cache::PersistentCache;
use crate::api::client::{FakeClient, HttpClient, Ns4KafkaClient};
use crate::api::config::{KafkaContext, LoadedConfig};
use crate::api::discovery::DiscoveryRegistry;
use crate::api::poller::{PollTarget, poll_once, spawn_poller};
use crate::app::{App, EditorRequest};

#[derive(Debug, Parser)]
#[command(
    name = "domite",
    version,
    about = "A terminal UI for ns4kafka, built the sofka way"
)]
pub struct Cli {
    /// Initial resource kind or short alias (e.g. topics, to, connectors, co, cg, schemas, acls)
    #[arg(value_name = "RESOURCE")]
    pub resource: Option<String>,

    /// Select kafkactl context by name
    #[arg(short = 'c', long = "context")]
    pub context: Option<String>,

    /// Select initial namespace (use '*' or 'all' for all namespaces)
    #[arg(short = 'n', long = "namespace")]
    pub namespace: Option<String>,

    /// Force read-only mode (disables all apply/delete/reset/import mutations)
    #[arg(long = "readonly", conflicts_with = "write")]
    pub readonly: bool,

    /// Force writable mode (overrides default read-only guardrail on prod* contexts)
    #[arg(long = "write", conflicts_with = "readonly")]
    pub write: bool,

    /// Override built-in color skin (e.g. catppuccin-mocha, catppuccin-latte, gruvbox-dark, nord, dracula, tokyo-night)
    #[arg(long = "skin")]
    pub skin: Option<String>,

    /// Path to kafkactl config.yml (defaults to ~/.kafkactl/config.yml)
    #[arg(long = "kafkactl-config")]
    pub kafkactl_config: Option<PathBuf>,

    /// Path to domite config.toml (defaults to ~/.config/domite/config.toml)
    #[arg(long = "config")]
    pub config: Option<PathBuf>,

    /// Use built-in in-memory demo cluster (no external ns4kafka server required)
    #[arg(long = "demo")]
    pub demo: bool,

    /// Headless check: connect, discover /api-resources & server version, print summary and exit
    #[arg(long = "check")]
    pub check: bool,

    /// Headless snapshot: poll once, render one 120x32 TUI frame to stdout and exit
    #[arg(long = "snapshot")]
    pub snapshot: bool,

    /// Generate shell completions for the given shell and exit
    #[arg(long = "completions", value_name = "SHELL")]
    pub completions: Option<Shell>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(shell) = cli.completions {
        let mut cmd = Cli::command();
        clap_complete::generate(shell, &mut cmd, "domite", &mut io::stdout());
        return Ok(());
    }

    applog::init(None);

    let mut loaded = LoadedConfig::load(cli.kafkactl_config.as_deref(), cli.config.as_deref());

    if let Some(skin_override) = &cli.skin {
        loaded.domite.skin.name = Some(skin_override.clone());
    }

    let palette = theme::resolve_skin(
        loaded.domite.skin.name.as_deref(),
        &loaded.domite.skin.colors,
    );
    theme::init(palette);
    theme::set_background(loaded.domite.skin.background);

    let readonly_override = if cli.readonly {
        Some(true)
    } else if cli.write {
        Some(false)
    } else {
        loaded.domite.readonly
    };

    let client: Arc<dyn Ns4KafkaClient> = build_client(&cli, &loaded)?;

    if cli.check {
        return run_headless_check(client, readonly_override).await;
    }

    let namespace_override = cli.namespace.as_deref().map(|ns| {
        if ns.eq_ignore_ascii_case("all") {
            "*".to_string()
        } else {
            ns.to_string()
        }
    });

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (poll_target_tx, poll_target_rx) = watch::channel::<Option<PollTarget>>(None);
    let (force_poll_tx, force_poll_rx) = mpsc::unbounded_channel();

    let schema_ttl_secs = (loaded.domite.schema_cache_ttl_hours as i64) * 3600;
    let cache = if cli.demo {
        PersistentCache::in_memory(schema_ttl_secs)
    } else {
        PersistentCache::new(PersistentCache::default_cache_dir(), schema_ttl_secs)
    };

    let mut app = App::new(
        loaded,
        client,
        cache,
        cli.resource.as_deref(),
        namespace_override,
        readonly_override,
        msg_tx.clone(),
        poll_target_tx,
        force_poll_tx,
    );

    if cli.snapshot {
        return run_headless_snapshot(&mut app, poll_target_rx, &msg_tx, &mut msg_rx).await;
    }

    app.spawn_discovery();
    let _poller_handle = spawn_poller(poll_target_rx, force_poll_rx, msg_tx);

    run_tui(&mut app, &mut msg_rx).await
}

fn build_client(cli: &Cli, loaded: &LoadedConfig) -> anyhow::Result<Arc<dyn Ns4KafkaClient>> {
    if cli.demo {
        return Ok(Arc::new(FakeClient::demo(None)));
    }
    let ctx_name = cli.context.as_deref().or(loaded.current_context.as_deref());

    if let Some(name) = ctx_name
        && let Some(ctx) = loaded.find_context(name).cloned()
    {
        let client = HttpClient::new(ctx).map_err(|e| anyhow::anyhow!(e))?;
        return Ok(Arc::new(client));
    }

    // In --snapshot or --check mode with no kafkactl config on disk, fall back to FakeClient
    if cli.snapshot || cli.check || loaded.contexts.is_empty() {
        let fallback_ctx = cli.context.as_ref().map(|name| KafkaContext {
            name: name.clone(),
            api: "https://ns4kafka.clermont.example.com".into(),
            user_token: "glpat-demo-token".into(),
            username: "michelin-dev".into(),
            namespace: cli
                .namespace
                .clone()
                .unwrap_or_else(|| "team-orders".into()),
            readonly: KafkaContext::default_readonly_for_name(name),
        });
        return Ok(Arc::new(FakeClient::demo(fallback_ctx)));
    }

    let first = loaded.contexts[0].clone();
    let client = HttpClient::new(first).map_err(|e| anyhow::anyhow!(e))?;
    Ok(Arc::new(client))
}

async fn run_headless_check(
    client: Arc<dyn Ns4KafkaClient>,
    readonly_override: Option<bool>,
) -> anyhow::Result<()> {
    let ctx = client.context();
    let ro = readonly_override.unwrap_or(ctx.readonly);
    let token = client.login().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let version = client
        .server_version()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let resources = client
        .api_resources()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let reg = DiscoveryRegistry::from_server_list(resources);
    let namespaces = client.list_namespaces().await.unwrap_or_default();

    println!("domite check: OK");
    println!("  context:    {}", ctx.name);
    println!("  api:        {}", redact::credentials(&ctx.api));
    println!("  namespace:  {}", ctx.namespace);
    println!("  user:       {} (active={})", token.username, token.active);
    println!("  server:     ns4kafka v{version}");
    println!("  mode:       {}", if ro { "readonly" } else { "writable" });
    println!(
        "  namespaces: {}",
        namespaces
            .iter()
            .map(|(n, f)| if *f { format!("{n}(403)") } else { n.clone() })
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  resources:  {}",
        reg.resources()
            .iter()
            .map(|r| format!("{} ({})", r.kind, r.short_name()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

async fn run_headless_snapshot(
    app: &mut App,
    poll_target_rx: watch::Receiver<Option<PollTarget>>,
    msg_tx: &mpsc::UnboundedSender<crate::store::Msg>,
    msg_rx: &mut mpsc::UnboundedReceiver<crate::store::Msg>,
) -> anyhow::Result<()> {
    if let Ok(ver) = app.client.server_version().await {
        app.server_version = Some(ver);
    }
    if let Ok(info) = app.client.token_info().await {
        app.username = Some(info.username);
    }
    if let Some(target) = poll_target_rx.borrow().clone() {
        let mut prev = std::collections::HashMap::new();
        let mut first = true;
        poll_once(&target, &mut prev, &mut first, msg_tx).await;
    }
    if app.demo_or_fast_hydrate() {
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let mut batch = Vec::new();
    while let Ok(msg) = msg_rx.try_recv() {
        batch.push(msg);
    }
    app.handle_msgs(batch);
    let frame = ui::render_snapshot(app, 120, 32);
    print!("{}", redact::credentials(&frame));
    Ok(())
}

async fn run_tui(
    app: &mut App,
    msg_rx: &mut mpsc::UnboundedReceiver<crate::store::Msg>,
) -> anyhow::Result<()> {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(info);
    }));

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    let res = loop {
        terminal.draw(|f| ui::draw(f, app))?;

        if let Some(req) = app.pending_editor.take() {
            suspend_and_edit(&mut terminal, app, req)?;
            continue;
        }

        if app.should_quit {
            break Ok(());
        }

        tokio::select! {
            maybe_ev = events.next() => {
                match maybe_ev {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        app.handle_key(key);
                    }
                    Some(Ok(Event::Resize(_, _))) => {}
                    Some(Err(e)) => break Err(anyhow::anyhow!(e)),
                    None => break Ok(()),
                    _ => {}
                }
            }
            Some(first_msg) = msg_rx.recv() => {
                let mut batch = vec![first_msg];
                while let Ok(queued) = msg_rx.try_recv() {
                    batch.push(queued);
                }
                app.handle_msgs(batch);
            }
            _ = tick.tick() => {
                app.rebuild_rows();
            }
        }
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    res
}

fn suspend_and_edit(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    req: EditorRequest,
) -> anyhow::Result<()> {
    let tmp_path = std::env::temp_dir().join(format!(
        "domite-edit-{}-{}.yaml",
        std::process::id(),
        req.resource.path
    ));
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(req.yaml.as_bytes())?;
        f.sync_all()?;
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    let editor = std::env::var("EDITOR")
        .or_else(|_| std::env::var("VISUAL"))
        .unwrap_or_else(|_| "vi".into());
    let status = std::process::Command::new(&editor).arg(&tmp_path).status();

    enable_raw_mode()?;
    execute!(terminal.backend_mut(), EnterAlternateScreen)?;
    terminal.clear()?;

    match status {
        Ok(s) if s.success() => {
            if let Ok(edited) = std::fs::read_to_string(&tmp_path) {
                app.submit_edited_yaml(req, &edited);
            }
        }
        Ok(s) => {
            app.flash = Some((format!("Editor exited with {s}"), true));
        }
        Err(e) => {
            app.flash = Some((format!("Failed to launch editor '{editor}': {e}"), true));
        }
    }
    let _ = std::fs::remove_file(&tmp_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_command_factory_and_argument_parsing_are_valid() {
        Cli::command().debug_assert();

        let cli = Cli::try_parse_from([
            "domite",
            "connectors",
            "-c",
            "oom-cqt",
            "-n",
            "coomgbl0",
            "--readonly",
            "--skin",
            "nord",
            "--demo",
        ])
        .unwrap();
        assert_eq!(cli.resource.as_deref(), Some("connectors"));
        assert_eq!(cli.context.as_deref(), Some("oom-cqt"));
        assert_eq!(cli.namespace.as_deref(), Some("coomgbl0"));
        assert!(cli.readonly);
        assert!(!cli.write);
        assert_eq!(cli.skin.as_deref(), Some("nord"));
        assert!(cli.demo);

        // --readonly and --write are mutually exclusive
        assert!(Cli::try_parse_from(["domite", "--readonly", "--write"]).is_err());
    }

    #[test]
    fn generates_non_empty_shell_completions_for_all_shells() {
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::Elvish,
            Shell::PowerShell,
        ] {
            let mut cmd = Cli::command();
            let mut buf = Vec::new();
            clap_complete::generate(shell, &mut cmd, "domite", &mut buf);
            assert!(
                !buf.is_empty(),
                "shell completion for {shell:?} should not be empty"
            );
        }
    }
}
