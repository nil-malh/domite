# domite Architecture & Module Map

## 1. High-Level Overview

`domite` brings the [sofka](https://github.com/nklmilojevic/sofka) (k9s-style Rust TUI) architecture to [michelin/ns4kafka](https://github.com/michelin/ns4kafka).

```text
src/
├── main.rs          CLI (clap), terminal lifecycle, $EDITOR handoff, tokio::select! loop, --check / --snapshot / --demo
├── ui.rs            Ratatui rendering: 7-line header + Puy de Dôme ASCII volcano, table, YAML/detail, diff, explain, popups
├── store.rs         In-memory Store + generation-tagged Msg + PendingWriteAction + composite row_key()
├── columns.rs       Curated per-kind column specs, JSON-Pointer evaluation, typed SortKey, status extraction
├── theme.rs         17 built-in 25-swatch palettes (Catppuccin, Gruvbox, Nord, Dracula, etc.) + semantic styles
├── filter.rs        Structured `/` filter parser & evaluator (text, a|b, ~fuzzy, /regex/, !not, label:, key<op>val, &&, ||)
├── fuzzy.rs         Shared nucleo-matcher wrapper with smart case
├── keys.rs          Human-readable key-chord parser (e.g. ctrl-d, shift-tab)
├── keymap.rs        Scoped Action bindings (table, detail, diff, explain, command, filter, confirm, action_menu, etc.)
├── redact.rs        Credential & IP redaction (JWTs, GitLab glpat-/glft- tokens, newPassword, aes256Key/Salt)
├── atomicfile.rs    Crash-safe atomic file replacement + 0600 secret file writer for cached JWTs
├── applog.rs        Non-blocking logfmt background logger (DOMITE_LOG=1) with automatic redaction
├── api/
│   ├── mod.rs
│   ├── config.rs    Loads ~/.kafkactl/config.yml contexts & ~/.config/domite/config.toml UI/view overrides
│   ├── discovery.rs GET /api-resources registry + ConsumerGroup + alias resolution (to, co, cg, sc, ac, st, cc, qu, rb, ns)
│   ├── client.rs    Ns4KafkaClient trait, HttpClient (reqwest + rustls-tls-native-roots + JWT cache), FakeClient
│   └── poller.rs    Per-view diffing poller + bounded-concurrency background Schema Registry hydrator
└── app/
    ├── mod.rs       App state machine, Mode enum, key handling, drill-downs, history stack, schema pretty-printer
    ├── actions.rs   Kind-specific `t` action menu definitions, dry-run diff generator, background action executors
    └── tests.rs     End-to-end TUI & state-machine tests over FakeClient
```

---

## 2. Data & Event Flow

```text
┌───────────────────────────┐
│   Ns4KafkaClient (HTTP)   │
│  or FakeClient (--demo)   │
└─────────────┬─────────────┘
              │
   ┌──────────┴──────────┐
   ▼                     ▼
┌──────────────┐  ┌──────────────────────┐
│  poller.rs   │  │  actions.rs          │
│ (list diff + │  │ (dryrun=true preview │
│  schema bg   │  │  & dryrun=false exec)│
│  hydrator)   │  └──────────┬───────────┘
└──────┬───────┘             │
       │  Msg (gen-tagged)   │
       └──────────┬──────────┘
                  ▼
      ┌───────────────────────┐
      │   main.rs select!     │◄──── Crossterm KeyEventStream
      │ (batches queued Msgs) │◄──── 1s Tick Interval
      └───────────┬───────────┘
                  ▼
      ┌───────────────────────┐
      │   App (app/mod.rs)    │
      │  Store -> rebuild_rows│
      │  (filter + sort)      │
      └───────────┬───────────┘
                  ▼
      ┌───────────────────────┐
      │     ui::draw (ui.rs)  │
      └───────────────────────┘
```

### Generation Tagging
Every `PollTarget` and background task carries `app.generation: u64`. Whenever the user switches resource kind (`:topics`, `:co`), switches namespace (`n`, `0`), switches context (`C`, `:ctx`), or drills down (`Enter`, `[`, `]`), `app.generation` increments and `app.store.clear()` runs. Any in-flight poll or background schema hydration message from an older generation is ignored by `App::apply_msg`.

---

## 3. Resource Pipelines & Special Cases

### 3.1 Schema Background Hydration (`src/api/poller.rs`)
- **Why**: `ns4kafka`'s `SchemaController.list` (`GET /api/namespaces/{ns}/schemas`) only returns subject names (`metadata.name`) with `spec: null` when more than one schema matches, because fetching every subject from Confluent Schema Registry synchronously on list would time out on large namespaces (e.g., 2,000+ schemas).
- **How `domite` handles it**:
  1. `poll_once_with_cache` sorts the subject list and checks `SchemaCache` (`Arc<RwLock<HashMap<String, Value>>>`).
  2. Subjects already in `SchemaCache` immediately get their cached `spec` (`id`, `version`, `compatibility`, `schemaType`, `schema`).
  3. Uncached subjects get `"_hydrating": true` (rendered as `…` in `VERSION`, `ID`, `COMPATIBILITY` columns) and are queued in `SchemaInFlight` (`Arc<RwLock<HashSet<String>>>`) so periodic 5s poll ticks never spawn duplicate hydration tasks for the same subject.
  4. `spawn_schema_hydrator` sorts queued subjects by distance from `target.cursor_hint` (an `Arc<AtomicUsize>` updated whenever the user moves the table cursor) and fetches `GET /api/namespaces/{ns}/schemas/{subject}` with a `Semaphore` of 6 concurrent requests.
  5. As each subject resolves, it writes to `SchemaCache`, removes itself from `SchemaInFlight`, and emits `Msg::Applied { generation, key, obj }`.
  6. If the user presses `Enter` (pretty-printed schema view) or `y` (raw YAML view) on a row that is still hydrating, `App::fetch_schema_and_open` immediately fetches that subject on demand.

### 3.2 Consumer Groups (`ConsumerGroup`)
- Exposed at `/api/namespaces/{ns}/consumer-groups` (not listed by `/api-resources`, so `DiscoveryRegistry` injects it automatically with aliases `consumer-groups`, `cg`, `groups`, `group`).
- Each item in the response represents a single `(group, topic, partition)` lag entry, so `store::row_key` builds a composite key `{namespace}/{group}/{topic}/{partition}`.
- Non-declarative (`is_declarative() == false`): cannot be edited with `e`, but supports offset reset (`POST .../consumer-groups/{group}/reset?dryrun=true|false` with `TO_EARLIEST`, `TO_LATEST`, `TO_DATETIME`, `TO_OFFSET`, `SHIFT_BY`) and deletion (`DELETE .../consumer-groups/{group}?dryrun=true|false`) via the `t` action menu.

### 3.3 Resource Quotas (`ResourceQuota`)
- `GET /api/namespaces/{ns}/resource-quotas` returns `ResourceQuotaResponse` objects where `spec` contains summary strings like `"countTopic": "3/20"`, `"diskTopic": "4.5GiB/50.0GiB"`.
- `client::expand_quota_rows` expands each quota field into a dedicated table row (`_quotaKey`, `_quotaValue`, `_quotaUsed`) so the table displays `NAME | KEY | VALUE | USED | AGE`.
- Before sending a quota to `$EDITOR` or `POST ?dryrun=...`, `_quotaKey`, `_quotaValue`, and `_quotaUsed` are stripped.

---

## 4. Mode State Machine (`src/app/mod.rs`)

| `Mode` Variant | Trigger | Purpose |
|---|---|---|
| `Mode::Table` | Default | Main resource table with sorting, multi-select (`Space`), drill-down (`Enter`) |
| `Mode::Command` | `:` | Fuzzy command palette over resource aliases and built-in commands (`:ctx`, `:ns`, `:skin`, `:readonly`, `:write`, `:import`, `:q`) |
| `Mode::Filter` | `/` | Live row filter bar supporting boolean/regex/fuzzy/column comparisons |
| `Mode::Detail` | `y` / `d` / `Enter` on Schema | Scrollable YAML or pretty-printed Schema viewer (`w` wrap, `e` edit) |
| `Mode::Diff` | After edit/delete/import dry-run | Unified diff preview (`+` green, `-` red); `y`/`Enter` executes, `n`/`Esc` cancels |
| `Mode::Confirm` | After action dry-run | Summary confirmation dialog for state/offset/compatibility/password changes |
| `Mode::Prompt` | `t` menu ops needing input | Single-line input prompt (e.g., `TO_DATETIME`, `TO_OFFSET`, `SHIFT_BY`, Vault secret) |
| `Mode::Explain` | `X` (or `Enter` on failed Connector) | Structured health diagnostics (`[FAIL]`, `[WARN]`, `[OK]`) + Connector task stack traces |
| `Mode::Namespaces` | `n` or `:ns` | Fuzzy namespace switcher (`403 Forbidden` namespaces greyed out; `*` for all namespaces) |
| `Mode::Contexts` | `C` or `:ctx` | Fuzzy `kafkactl` context switcher showing `[RO]` / `[RW]` and API URL |
| `Mode::SortPicker` | `S` | Column sort picker (`I` inverts sort, `A` sorts by `AGE`) |
| `Mode::ActionMenu` | `t` | Kind-specific action menu (`Topic`, `Connector`, `ConsumerGroup`, `Schema`, `ConnectCluster`, `Namespace`) |
| `Mode::Skins` | `K` or `:skin` | Interactive live-preview picker over all 17 built-in color palettes |
| `Mode::Help` | `?` or `:help` | Scrollable keyboard shortcut & filter syntax reference |
