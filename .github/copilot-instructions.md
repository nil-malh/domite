# domite — Project Context & Agent Instructions

`domite` is a terminal UI (TUI) in Rust (Edition 2024, `ratatui` 0.30, `crossterm` 0.29, `tokio`, `reqwest`) for [michelin/ns4kafka](https://github.com/michelin/ns4kafka), modelled after the architecture and UX of [nklmilojevic/sofka](https://github.com/nklmilojevic/sofka) and fully compatible with [michelin/kafkactl](https://github.com/michelin/kafkactl) configuration files.

See [`.github/architecture.md`](./architecture.md) for the deep-dive module map, data flow, and ns4kafka API quirks.

---

## Core Design Principles

1. **One generic object pipeline (`serde_json::Value`)**:
   - Every ns4kafka resource is stored in `Store` (`src/store.rs`) as a dynamic `serde_json::Value` keyed by a composite `RowKey` (`src/store.rs::row_key`).
   - Curated columns per kind live in `src/columns.rs` (`Topic`, `Connector`, `Schema`, `AccessControlEntry`, `ConsumerGroup`, `KafkaStream`, `ResourceQuota`, `ConnectCluster`, `RoleBinding`, `Namespace`) with a `NAME / AGE` fallback for any unknown kind and user-configurable JSON-Pointer column overrides via `~/.config/domite/config.toml` (`[views.<kind>]`).
2. **Async-first, non-blocking UI loop**:
   - Background tasks (the view poller, background schema hydrator, dry-run validator, and write executors) communicate with the UI exclusively through generation-tagged `Msg` variants (`Applied`, `Deleted`, `Synced`, `PollError`, `Discovered`, `NamespacesLoaded`, `DryRunReady`, `ActionComplete`) over an unbounded `mpsc` channel.
   - `App::handle_msgs` (`src/app/mod.rs`) drains and applies queued message batches before calling `rebuild_rows()` once per frame, keeping 1,000+ resource namespaces instantaneous.
   - Switching resource kind, namespace, or context increments `app.generation`, so late responses from previous views or hydration tasks are dropped automatically.
3. **Important ns4kafka Specifics**:
   - **TLS & Certificates**: Internal Michelin ns4kafka endpoints (`https://kfe-namespace-*.michelin.com`) use internal corporate CAs installed in the OS certificate store. Always keep `rustls-tls-native-roots` enabled on `reqwest` in `Cargo.toml`.
   - **`kafkactl` Config & JWT Compatibility**:
     - Reads `~/.kafkactl/config.yml` (`src/api/config.rs`). Note that `kafkactl use-context` writes the active `api`, `current-namespace`, and `user-token` at the top level of `kafkactl:` rather than setting `current-context`; `parse_kafkactl_yaml` resolves the active context by matching `(api, current-namespace)` against `kafkactl.contexts`.
     - Reuses unexpired JWTs from both `~/.cache/domite/jwt/<ctx>.jwt` (`0600` permissions) and `~/.kafkactl/jwt`, and decodes JWT payload claims (`sub`, `roles`, `exp`) directly.
   - **Schema Registry Lazy Loading & Long-Lived Disk Cache (`src/api/cache.rs`, `src/api/poller.rs`)**:
     - In `ns4kafka`, `GET /api/namespaces/{ns}/schemas` (`name=*`) only returns stub `Schema` objects (`metadata.name` only, `spec == null`) whenever more than 1 subject exists, to avoid querying Schema Registry for every subject synchronously.
     - `PersistentCache` (`src/api/cache.rs`) persists all resource view snapshots (`~/.cache/domite/views/<ctx>/<ns>__<path>.json`), namespace lists (`_namespaces.json`), and hydrated Schema Registry specs (`~/.cache/domite/schemas/<ctx>.json`) across app restarts via a coalescing background writer thread using `atomicfile::write`.
     - **Stale-while-revalidate**: On startup or view/namespace/context switch, `App::seed_store_from_cache` immediately populates `Store` from disk (`store.synced = false`) so the UI is populated on frame 1 while the background poller fetches live state (`Msg::Reset` buffers the incoming list and `Msg::Synced` atomically swaps it in).
     - Hydrated `Schema` entries carry a `fetched_at` timestamp (default TTL: `schema_cache_ttl_hours = 24`). Fresh cached schemas skip network hydration entirely; expired or `r`-refreshed schemas continue displaying their cached `VERSION`/`ID`/`COMPATIBILITY` immediately while `spawn_schema_hydrator` revalidates them in the background (prioritizing completely uncached subjects first and ordering by proximity to `cursor_hint`).
     - Pressing `Enter` on a `Schema` opens a pretty-printed schema definition view (`format_schema_pretty`), while `y` opens the full ns4kafka `Schema` YAML resource. If a row is not yet hydrated, `fetch_schema_and_open` fetches it on demand immediately.
   - **Resource Status Location**:
     - Unlike Kubernetes, ns4kafka resource deployment status lives under `/metadata/status/phase` (`Success`, `Pending`, `Fail`, `Deleting`) and `/metadata/status/message`.
     - `Connector` additionally has a top-level `/status/state` (`RUNNING`, `PAUSED`, `FAILED`, `UNASSIGNED`) and `/status/tasks[]` (with `trace` on failed tasks). `row_status` in `src/columns.rs` checks both `/status/state` and whether any task in `/status/tasks` is `FAILED`.
   - **Two-Level `ConsumerGroup` Pipeline, External Groups & Background Hydration**:
     - In `ns4kafka`, `GET /api/namespaces/{ns}/consumer-groups` (`name=*`) only sets `includeOffsets = (consumerGroupIds.size() == 1)` — when more than 1 group exists, it returns stub `(name, state)` objects without topic/partition/offset/lag, whereas `GET .../consumer-groups?name={group}` and `GET .../consumer-groups/_/external` return full partition offsets.
     - `client::aggregate_consumer_groups` (`src/api/client.rs`) aggregates partition items into one summary row per group (`NAME`, `OWNERSHIP` [`Owned` / `External`], `STATE`, `TOPICS`, `PARTITIONS`, `TOTAL-LAG`, `MAX-LAG`) and merges both owned and external (`/consumer-groups/_/external`) groups (`:cg` shows both; `:cg-ext` shows only external groups).
     - `poller::spawn_cg_hydrator` (`src/api/poller.rs`) hydrates owned groups in the background via `GET /api/namespaces/{ns}/consumer-groups?name={group}` with bounded concurrency, ordered by proximity to `cursor_hint`.
     - Pressing `Enter` on a `ConsumerGroup` drills down into `ConsumerGroupMember` (`GROUP`, `STATE`, `TOPIC`, `PARTITION`, `CURRENT-OFFSET`, `END-OFFSET`, `LAG`), where `t` can reset offsets on a specific `topic:partition` (`TO_EARLIEST`, `TO_LATEST`, `TO_DATETIME`, `BY_DURATION`, `TO_OFFSET`, `SHIFT_BY`) or jump back to the `Topic`.
   - **Composite Keys for `ConsumerGroupMember` & `ResourceQuota`**:
     - `ConsumerGroupMember` partition rows are keyed by `store::row_key` as `{ns}/{name}/{topic}/{partition}`.
     - `ResourceQuota` list responses return `ResourceQuotaResponse` with `spec.{countTopic, countPartition, diskTopic, countConnector, producerByteRate, consumerByteRate}` formatted as `"used/limit"`. `expand_quota_rows` (`src/api/client.rs`) expands each metric into its own row with `_quotaKey`, `_quotaValue`, and `_quotaUsed`.
   - **Safe Writes & Read-Only Guardrails**:
     - Any context whose name starts with or contains `prod` (`prod*`, `*-prod`, `*_prod`) defaults to `readonly = true`. Users can toggle with `:write` / `:readonly` or CLI flags `--write` / `--readonly`.
     - All mutating operations (`e` edit in `$EDITOR`, `Ctrl-d` delete, `i` import unsynchronized, topic `delete-records`, consumer group offset `reset`) run through `?dryrun=true` first (`src/app/actions.rs::spawn_dry_run`) and present a unified YAML diff (`Mode::Diff`) or confirmation summary (`Mode::Confirm`) before executing.

---

## Development & Verification Commands

```bash
# Lint with zero warnings allowed
cargo clippy --all-targets -- -D warnings

# Run all unit and integration tests (uses FakeClient, no live cluster required)
cargo test

# Run headless connectivity check against ~/.kafkactl/config.yml (or --demo)
cargo run -- --check
cargo run -- --demo --check

# Render a single 120x32 TUI frame to stdout
cargo run -- --snapshot
cargo run -- --demo --snapshot
```
