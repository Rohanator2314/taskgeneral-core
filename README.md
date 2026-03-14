# taskgeneral-core

A platform-agnostic Rust core library for [TaskGeneral](https://github.com/Rohanator2314/taskgeneral-android) — an Android client for Taskwarrior 3.x.

This crate wraps [TaskChampion 3.x](https://crates.io/crates/taskchampion) and exposes a [UniFFI](https://mozilla.github.io/uniffi-rs/)-annotated API so that any target platform (Android, desktop, web via WASM) can generate native language bindings from the same Rust code.

## What this crate provides

- Full task CRUD (create, read, update, delete, complete, uncomplete)
- Due dates, wait dates, and recurrence fields
- Start / stop tasks with active-task tracking
- Urgency scoring — Taskwarrior-compatible formula
- Sorted task listing by urgency, due date, priority, creation date, or description
- Filtering by status, project, and tags
- Sync with [taskchampion-sync-server](https://github.com/GothenburgBitFactory/taskchampion-sync-server)
- Offline-first SQLite storage via TaskChampion

## Using this crate

### As a Rust library

```toml
[dependencies]
taskgeneral-core = { git = "https://github.com/Rohanator2314/taskgeneral-core.git" }
```

### Generating bindings for another platform

UniFFI can generate bindings for Kotlin, Swift, Python, and more. See the [UniFFI docs](https://mozilla.github.io/uniffi-rs/) for the full list of supported languages.

**Kotlin (Android):**
```bash
cargo build
cargo run --bin uniffi-bindgen generate src/lib.udl --language kotlin --out-dir ./bindings/kotlin
```

**Swift (iOS/macOS):**
```bash
cargo build
cargo run --bin uniffi-bindgen generate src/lib.udl --language swift --out-dir ./bindings/swift
```

**Python:**
```bash
cargo build
cargo run --bin uniffi-bindgen generate src/lib.udl --language python --out-dir ./bindings/python
```

## Building

### Prerequisites

- **Rust** 1.88.0 or later (MSRV — required by TaskChampion)

### Run tests

```bash
cargo test
```

### Run linter

```bash
cargo clippy -- -D warnings
```

## Project structure

```
src/
├── lib.rs           # UniFFI scaffolding + public API surface (TaskManagerWrapper)
├── task_manager.rs  # TaskManager — wraps TaskChampion storage and sync
├── models.rs        # Data models (TaskInfo, TaskFilter, TaskUpdate, SortField, etc.)
└── error.rs         # Error types (CoreError, propagated through UniFFI as enums)
```

## Platform consumers

| Platform | Repository |
|---|---|
| Android (Jetpack Compose) | [taskgeneral-android](https://github.com/Rohanator2314/taskgeneral-android) |

PRs adding support for other platforms (desktop via Tauri, web via WASM, iOS via Swift) are welcome.

## License

Apache 2.0 — see [LICENSE](../LICENSE) in the parent repository, or the [SPDX identifier](https://spdx.org/licenses/Apache-2.0.html).
