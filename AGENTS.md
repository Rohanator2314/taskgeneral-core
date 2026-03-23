# AGENTS.md

## Pre-push Checklist

Before every push, run:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test --features postgres
```
