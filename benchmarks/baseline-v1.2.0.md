# RuseDB Baseline (v1.2.0)

Date: 2026-03-10

## Environment

- OS: Windows (PowerShell)
- Build profile: debug
- Command baseline captured from local developer machine.

## Baseline Commands

```powershell
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Recorded Results

| Command | Result |
|---|---|
| `cargo check --workspace` | pass |
| `cargo test --workspace` | pass, full suite ~21s |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass |

## Notes

- This baseline is used as regression guard, not as an absolute performance target.
- Next phase can add workload-level query throughput/latency benchmarks.
