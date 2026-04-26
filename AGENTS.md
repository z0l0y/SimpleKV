# AGENTS.md

This file provides operational instructions for AI coding agents working in this repository.

## 1. Project Snapshot

- Project: `SimpleKV`
- Stack: `Java 25`, `Spring Boot 4`, `Maven` multi-module
- Core architecture: LSM KV engine (`api` / `storage` / `core` / `app` / `benchmark` / `client`)
- Runtime model: single-node storage engine + CLI + SKSP server/client

## 2. Repository Layout

- `simplekv-api`: public interfaces and models (`model`, `options`, `write`)
- `simplekv-storage`: WAL, MemTable, SSTable, Manifest, Bloom, cache, compaction
- `simplekv-core`: engine orchestration (recovery, flush, compaction scheduling)
- `simplekv-app`: Spring Boot 4 application, CLI, diagnostics, native-image hints
- `simplekv-benchmark`: JMH benchmark entry points
- `simplekv-client`: Java SKSP client and integration tests

## 3. Non-Negotiable Rules

- Use English for all logs, command output text, and new docs.
- Do not add Grafana, Prometheus, Micrometer, or OpenTelemetry exporters.
- Prefer detailed logs and CLI diagnostics over metrics instrumentation.
- Use shell scripts (`.sh`) for documented build/run flows; do not introduce `.ps1`.
- Keep documentation concise and practical; avoid long narrative sections.

## 4. Build and Test Commands

Run from repository root:

```bash
mvn -pl simplekv-app -am test
mvn -pl simplekv-app -am verify
mvn -pl simplekv-api,simplekv-storage,simplekv-core,simplekv-app -am test
mvn -pl simplekv-client test
mvn -DskipTests package
```

Native and CLI scripts:

```bash
./scripts/run-cli.sh ./data -- stats
./scripts/build-native.sh ./data
```

## 5. Coding Conventions

- Prefer simple, explicit code over clever abstractions.
- Keep module boundaries clean; avoid cross-layer leakage.
- Follow existing package naming and class style.
- Add/adjust tests when behavior changes.
- Preserve backward compatibility unless explicitly requested.
- Avoid broad refactors unrelated to the task.

## 6. Spring Boot 4 and Java 25 Guidance

- Keep Spring config lightweight (`@Configuration(proxyBeanMethods = false)` where appropriate).
- Use modern Java features when they improve clarity (for example, switch expressions).
- Do not introduce features that reduce runtime portability or test stability.

## 7. Storage and Compaction Guidance

- `CompactionStyle` must support:
  - `LEVELED`
  - `SIZE_TIERED`
  - compatibility alias input: `size-tired` -> `SIZE_TIERED`
- Ensure configured compaction style flows through:
  - `StorageOptions -> SimpleKvEngine -> LsmCompactor`

## 8. Observability and Diagnostics

- Keep structured English logs for:
  - recovery lifecycle
  - compaction start/end with context
  - slow-query related events
- Keep CLI diagnostic commands functional:
  - `stats`, `sst-dump`, `recover`, `compact`, `trace-key`, `wal-tail`, `sst-inspect`, `manifest-inspect`

## 9. Agent Workflow Expectations

- Before editing, inspect related modules and tests.
- After edits, run targeted tests first, then broader module tests as needed.
- If toolchain issues appear (for example wrong JDK), switch to Java 25 and rerun.
- Keep changes scoped to the request; document only what changed.

## 10. Definition of Done

A change is done when all of the following are true:

- Code compiles in affected modules.
- Relevant tests pass.
- No conflicting docs or stale command references remain.
- Logs/messages are English.
- No forbidden observability dependencies are introduced.
