# SimpleKV (TL;DR)

SimpleKV is a single-node LSM KV engine based on **Java 25** and **Spring Boot 4**.

## Quick Facts

- Runtime: Java 25+
- Build: Maven 3.9+
- Architecture: `api` / `storage` / `core` / `app` / `benchmark` / `client`
- Observability: **No Grafana / Prometheus / Micrometer exporters**
- Ops style: detailed **English logs** + CLI diagnostics

## Build

```bash
mvn -pl simplekv-app -am test
mvn -pl simplekv-app -am verify
mvn -DskipTests package
```

## Run

Use `sh` scripts only:

```bash
./scripts/run-cli.sh ./data -- stats
./scripts/run-cli.sh ./data -- put demo-key demo-value
./scripts/run-cli.sh ./data -- get demo-key
```

Build native:

```bash
./scripts/build-native.sh ./data
```

## Core CLI Commands

- Data: `put`, `ttl-put`, `get`, `delete`, `scan`, `prefix`
- Diagnose: `stats`, `sst-dump`, `recover`, `compact`, `trace-key`, `wal-tail`, `sst-inspect`, `manifest-inspect`

## Storage Config

Main config file:

- `simplekv-app/src/main/resources/application.yml`

Important keys:

- `simplekv.storage.data-dir`
- `simplekv.storage.mem-table-max-entries`
- `simplekv.storage.cache-max-entries`
- `simplekv.storage.level0-compaction-trigger`
- `simplekv.storage.level0-max-files`
- `simplekv.storage.flush-batch-size`
- `simplekv.storage.tombstone-retention-seconds`
- `simplekv.storage.compaction-style`

## Compaction Modes

- `leveled` -> `LEVELED`
- `size-tiered` -> `SIZE_TIERED`
- `size-tired` -> compatibility alias -> `SIZE_TIERED`

Compaction style is applied end-to-end:

`StorageOptions -> SimpleKvEngine -> LsmCompactor`

## Notes

- Keep logs in English.
- Keep docs concise; this file is intentionally short.
- For detailed internals, check module source code directly.
