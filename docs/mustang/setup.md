# Mustang SQL + analytics-engine — Local Setup

End-to-end guide to build and run the OpenSearch SQL plugin on the `feature/mustang-ppl-integration` branch against a local OpenSearch core with the sandbox `analytics-engine` stack enabled.

Tested on Linux x86_64 (Amazon Linux 2), OpenSearch core `3.7.0-SNAPSHOT`, os-sql `3.7.0.0-SNAPSHOT`.

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| JDK | **25** (Corretto or any OpenJDK 25) | Hard requirement — `sandbox/libs/dataformat-native` uses FFM APIs and hard-codes `sourceCompatibility = 25`. |
| Rust / Cargo | 1.85+ | For the native dataformat library. |
| `protoc` | 3.x+ | Substrait crate needs it at build time. |
| curl, python3 | any recent | For the REST harness. |

### Install JDK 25 (user-space, no root)

```bash
mkdir -p $HOME/jdk
curl -fsSL -o /tmp/corretto-25.tar.gz \
  https://corretto.aws/downloads/latest/amazon-corretto-25-x64-linux-jdk.tar.gz
tar -xzf /tmp/corretto-25.tar.gz -C $HOME/jdk/
rm /tmp/corretto-25.tar.gz

export JAVA_HOME=$(ls -d $HOME/jdk/amazon-corretto-25*)
export PATH="$JAVA_HOME/bin:$PATH"
java --version   # should print 25.x.x
```

### Install `protoc` (if missing)

```bash
mkdir -p ~/.local/bin
curl -fsSL -o /tmp/protoc.zip \
  https://github.com/protocolbuffers/protobuf/releases/download/v25.3/protoc-25.3-linux-x86_64.zip
unzip -o /tmp/protoc.zip -d /tmp/protoc
cp /tmp/protoc/bin/protoc ~/.local/bin/
rm -rf /tmp/protoc /tmp/protoc.zip
export PATH="$HOME/.local/bin:$PATH"
```

---

## 2. Repository layout

Two repos, side by side:

```
~/oss/
├── OpenSearch/        ← github.com/opensearch-project/OpenSearch (main)
│   └── sandbox/       ← contains analytics-engine, parquet-data-format, etc.
└── os-sql/            ← github.com/opensearch-project/sql
    └── (branch: feat/mustang  or  feature/mustang-ppl-integration)
```

Clone them at the same level:

```bash
mkdir -p ~/oss && cd ~/oss
git clone https://github.com/opensearch-project/OpenSearch.git
git clone https://github.com/opensearch-project/sql.git os-sql
cd os-sql && git checkout feature/mustang-ppl-integration
```

> If a `feat/mustang` branch exists, use that — it includes the force-routing test harness patch (`plugins.calcite.analytics.force_routing`).

---

## 3. Build

### 3a. Rust native library (for DataFusion / parquet backend)

```bash
cd ~/oss/OpenSearch/sandbox/libs/dataformat-native/rust
cargo build --release
# produces: target/release/libopensearch_native.so
```

Takes ~10 min on first build. Re-runs are incremental.

### 3b. Publish OpenSearch core + sandbox plugins to Maven local

```bash
export JAVA_HOME=$(ls -d $HOME/jdk/amazon-corretto-25*)
export PATH="$JAVA_HOME/bin:$HOME/.local/bin:$PATH"

cd ~/oss/OpenSearch
./gradlew publishToMavenLocal \
  -Dsandbox.enabled=true \
  -Dorg.gradle.java.installations.paths=$JAVA_HOME \
  --no-daemon
```

Takes ~3 min. Publishes `org.opensearch:*` and `org.opensearch.sandbox:*` artifacts at `3.7.0-SNAPSHOT` into `~/.m2/repository/`, and builds all 11 sandbox plugin ZIPs under `sandbox/plugins/<name>/build/distributions/`.

**Gotcha**: `-Dsandbox.enabled=true` is mandatory. Without it, the sandbox modules are excluded from the build tree and no analytics-engine artifact is produced.

### 3c. Publish os-sql to Maven local

```bash
cd ~/oss/os-sql

# The mustang branch's libs/ directory already contains the expected
# analytics-engine/analytics-framework JARs. Refresh them from the core
# build you just did (optional but ensures ABI match):
cp ~/oss/OpenSearch/sandbox/plugins/analytics-engine/build/distributions/analytics-engine-3.7.0-SNAPSHOT.jar libs/
cp ~/oss/OpenSearch/sandbox/plugins/analytics-engine/build/distributions/analytics-engine-3.7.0-SNAPSHOT.zip libs/
cp ~/oss/OpenSearch/sandbox/libs/analytics-framework/build/distributions/analytics-framework-3.7.0-SNAPSHOT.jar libs/

./gradlew publishToMavenLocal \
  -PhasAnalyticsEngine=true \
  -Dorg.gradle.java.installations.paths=$JAVA_HOME \
  --no-daemon
```

Takes ~30 s. Produces `opensearch-sql-plugin-3.7.0.0-SNAPSHOT.zip` at:
- `plugin/build/distributions/opensearch-sql-3.7.0.0-SNAPSHOT.zip` (raw output)
- `~/.m2/repository/org/opensearch/plugin/opensearch-sql-plugin/3.7.0.0-SNAPSHOT/` (published)

---

## 4. Run the dev cluster

```bash
export JAVA_HOME=$(ls -d $HOME/jdk/amazon-corretto-25*)
export PATH="$JAVA_HOME/bin:$HOME/.local/bin:$PATH"

cd ~/oss/OpenSearch
./gradlew run \
  -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT','analytics-engine','parquet-data-format','analytics-backend-datafusion','analytics-backend-lucene','composite-engine','opensearch-sql-plugin:3.7.0.0-SNAPSHOT']"
```

Cluster listens on:
- REST: `http://localhost:9200`
- Transport: `localhost:9300`
- Cluster name: `runTask`

### Plugin install order matters

`opensearch-sql-plugin` declares `extendedPlugins = ['opensearch-job-scheduler', 'analytics-engine']`. The plugin installer validates dependencies at install time, so **job-scheduler and analytics-engine MUST be earlier in the list** than `opensearch-sql-plugin`. Reordering to put SQL first yields:

```
Missing plugin [analytics-engine], dependency of [opensearch-sql]
```

### What `./gradlew run` auto-handles

`gradle/run.gradle` detects `parquet-data-format` / `analytics-backend-datafusion` in the plugin list and automatically adds:

- `-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true`
- `-Djava.library.path=.../sandbox/libs/dataformat-native/rust/target/release`
- `--add-opens=java.base/java.nio=ALL-UNNAMED`
- `--enable-native-access=ALL-UNNAMED`

So you do **not** need to pass `-Dtests.jvm.argline="..."` as some earlier docs suggest.

### Running in the background

```bash
cd ~/oss/OpenSearch
nohup ./gradlew run -Dsandbox.enabled=true -PinstalledPlugins="..." > cluster.log 2>&1 &
echo $! > cluster.pid

# wait for readiness
for i in $(seq 1 60); do
  curl -sf http://localhost:9200/ >/dev/null 2>&1 && { echo "UP after $((i*5))s"; break; }
  sleep 5
done

# kill when done
kill $(cat cluster.pid)
```

---

## 5. The three-layer settings model

To run a query through the analytics-engine path, **all three layers** must be satisfied:

### Layer 1 — Node JVM flags (handled automatically by `./gradlew run` above)

| Flag | Why |
|---|---|
| `opensearch.experimental.feature.pluggable.dataformat.enabled=true` | Unlocks the index-level `pluggable.dataformat.*` settings. |
| `java.library.path=.../rust/target/release` | DataFusion backend loads `libopensearch_native.so` via JNI. |
| `--add-opens=java.base/java.nio=ALL-UNNAMED`, `--enable-native-access=ALL-UNNAMED` | FFM requirements. |

### Layer 2 — Index settings (per-index, at create time)

```json
PUT /hits
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "pluggable.dataformat.enabled": true,
    "pluggable.dataformat": "composite",
    "composite.primary_data_format": "parquet"
  },
  "mappings": { "properties": { ... } }
}
```

All four settings are required together. Missing any one → silent fallback to pure Lucene, and the analytics-engine path fails later with `No backend can scan all requested fields on index`.

**`number_of_shards` MUST be 1** — the parquet backend only supports single-shard indices today.

### Layer 3 — Query routing (per-query decision)

`RestUnifiedQueryAction.isAnalyticsIndex()` picks the path. Two ways to route to analytics-engine:

| Mode | How | Scope |
|---|---|---|
| **Prefix heuristic (default)** | Index name's last `.`-segment begins with `parquet_` (e.g. `parquet_logs`, `catalog.parquet_hits`) | Per-query |
| **Force routing** | `PUT /_cluster/settings {"persistent":{"plugins.calcite.analytics.force_routing":"true"}}` | Cluster-wide, every query |

The `plugins.calcite.analytics.force_routing` setting is added by the mustang test harness (commit 29339c9c). Used by the `analyticsCompatibilityReport` gradle task to measure analytics-engine coverage of the full PPL test suite.

---

## 6. Smoke test

### 6a. Create a parquet-backed index

```bash
curl -sS -X PUT http://localhost:9200/parquet_logs \
  -H 'Content-Type: application/json' -d '{
    "settings": {
      "number_of_shards": 1, "number_of_replicas": 0,
      "pluggable.dataformat.enabled": true,
      "pluggable.dataformat": "composite",
      "composite.primary_data_format": "parquet"
    },
    "mappings": {"properties": {
      "ts":{"type":"date"}, "status":{"type":"integer"}, "msg":{"type":"keyword"}
    }}
  }'
```

### 6b. Index a document

```bash
curl -sS -X POST http://localhost:9200/parquet_logs/_doc?refresh=true \
  -H 'Content-Type: application/json' \
  -d '{"ts":"2026-05-01T10:30:00Z","status":200,"msg":"hello"}'
```

### 6c. Query via PPL (routes through analytics-engine thanks to `parquet_` prefix)

```bash
curl -sS -X POST http://localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source = parquet_logs | stats count()"}'
```

Expected response:
```json
{"schema":[{"name":"count()","type":"bigint"}],"datarows":[[1]],"total":1,"size":1}
```

### 6d. Force-routing a non-`parquet_` index

```bash
# enable force routing
curl -sS -X PUT http://localhost:9200/_cluster/settings \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.analytics.force_routing":"true"}}'

# now a query on `hits` (no parquet_ prefix) also routes to analytics-engine
curl -sS -X POST http://localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source = hits | stats count()"}'
```

### 6e. Confirm the path taken

If the query reaches analytics-engine, the cluster log shows:

```
o.o.a.p.PlannerImpl [runTask-0] Input RelNode:
LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])
  ...
```

If instead you see `SearchPhaseExecutionException: all shards failed` on a parquet-backed index, your query went through the Lucene SQL path (Layer 3 was not satisfied).

---

## 7. Running ClickBench via shell (no JUnit)

The os-sql repo ships mapping + data + 43 PPL queries under `integ-test/src/test/resources/clickbench/`. A shell harness is at `docs/mustang/scripts/clickbench.sh`:

```bash
cd docs/mustang/scripts

# parquet-backed index + force routing → all queries through analytics-engine
PARQUET=1 FORCE_ROUTING=1 ./clickbench.sh setup
FORCE_ROUTING=1 ./clickbench.sh run
```

See the script header for full flag reference. On the 1-row sample dataset, expect ~20/43 to pass; failures map to real analytics-engine compatibility gaps (`UnsupportedOperationException`, `IllegalStateException`, `IllegalArgumentException`).

---

## 8. Running the integ test suite (alternative to shell)

Per the Quip doc, step 5:

```bash
cd ~/oss/os-sql
./gradlew :integ-test:integTestRemote \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  --tests "org.opensearch.sql.calcite.remote.CalcitePPLAggregationIT"
```

### Analytics compatibility report (full suite)

```bash
./gradlew :integ-test:analyticsCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask
```

Sets `tests.analytics.force_routing=true` → flips `plugins.calcite.analytics.force_routing` on the cluster → every test query goes through analytics-engine → markdown report at `integ-test/build/reports/analytics-compatibility/REPORT.md` bucketed by exception type.

`ignoreFailures = true` — the task never fails the build; failures **are** the signal.

---

## 9. Known issues on this branch

| # | Symptom | Cause | Workaround |
|---|---|---|---|
| 1 | `NoClassDefFoundError: org/apache/commons/text/similarity/LevenshteinDistance` on any aggregation query | `commons-text` is excluded from the SQL plugin bundle (likely over-exclusion in `e113b7f91 "Exclude httpcore5-* from SQL bundle"`) | Add `commons-text` back via `plugin/build.gradle` `bundlePlugin { include ... }` |
| 2 | Tests that wait for refresh hang indefinitely | Known limitation of the composite-engine refresh path | Skip those tests |
| 3 | `SearchPhaseExecutionException: all shards failed` on parquet-backed index | Layer 3 not satisfied — query went to Lucene SQL path which can't scan parquet shards | Prefix index with `parquet_` OR enable `force_routing` |
| 4 | `No backend can scan all requested fields on index [X]` | Index schema has no matching registered `AnalyticsSearchBackendPlugin` scanner | Ensure `analytics-backend-datafusion` and `analytics-backend-lucene` plugins are installed |
| 5 | `error: release version 25 not supported` during core build | JDK 21 active | Install JDK 25 (Section 1) |

---

## 10. Quick reference

```bash
# One-shot build & launch (everything from scratch)
export JAVA_HOME=$(ls -d $HOME/jdk/amazon-corretto-25*)
export PATH="$JAVA_HOME/bin:$HOME/.local/bin:$PATH"

cd ~/oss/OpenSearch/sandbox/libs/dataformat-native/rust && cargo build --release
cd ~/oss/OpenSearch && ./gradlew publishToMavenLocal -Dsandbox.enabled=true --no-daemon
cd ~/oss/os-sql && ./gradlew publishToMavenLocal -PhasAnalyticsEngine=true --no-daemon
cd ~/oss/OpenSearch && ./gradlew run -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT','analytics-engine','parquet-data-format','analytics-backend-datafusion','analytics-backend-lucene','composite-engine','opensearch-sql-plugin:3.7.0.0-SNAPSHOT']"
```

## References

- Upstream doc: https://quip-amazon.com/Nnw1AZO8dRLg/Mustang-SQL-plugin-testing-steps
- Integration commits (os-sql):
  - `24dd79c99` — Wire analytics-engine as extendedPlugins dependency
  - `5fa54c3aa` — Integrate SQL REST endpoint with analytics engine path
  - `5c5ffa2e2` — Version bump to OpenSearch 3.7 with async QueryPlanExecutor
  - `29339c9c0` — Add PPL IT coverage report (force_routing setting) *(not yet merged; patch available)*
