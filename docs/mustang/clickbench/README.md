# ClickBench resources for Mustang

Vendored from the upstream OpenSearch SQL plugin test resources:

**Source**: https://github.com/opensearch-project/sql
**Path**:   `integ-test/src/test/resources/clickbench/`
**License**: Apache-2.0 (same as this repo)

These files are referenced by `docs/mustang/scripts/clickbench.sh` so the
harness is self-contained — you do not need a local checkout of the
`os-sql` repo to run it.

## Contents

| Path | Count | Purpose |
|---|---|---|
| `mappings/clickbench_index_mapping.json` | 1 | OpenSearch mapping for the `hits` index |
| `data/clickbench.json` | 1 | Sample bulk-indexable document (1 row) |
| `queries/q{1..43}.ppl` | 43 | ClickBench PPL queries with SQL originals in `/* … */` header |

## Refreshing from upstream

If the upstream SQL repo updates these resources, refresh with:

```bash
OS_SQL=/path/to/os-sql
cp $OS_SQL/integ-test/src/test/resources/clickbench/mappings/*.json docs/mustang/clickbench/mappings/
cp $OS_SQL/integ-test/src/test/resources/clickbench/data/*.json     docs/mustang/clickbench/data/
cp $OS_SQL/integ-test/src/test/resources/clickbench/queries/*.ppl   docs/mustang/clickbench/queries/
```

Or point the harness at a live os-sql tree without copying:

```bash
OS_SQL_DIR=$OS_SQL docs/mustang/scripts/clickbench.sh setup
```

## Reference

- ClickBench upstream project: https://github.com/ClickHouse/ClickBench
- OpenSearch SQL plugin tests: `PPLClickBenchIT` / `CalcitePPLClickBenchIT`
