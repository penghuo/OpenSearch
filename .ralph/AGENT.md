# Agent Notes

## Build
```bash
./gradlew assemble                    # build all distributions
./gradlew localDistro                 # build for local platform only
./gradlew :server:precommit           # precommit checks for server module
./gradlew spotlessApply               # fix Java formatting (4-space indent, 140-char line width, no wildcard imports)
./gradlew :server:updateSHAs          # regenerate SHA files after adding dependencies
```

## Test
```bash
# Unit tests
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.*" -Dtests.iters=10

# Conformance tests
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetDocValuesFormatTests" -Dtests.iters=3

# All codec tests (regression check)
./gradlew :server:test --tests "org.opensearch.index.codec.*" -Dtests.iters=3

# Integration tests
./gradlew :server:internalClusterTest --tests "org.opensearch.index.codec.parquet.ParquetDocValuesIT" -Dtests.iters=3

# Full server check
./gradlew :server:check

# Specific test with seed for reproducibility
./gradlew :server:test --tests "*.ParquetDocValuesFormatTests" -Dtests.seed=DEADBEEF

# Benchmark
opensearch-benchmark execute-test --workload http_logs --target-hosts localhost:9200 --pipeline benchmark-only
```

## Code Style
- Java formatting: Eclipse JDT via Spotless. Run `./gradlew spotlessApply` to fix.
- 4-space indent, 140-character line width.
- Wildcard imports forbidden.
- Prefer `foo == false` over `!foo`.
- Test class names must end with "Tests" (unit) or "IT" (integration).
- Unit test base class: `OpenSearchTestCase`
- Integration test base class: `OpenSearchIntegTestCase`

## Dependencies
When adding dependencies to `build.gradle`:
1. Copy LICENSE.txt and NOTICE.txt to `server/licenses/<artifact>-LICENSE.txt` and `<artifact>-NOTICE.txt`
2. Run `./gradlew :server:updateSHAs`
3. Verify with `./gradlew :server:check`

## Key Codebase Patterns
- Feature flags: `FeatureFlags.java` — add flag + setting, register in `FeatureFlagsImpl` map
- Index settings: `IndexSettings.java` — add `Setting<>`, field, accessor. Register in `IndexScopedSettings.java`
- DocValues format: `PerFieldMappingPostingFormatCodec.getDocValuesFormatForField()` — returns format per field
- _source skip: `SourceFieldMapper.preParse()` — check setting, return early to skip storage
- _source reconstruction: `IndexShard` — wrap with `DerivedSourceDirectoryReader`
- Conformance testing: Extend `BaseDocValuesFormatTestCase` (see `AbstractStarTreeDVFormatTests` for example)

## Completion Criteria
Benchmark with opensearch-benchmark http_logs workload:
1. Correctness: 100% pass
2. Ingestion throughput: 2x improvement over baseline
3. Query performance: within 10% of baseline
4. Storage size: 70% reduction from baseline

## Notes
- Project created from plan: `docs/plans/2026-03-18-parquet-docvalues-plan.md`
- Design doc: `docs/plans/2026-03-18-parquet-docvalues-design.md`
- JDK 21 minimum. `JAVA_HOME` must be set.
- Run `./gradlew precommit` before committing.
