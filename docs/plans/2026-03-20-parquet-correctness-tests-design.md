# Parquet DocValues Correctness Tests — Design

## Goal
Verify parquet doc_values format produces identical search/aggregation results as the default format by reusing the existing 56 YAML REST aggregation tests with parquet enabled.

## Approach
- New Gradle task `yamlRestTestParquet` in `rest-api-spec/build.gradle`
- Copy all 56 YAML files from `search.aggregation/` into a parallel source set
- In each copied file, add `index.codec.doc_values.format: parquet` to every `indices.create` settings block
- Keep all expected results unchanged — if any test fails, it's a parquet regression
- Enable the parquet feature flag on the test cluster

## File Structure
```
rest-api-spec/
├── build.gradle                          # Add yamlRestTestParquet task + cluster config
├── src/main/resources/rest-api-spec/test/search.aggregation/   # Original (unchanged)
│   ├── 20_terms.yml
│   └── ... (56 files)
└── src/yamlRestTestParquet/resources/rest-api-spec/test/search.aggregation/  # Parquet copies
    ├── 20_terms.yml                      # Same + parquet index setting
    └── ... (56 files)
```

## build.gradle Changes
- Register `yamlRestTestParquet` source set and task (RestIntegTestTask)
- Configure test cluster with feature flag: `systemProperty 'opensearch.experimental.feature.parquet_doc_values.enabled', 'true'`
- Include mapper-extras module (same as existing yamlRestTest)

## Per-File Modification
Every `indices.create` call gets the parquet setting:
```yaml
indices.create:
    index: test_1
    body:
      settings:
        number_of_shards: 1
        index.codec.doc_values.format: parquet
```

## How to Run
```bash
./gradlew :rest-api-spec:yamlRestTestParquet
```

## Success Criteria
- All 56 tests pass with parquet enabled
- Zero expected result changes
- Runs as part of `./gradlew :rest-api-spec:check`
