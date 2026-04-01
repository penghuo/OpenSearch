# Parquet DocValues Correctness Tests — Implementation Plan

Design: `docs/plans/2026-03-20-parquet-correctness-tests-design.md`

## Step 1: Modify `rest-api-spec/build.gradle`

Add after the existing `yamlRestTest` block:

```groovy
// --- Parquet DocValues correctness tests ---
// Reuses the same aggregation tests with parquet doc_values format enabled.
// If any test fails, it means parquet produces different results than the default format.

sourceSets {
  yamlRestTestParquet {
    compileClasspath += sourceSets.yamlRestTest.compileClasspath
    runtimeClasspath += sourceSets.yamlRestTest.runtimeClasspath
  }
}

tasks.register("yamlRestTestParquet", RestIntegTestTask) {
  description = "Runs YAML REST tests with parquet doc_values format"
  testClassesDirs = sourceSets.yamlRestTestParquet.output.classesDirs
  classpath = sourceSets.yamlRestTestParquet.runtimeClasspath
}

testClusters.yamlRestTestParquet {
  module ':modules:mapper-extras'
  systemProperty 'opensearch.experimental.feature.parquet_doc_values.enabled', 'true'
}
```

Note: May need to import `RestIntegTestTask` and wire up the REST test runner class.
Look at how `YamlRestTestPlugin` registers the standard task and replicate for the parquet variant.

**Verify**: `./gradlew :rest-api-spec:tasks` shows `yamlRestTestParquet` task.

## Step 2: Copy YAML test files

```bash
mkdir -p rest-api-spec/src/yamlRestTestParquet/resources/rest-api-spec/test/search.aggregation
cp rest-api-spec/src/main/resources/rest-api-spec/test/search.aggregation/*.yml \
   rest-api-spec/src/yamlRestTestParquet/resources/rest-api-spec/test/search.aggregation/
```

**Verify**: 56 files copied.

## Step 3: Add parquet index setting to each copied file

For every `indices.create` block in every copied YAML file, add:
```yaml
        index.codec.doc_values.format: parquet
```
under the `settings:` key.

Rules:
- If `settings:` exists, add the line under it
- If `settings:` doesn't exist, add `settings:` block with the parquet line
- Preserve all other content exactly as-is
- Do NOT change any expected results or assertions

**Verify**: `grep -rL "index.codec.doc_values.format" rest-api-spec/src/yamlRestTestParquet/` returns no files (all files have the setting).

## Step 4: Run and verify

```bash
./gradlew :rest-api-spec:yamlRestTestParquet
```

**Success**: All tests pass. Any failure = parquet regression to investigate.

## Step 5: Wire into check lifecycle (optional)

Add to build.gradle:
```groovy
check.dependsOn yamlRestTestParquet
```

So `./gradlew :rest-api-spec:check` runs both standard and parquet tests.
