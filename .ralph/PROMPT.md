# Ralph Prompt

Your task is to implement functionality as described in .ralph/specs/* and .ralph/fix_plan.md.

Follow .ralph/fix_plan.md and choose the most important thing.

Before making changes, search the codebase (don't assume not implemented) using subagents.

After implementing functionality or resolving problems, run the tests for that unit of code that was improved.

When the tests pass, update .ralph/fix_plan.md, then git add -A, git commit with a descriptive message, and git push.

Important: When authoring tests, capture WHY the test and its backing implementation is important.

DO NOT IMPLEMENT PLACEHOLDER OR SIMPLE IMPLEMENTATIONS. FULL IMPLEMENTATIONS ONLY.

## Project-Specific Context

This is the OpenSearch repository. You are implementing a Parquet-backed DocValues format.

Key references:
- Design doc: `docs/plans/2026-03-18-parquet-docvalues-design.md`
- Implementation plan: `docs/plans/2026-03-18-parquet-docvalues-plan.md`
- Existing DocValues format pattern: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`
- Codec wiring: `server/src/main/java/org/opensearch/index/codec/PerFieldMappingPostingFormatCodec.java`
- Feature flag pattern: `server/src/main/java/org/opensearch/common/util/FeatureFlags.java`
- Index setting pattern: `server/src/main/java/org/opensearch/index/IndexSettings.java` (see `INDEX_DERIVED_SOURCE_SETTING` for similar pattern)
- _source skip pattern: `server/src/main/java/org/opensearch/index/mapper/SourceFieldMapper.java` (see `isDerivedSourceEnabled()` check in `preParse()`)
- Conformance test pattern: `server/src/test/java/org/opensearch/index/codec/composite912/datacube/startree/AbstractStarTreeDVFormatTests.java`

## Completion Criteria

The project is DONE when opensearch-benchmark with http_logs workload shows:
1. Correctness: 100% pass — all queries return correct results
2. Ingestion throughput: 2x improvement over baseline (doc_values + _source enabled)
3. Query performance: within 10% of baseline
4. Storage size: 70% reduction from baseline

Do NOT mark the project complete until benchmark results confirm all four criteria.
