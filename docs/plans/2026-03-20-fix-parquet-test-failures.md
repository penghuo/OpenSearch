# Fix 7 Parquet YAML REST Test Failures

> **For Kiro:** Use `#executing-plans` for sequential execution.

**Goal:** Make all 357 yamlRestTestParquet tests pass (currently 7 failures).

**Strict rule:** `_source` must NEVER be stored when parquet is enabled. All _source reconstruction must come from doc_values.

**Architecture:** Three independent fixes targeting three root causes:
1. ParquetDocValuesReader singleton optimization (1 test)
2. Text field _source reconstruction via BinaryDocValues (4 tests)
3. Nested doc _source reconstruction from child doc values (2 tests)

**Tech Stack:** Java, Lucene DocValues API, Parquet codec, OpenSearch mapper framework

---

### Task 1: Fix global ordinals singleton detection in ParquetDocValuesReader

**Root cause:** `getSortedSet()` always returns a plain `SortedSetDocValues`. `DocValues.unwrapSingleton()` needs `SingletonSortedSetDocValues` to detect single-valued fields.

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesReader.java`

**Step 1: Modify getSortedSet() to detect single-valued fields**

After loading `CachedSortedSet`, check if every doc has exactly 1 ordinal in `cached.docOrds`. If so, build a `SortedDocValues` from the cached data (reuse docIds, extract single ord per doc, reuse dict) and return `DocValues.singleton(sortedDv)`.

Detection: iterate `cached.docOrds` — if all `ords.length == 1`, it's single-valued.

When single-valued, construct an anonymous `SortedDocValues` inline from the CachedSortedSet data:
- `docIds` = same as CachedSortedSet.docIds
- `ords[i]` = `(int) cached.docOrds[i][0]` for each doc
- `dict` = same as CachedSortedSet.dict
- `isDense` = same as CachedSortedSet.isDense

Then wrap: `return DocValues.singleton(sortedDv)`

**Step 2: Run the failing test**

```bash
./gradlew :rest-api-spec:yamlRestTestParquet --tests "*parquet.search.aggregation/20_terms*" 2>&1 | tail -20
```

Expected: All 20_terms tests pass including "string profiler via global ordinals".

**Step 3: Commit**

```bash
git add server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesReader.java
git commit -m "Fix ParquetDocValuesReader to return singleton SortedSetDocValues for single-valued fields

ParquetDocValuesReader.getSortedSet() now detects when every document
has exactly one ordinal and returns DocValues.singleton(sortedDv).
This enables DocValues.unwrapSingleton() to succeed, required by
GlobalOrdinalsStringTermsAggregator for the optimized single-valued
collection path and correct profiler debug output."
```

---

### Task 2: Add BinaryDocValues to TextFieldMapper for parquet _source reconstruction

**Root cause:** When parquet is enabled, _source is reconstructed from doc values. Text fields have no doc values (DocValuesType.NONE), so reconstructed _source has no text field values. `significant_text` reads from _source → gets nothing → 0 buckets.

**Fix:** When parquet is enabled, write a `BinaryDocValuesField` with the raw text value during indexing. Create a `BinaryDocValuesFetcher` for _source reconstruction.

**Files:**
- Create: `server/src/main/java/org/opensearch/index/mapper/BinaryDocValuesFetcher.java`
- Modify: `server/src/main/java/org/opensearch/index/mapper/TextFieldMapper.java`

**Step 1: Create BinaryDocValuesFetcher**

Follow the pattern of `SortedSetDocValuesFetcher`. Extend `FieldValueFetcher`. Use `LeafReader.getBinaryDocValues(fieldName)`, call `advanceExact(docId)`, return `binaryValue().utf8ToString()`.

```java
package org.opensearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class BinaryDocValuesFetcher extends FieldValueFetcher {
    private final MappedFieldType fieldType;
    private final String simpleName;

    public BinaryDocValuesFetcher(MappedFieldType fieldType, String simpleName) {
        this.fieldType = fieldType;
        this.simpleName = simpleName;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        BinaryDocValues dv = reader.getBinaryDocValues(fieldType.name());
        if (dv == null || !dv.advanceExact(docId)) {
            return Collections.emptyList();
        }
        BytesRef value = dv.binaryValue();
        return Collections.singletonList(value.utf8ToString());
    }

    @Override
    public void write(XContentBuilder builder, String name, List<Object> values) throws IOException {
        if (values.isEmpty()) return;
        builder.field(name, values.get(0));
    }
}
```

**Step 2: Modify TextFieldMapper.parseCreateField() to write BinaryDocValuesField when parquet is enabled**

After creating the indexed Field, add:

```java
if (context.indexSettings().isParquetDocValuesEnabled()) {
    context.doc().add(new BinaryDocValuesField(fieldType().name(), new BytesRef(value)));
}
```

Import `org.apache.lucene.document.BinaryDocValuesField`.

**Step 3: Update TextFieldMapper.derivedFieldGenerator()**

Change from:
```java
return new DerivedFieldGenerator(mappedFieldType, null, new StoredFieldFetcher(mappedFieldType, simpleName())) {
    public FieldValueType getDerivedFieldPreference() { return FieldValueType.STORED; }
};
```

To:
```java
return new DerivedFieldGenerator(
    mappedFieldType,
    new BinaryDocValuesFetcher(mappedFieldType, simpleName()),
    new StoredFieldFetcher(mappedFieldType, simpleName())
);
```

Remove the `getDerivedFieldPreference()` override. The base `DerivedFieldGenerator` checks `hasDocValues()` to pick DOC_VALUES vs STORED.

**Step 4: Make TextFieldType report hasDocValues=true when parquet is enabled**

The `DerivedFieldGenerator.getDerivedFieldPreference()` checks `mappedFieldType.hasDocValues()`. Since we're adding BinaryDocValuesField only when parquet is enabled, we need the field type to report `hasDocValues=true` in that case.

Find where TextFieldType is constructed in TextFieldMapper.Builder and pass `true` for hasDocValues when parquet is enabled. Also remove or conditionalize the `assert mappedFieldType.hasDocValues() == false` at line 1010.

NOTE: If index settings aren't available during field type construction, override `getDerivedFieldPreference()` to return `DOC_VALUES` unconditionally when the BinaryDocValuesFetcher is provided, bypassing the `hasDocValues()` check.

**Step 5: Run the failing tests**

```bash
./gradlew :rest-api-spec:yamlRestTestParquet --tests "*parquet.search.aggregation/90_sig_text*" 2>&1 | tail -20
```

Expected: All 4 sig_text tests pass.

**Step 6: Commit**

```bash
git add server/src/main/java/org/opensearch/index/mapper/BinaryDocValuesFetcher.java \
       server/src/main/java/org/opensearch/index/mapper/TextFieldMapper.java
git commit -m "Add BinaryDocValues to text fields for parquet _source reconstruction

When parquet doc_values format is enabled, text fields now write a
BinaryDocValuesField with the raw text value during indexing. A new
BinaryDocValuesFetcher reads these values back during _source
reconstruction."
```

---

### Task 3: Reconstruct nested arrays from child doc values

**Root cause:** `ObjectMapper.canDeriveSource()` throws for nested fields. `ObjectMapper.deriveSource()` has no nested handling. Reconstructed _source is missing nested arrays → `FetchPhase.prepareNestedHitContext` crashes with `IndexOutOfBoundsException`.

**Fix:** Override `deriveSource()` for nested ObjectMappers to find child doc IDs in the segment and reconstruct the nested array from their doc values.

**Background — Lucene nested doc layout:**
- Nested child docs are separate Lucene documents stored BEFORE the parent doc in the segment
- For parent docId=5 with 2 children: child docIds are 3, 4
- Child docs have their own doc_values for nested fields
- Child docs are identified by `_nested_path` field (indexed, queryable via TermQuery)
- Parent docs are identified by having `_primary_term` field (FieldExistsQuery)

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/mapper/ObjectMapper.java`

**Step 1: Modify canDeriveSource() to allow nested fields**

Change from:
```java
public void canDeriveSource() {
    if (!this.enabled.value() || this.nested.isNested()) {
        throw new UnsupportedOperationException(...);
    }
    for (final Mapper mapper : this.mappers.values()) { mapper.canDeriveSource(); }
}
```

To:
```java
public void canDeriveSource() {
    if (!this.enabled.value()) {
        throw new UnsupportedOperationException(...);
    }
    // Nested fields are now supported — we reconstruct from child doc values
    for (final Mapper mapper : this.mappers.values()) { mapper.canDeriveSource(); }
}
```

**Step 2: Override deriveSource() for nested ObjectMappers**

When `this.nested.isNested()`, the method must:

1. Find child doc IDs belonging to the parent `docId`:
   - Get parent BitSet: query `FieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME)` on the leafReader
   - Get child BitSet: query `TermQuery(_nested_path, nestedTypePath)` on the leafReader
   - Previous parent = parentBits.prevSetBit(docId - 1) (or -1 if first parent)
   - Child doc IDs = iterate childBits.nextSetBit(previousParent + 1) while < docId

2. For each child doc ID, reconstruct the nested object:
   - `builder.startObject()`
   - For each child field mapper: `mapper.deriveSource(builder, leafReader, childDocId)`
   - `builder.endObject()`

3. Write as array: `builder.startArray(simpleName())` → objects → `builder.endArray()`

```java
public void deriveSource(XContentBuilder builder, LeafReader leafReader, int docId) throws IOException {
    if (this.nested.isNested()) {
        deriveNestedSource(builder, leafReader, docId);
        return;
    }
    builder.startObject(simpleName());
    for (final Mapper mapper : this.mappers.values()) {
        mapper.deriveSource(builder, leafReader, docId);
    }
    builder.endObject();
}

private void deriveNestedSource(XContentBuilder builder, LeafReader leafReader, int docId) throws IOException {
    // Find parent and child doc boundaries
    BitSet parentBits = BitSet.of(leafReader, Queries.newNonNestedFilter());
    BitSet childBits = BitSet.of(leafReader, NestedPathFieldMapper.filter(nestedTypePath));
    int prevParent = docId > 0 ? parentBits.prevSetBit(docId - 1) : -1;

    // Collect child doc IDs
    List<Integer> childDocIds = new ArrayList<>();
    for (int childId = childBits.nextSetBit(prevParent + 1);
         childId >= 0 && childId < docId;
         childId = childBits.nextSetBit(childId + 1)) {
        childDocIds.add(childId);
    }

    if (childDocIds.isEmpty()) return;

    // Write nested array
    builder.startArray(simpleName());
    for (int childDocId : childDocIds) {
        builder.startObject();
        for (final Mapper mapper : this.mappers.values()) {
            mapper.deriveSource(builder, leafReader, childDocId);
        }
        builder.endObject();
    }
    builder.endArray();
}
```

NOTE: Getting BitSets from a LeafReader requires running the query against the reader. Use `new IndexSearcher(leafReader).createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f).scorerSupplier(leafReader.getContext()).get(Long.MAX_VALUE).iterator()` or similar to get the doc ID set. Alternatively, use `leafReader.getPostings(new Term("_nested_path", nestedTypePath))` directly since _nested_path is an indexed field.

The exact BitSet API may need adjustment — check how `BitsetFilterCache` works and whether we can use `PostingsEnum` directly for the child filter.

**Step 3: Run the failing tests**

```bash
./gradlew :rest-api-spec:yamlRestTestParquet --tests "*parquet.search.aggregation/200_top_hits*" --tests "*parquet.search.aggregation/400_inner_hits*" 2>&1 | tail -20
```

Expected: Both nested doc tests pass.

**Step 4: Commit**

```bash
git add server/src/main/java/org/opensearch/index/mapper/ObjectMapper.java
git commit -m "Reconstruct nested arrays from child doc values for parquet _source

ObjectMapper.deriveSource() now handles nested fields by finding child
doc IDs in the segment (using _nested_path and parent BitSet) and
reconstructing the nested array from each child doc's doc_values.
This enables _source reconstruction for nested documents when parquet
doc_values format is enabled without storing _source."
```

---

### Task 4: Run full test suite and verify

**Step 1: Clean build cache and run all parquet tests**

```bash
rm -rf rest-api-spec/build/resources/yamlRestTest/rest-api-spec/test/parquet.search.aggregation
./gradlew :rest-api-spec:yamlRestTestParquet 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`, 357 tests, 0 failures.

**Step 2: Run server unit tests for modified files**

```bash
./gradlew :server:test --tests "*ParquetDocValuesReader*" --tests "*TextFieldMapper*" --tests "*ObjectMapper*" 2>&1 | tail -20
```

Expected: All pass.

**Step 3: Push**

```bash
git push origin features/parquet
```
