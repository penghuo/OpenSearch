# Design Document: Field Mapping Limit Performance Analysis

## Overview

This design outlines a comprehensive benchmark suite to reproduce and measure the performance bottlenecks that limit OpenSearch field mappings. The benchmarks will test indices with 1,000 to 100,000 non-indexed fields to identify actual constraints when `index=false` and `doc_values=false` are used.

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                  Benchmark Test Suite                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │  Memory Profiler │  │ Cluster State    │                │
│  │  Benchmark       │  │ Benchmark        │                │
│  └──────────────────┘  └──────────────────┘                │
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │  Field Lookup    │  │ Mapping Merge    │                │
│  │  Benchmark       │  │ Benchmark        │                │
│  └──────────────────┘  └──────────────────┘                │
│                                                               │
│  ┌──────────────────────────────────────────┐               │
│  │     Test Data Generator                   │               │
│  └──────────────────────────────────────────┘               │
│                                                               │
│  ┌──────────────────────────────────────────┐               │
│  │     Performance Reporter                  │               │
│  └──────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### Test Configuration Matrix

| Field Count | Test Scenarios | Purpose |
|-------------|----------------|---------|
| 1,000 | Baseline | Current default limit |
| 10,000 | Soft Maximum | Current recommended max |
| 50,000 | Mid-range | Intermediate scaling test |
| 100,000 | Target | Proposed new limit |

## Components and Interfaces

### 1. Test Data Generator

**Purpose**: Generate realistic field mappings for benchmarking

**Interface**:
```java
public class FieldMappingGenerator {
    /**
     * Generate mapping with specified number of non-indexed fields
     * @param fieldCount Number of fields to generate
     * @param fieldNamePrefix Prefix for field names
     * @return Mapping as CompressedXContent
     */
    public CompressedXContent generateMapping(int fieldCount, String fieldNamePrefix);
    
    /**
     * Generate nested object structure
     * @param fieldCount Total fields across all nesting levels
     * @param maxDepth Maximum nesting depth
     * @return Mapping with nested objects
     */
    public CompressedXContent generateNestedMapping(int fieldCount, int maxDepth);
    
    /**
     * Generate field names with realistic characteristics
     * @param count Number of names to generate
     * @return List of field names (10-50 chars, alphanumeric + underscores)
     */
    public List<String> generateFieldNames(int count);
}
```

**Implementation Details**:
- Field names: 10-50 characters, pattern: `field_{category}_{index}_{random}`
- Field types: Mix of keyword (50%), long (30%), text (10%), boolean (10%)
- All fields: `"index": false, "doc_values": false, "store": false`
- Nested objects: Balanced tree structure to avoid depth limit

### 2. Memory Profiler Benchmark

**Purpose**: Measure heap memory overhead of field mappings

**Interface**:
```java
public class MemoryProfilerBenchmark extends OpenSearchIntegTestCase {
    /**
     * Measure memory before and after creating index with N fields
     * @param fieldCount Number of fields
     * @return MemoryMetrics with heap usage details
     */
    public MemoryMetrics measureMappingMemory(int fieldCount);
    
    /**
     * Calculate per-field memory overhead
     * @param metrics Memory measurements at different field counts
     * @return Average bytes per field
     */
    public long calculatePerFieldOverhead(List<MemoryMetrics> metrics);
}

public class MemoryMetrics {
    private long heapUsedBefore;
    private long heapUsedAfter;
    private long mappingStructureSize;
    private int fieldCount;
    
    public long getMemoryIncrease() {
        return heapUsedAfter - heapUsedBefore;
    }
    
    public long getPerFieldCost() {
        return mappingStructureSize / fieldCount;
    }
}
```

**Measurement Strategy**:
1. Force full GC before measurement
2. Create index with N fields
3. Force full GC after creation
4. Measure heap usage via `Runtime.getRuntime().totalMemory() - freeMemory()`
5. Use Java instrumentation to measure specific object sizes
6. Repeat 5 times and take median to reduce noise

**Memory Breakdown**:
- `FieldTypeLookup.fullNameToFieldType` HashMap
- `MappingLookup.fieldMappers` HashMap
- `MappingLookup.objectMappers` HashMap
- Individual `MappedFieldType` objects
- String interning overhead for field names

### 3. Cluster State Benchmark

**Purpose**: Measure cluster state size and serialization performance

**Interface**:
```java
public class ClusterStateBenchmark extends OpenSearchIntegTestCase {
    /**
     * Measure cluster state size with N fields
     * @param fieldCount Number of fields
     * @return ClusterStateMetrics with size and timing
     */
    public ClusterStateMetrics measureClusterState(int fieldCount);
    
    /**
     * Measure serialization/deserialization time
     * @param indexMetadata Index metadata to serialize
     * @return Timing metrics in milliseconds
     */
    public SerializationMetrics measureSerialization(IndexMetadata indexMetadata);
}

public class ClusterStateMetrics {
    private long uncompressedSizeBytes;
    private long compressedSizeBytes;
    private long serializationTimeMs;
    private long deserializationTimeMs;
    private int fieldCount;
    
    public double getCompressionRatio() {
        return (double) compressedSizeBytes / uncompressedSizeBytes;
    }
}
```

**Measurement Strategy**:
1. Create index with N fields
2. Extract `IndexMetadata` from cluster state
3. Serialize to JSON using `XContentFactory.jsonBuilder()`
4. Measure uncompressed JSON size
5. Compress using `CompressedXContent`
6. Measure compressed size
7. Measure serialization time (10 iterations, average)
8. Measure deserialization time (10 iterations, average)

**Metrics to Capture**:
- Uncompressed mapping JSON size
- Compressed mapping size (LZ4 compression)
- Compression ratio
- Serialization time (JSON generation)
- Deserialization time (JSON parsing)
- Network transfer time estimate (compressed size / typical bandwidth)

### 4. Field Lookup Benchmark

**Purpose**: Measure field lookup and pattern matching performance

**Interface**:
```java
public class FieldLookupBenchmark extends OpenSearchIntegTestCase {
    /**
     * Measure direct field lookup performance
     * @param mapperService MapperService with N fields
     * @param fieldNames List of field names to lookup
     * @return LookupMetrics with timing statistics
     */
    public LookupMetrics measureDirectLookup(MapperService mapperService, List<String> fieldNames);
    
    /**
     * Measure wildcard pattern matching performance
     * @param mapperService MapperService with N fields
     * @param patterns List of wildcard patterns
     * @return PatternMatchMetrics with timing statistics
     */
    public PatternMatchMetrics measurePatternMatching(MapperService mapperService, List<String> patterns);
}

public class LookupMetrics {
    private List<Long> lookupTimesNanos;
    private int fieldCount;
    
    public long getP50Nanos() { /* percentile calculation */ }
    public long getP95Nanos() { /* percentile calculation */ }
    public long getP99Nanos() { /* percentile calculation */ }
    public double getAverageNanos() { /* average calculation */ }
}
```

**Measurement Strategy**:

**Direct Lookup Test**:
1. Create index with N fields
2. Get `MapperService` instance
3. Warm up: 1000 lookups
4. Measure: 10,000 lookups of random field names
5. Record time for each lookup in nanoseconds
6. Calculate percentiles (p50, p95, p99)

**Pattern Matching Test**:
1. Create index with N fields
2. Test patterns: `field_*`, `field_category_*`, `*_1000`, `field_*_*`
3. Measure `simpleMatchToFullName()` execution time
4. Record number of fields matched
5. Calculate time per matched field

**Test Scenarios**:
- Best case: Direct lookup by exact name
- Average case: Pattern with moderate matches (10% of fields)
- Worst case: Pattern matching all fields (`*`)

### 5. Mapping Merge Benchmark

**Purpose**: Measure mapping update and merge performance

**Interface**:
```java
public class MappingMergeBenchmark extends OpenSearchIntegTestCase {
    /**
     * Measure time to create initial mapping
     * @param fieldCount Number of fields
     * @return MergeMetrics with timing
     */
    public MergeMetrics measureInitialMapping(int fieldCount);
    
    /**
     * Measure time to add fields to existing mapping
     * @param existingFieldCount Current field count
     * @param additionalFields Fields to add
     * @return MergeMetrics with timing
     */
    public MergeMetrics measureIncrementalMerge(int existingFieldCount, int additionalFields);
}

public class MergeMetrics {
    private long mergeTimeMs;
    private long validationTimeMs;
    private int totalFields;
    
    public long getTotalTimeMs() {
        return mergeTimeMs + validationTimeMs;
    }
}
```

**Measurement Strategy**:
1. Measure initial mapping creation (cold start)
2. Measure incremental updates (add 100 fields at a time)
3. Measure full mapping replacement
4. Record time for `MapperService.merge()` operation
5. Record time for `MappingLookup.checkLimits()` validation

### 6. Performance Reporter

**Purpose**: Aggregate and report benchmark results

**Interface**:
```java
public class PerformanceReporter {
    /**
     * Generate comprehensive performance report
     * @param results All benchmark results
     * @return Formatted report with charts and analysis
     */
    public String generateReport(BenchmarkResults results);
    
    /**
     * Export results to CSV for analysis
     * @param results Benchmark results
     * @param outputPath File path for CSV
     */
    public void exportToCSV(BenchmarkResults results, String outputPath);
    
    /**
     * Determine if performance meets thresholds
     * @param results Benchmark results
     * @return Pass/fail for each threshold
     */
    public ThresholdAnalysis analyzeThresholds(BenchmarkResults results);
}
```

**Report Format**:
```
=== OpenSearch Field Mapping Performance Benchmark ===

Test Configuration:
- OpenSearch Version: 2.x
- JVM: OpenJDK 17
- Heap Size: 4GB
- Field Configuration: index=false, doc_values=false

Results Summary:
┌─────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ Field Count │ Memory (MB)  │ State (MB)   │ Lookup (μs)  │ Merge (ms)   │
├─────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│ 1,000       │ 12.5         │ 0.8          │ 0.05         │ 45           │
│ 10,000      │ 125.0        │ 8.0          │ 0.08         │ 450          │
│ 50,000      │ 625.0        │ 40.0         │ 0.15         │ 2,250        │
│ 100,000     │ 1,250.0      │ 80.0         │ 0.25         │ 4,500        │
└─────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

Detailed Analysis:
[Charts and graphs showing scaling behavior]

Threshold Analysis:
✓ Field lookup p99 < 10ms: PASS
✓ Cluster state < 100MB: PASS
✗ Memory per field < 1KB: FAIL (1.25KB per field)
✓ Merge time < 5s: PASS
```

## Data Models

### Benchmark Configuration

```java
public class BenchmarkConfig {
    private List<Integer> fieldCounts = Arrays.asList(1000, 10000, 50000, 100000);
    private int warmupIterations = 5;
    private int measurementIterations = 10;
    private boolean forceGC = true;
    private String fieldNamePrefix = "field";
    private int maxNestingDepth = 5;
}
```

### Benchmark Results

```java
public class BenchmarkResults {
    private Map<Integer, MemoryMetrics> memoryResults;
    private Map<Integer, ClusterStateMetrics> clusterStateResults;
    private Map<Integer, LookupMetrics> lookupResults;
    private Map<Integer, MergeMetrics> mergeResults;
    private BenchmarkConfig config;
    private long testDurationMs;
}
```

## Error Handling

### Expected Errors

1. **OutOfMemoryError**: If heap is insufficient for 100K fields
   - Mitigation: Increase heap size or reduce field count
   - Report: Document minimum heap requirements

2. **IllegalArgumentException**: If field limit is exceeded
   - Expected: This is what we're testing
   - Handle: Temporarily increase limit for benchmarking

3. **Timeout**: If operations take too long
   - Mitigation: Increase timeout thresholds
   - Report: Document as performance failure

### Error Recovery

- Each benchmark runs independently
- Failures in one test don't affect others
- Clean up indices after each test
- Log all errors with context

## Testing Strategy

### Unit Tests

Test individual components:
- `FieldMappingGenerator` produces valid mappings
- `MemoryMetrics` calculations are correct
- `PerformanceReporter` formats output correctly

### Integration Tests

Test end-to-end scenarios:
- Create index with 1K fields and verify all metrics collected
- Run full benchmark suite and verify report generation
- Test with different field configurations

### Performance Validation

Verify benchmarks are accurate:
- Compare memory measurements with heap dumps
- Validate cluster state sizes with actual serialized data
- Cross-check lookup times with profiler data

## Implementation Plan

### Phase 1: Test Infrastructure (Week 1)
- Implement `FieldMappingGenerator`
- Implement `PerformanceReporter`
- Create base test class with common utilities

### Phase 2: Memory Benchmarks (Week 1)
- Implement `MemoryProfilerBenchmark`
- Test with 1K, 10K fields
- Validate measurements

### Phase 3: Cluster State Benchmarks (Week 2)
- Implement `ClusterStateBenchmark`
- Measure serialization performance
- Test compression ratios

### Phase 4: Lookup Benchmarks (Week 2)
- Implement `FieldLookupBenchmark`
- Test direct lookups
- Test pattern matching

### Phase 5: Merge Benchmarks (Week 3)
- Implement `MappingMergeBenchmark`
- Test initial creation
- Test incremental updates

### Phase 6: Full Suite Testing (Week 3)
- Run complete benchmark suite
- Generate comprehensive reports
- Analyze results and identify bottlenecks

## Performance Thresholds

Based on requirements, these are the acceptance criteria:

| Metric | Threshold | Rationale |
|--------|-----------|-----------|
| Field lookup p99 | < 10ms | Query performance impact |
| Cluster state size | < 100MB | Network and storage overhead |
| Memory per field | < 1KB | Heap usage at 100K fields |
| Merge time | < 5s | Mapping update responsiveness |
| Serialization time | < 1s | Cluster state update speed |

## Notes

- All tests run on single-node cluster to isolate mapping overhead
- Tests use non-indexed fields to eliminate Lucene overhead
- Multiple iterations ensure statistical significance
- Results will guide optimization decisions for 100K field support

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Memory measurement accuracy
*For any* index with N fields, the per-field memory cost should equal total memory increase divided by field count, with less than 5% variance across measurements.
**Validates: Requirements 1.5**

### Property 2: Cluster state serializability
*For any* mapping with N non-indexed fields, serializing to JSON and deserializing should produce an equivalent mapping (round-trip property).
**Validates: Requirements 2.4**

### Property 3: Compression effectiveness
*For any* cluster state JSON, the compressed size should be strictly less than the uncompressed size.
**Validates: Requirements 2.2**

### Property 4: Field lookup correctness
*For any* field name that exists in the mapping, calling `fieldType(name)` should return a non-null MappedFieldType with the correct field name.
**Validates: Requirements 3.1**

### Property 5: Pattern matching correctness
*For any* wildcard pattern and field set, all fields returned by `simpleMatchToFullName(pattern)` should match the pattern when tested with `Regex.simpleMatch()`.
**Validates: Requirements 3.2**

### Property 6: HashMap lookup performance advantage
*For any* field count N > 1000, HashMap-based field lookup should be faster than linear iteration through all fields.
**Validates: Requirements 3.5**

### Property 7: Index field count accuracy
*For any* requested field count N, the created index should have exactly N fields (excluding metadata fields).
**Validates: Requirements 4.1**

### Property 8: Non-indexed field configuration
*For any* field generated by the benchmark suite, the field mapping should have `index=false` and `doc_values=false`.
**Validates: Requirements 4.2**

### Property 9: Memory monotonicity
*For any* mapping creation operation, heap memory after creation should be greater than or equal to heap memory before creation.
**Validates: Requirements 4.3**

### Property 10: Percentile ordering
*For any* set of latency measurements, the calculated percentiles should satisfy: p50 ≤ p95 ≤ p99 ≤ max.
**Validates: Requirements 5.6**

### Property 11: Field name length bounds
*For any* field name generated by the Data_Generator, the length should be between 10 and 50 characters inclusive.
**Validates: Requirements 7.1**

### Property 12: Nested structure depth accuracy
*For any* requested nesting depth D, the generated nested structure should have maximum depth exactly equal to D.
**Validates: Requirements 7.2**

### Property 13: Field type distribution accuracy
*For any* requested field type distribution, the generated fields should match the distribution within 5% tolerance.
**Validates: Requirements 7.3**

### Property 14: Structure type correctness
*For any* generated mapping, flat structures should have all fields at depth 1, and nested structures should have at least one field at depth > 1.
**Validates: Requirements 7.5**

### Example Tests (Specific Thresholds)

These are specific threshold validations that should be tested with concrete examples:

**Example 1: Field lookup latency threshold**
- Test: Create index with 100,000 fields, perform 10,000 lookups, verify p99 < 10ms
- **Validates: Requirements 8.1**

**Example 2: Cluster state serialization threshold**
- Test: Create index with 100,000 fields, serialize cluster state, verify time < 1 second
- **Validates: Requirements 8.2**

**Example 3: Mapping merge threshold**
- Test: Create index with 100,000 fields, verify merge operation < 5 seconds
- **Validates: Requirements 8.3**

**Example 4: Memory per field threshold**
- Test: Create index with 100,000 fields, verify memory per field < 1KB
- **Validates: Requirements 8.4**

**Example 5: Cluster state size threshold**
- Test: Create index with 100,000 fields, verify compressed cluster state < 100MB
- **Validates: Requirements 8.5**
