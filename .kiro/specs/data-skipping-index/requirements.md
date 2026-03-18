# Requirements Document: Data Skipping Index

## Introduction

This document specifies the requirements for implementing data skipping indexes in OpenSearch. Data skipping indexes are specialized index structures that enable the query engine to skip reading entire data blocks that are guaranteed not to contain matching values, significantly improving query performance for non-primary key queries. This feature is inspired by ClickHouse's data skipping index implementation and focuses initially on bloom filter-based indexes.

The primary use case is optimizing queries on high-cardinality fields (such as customer IDs, URLs, or product numbers) in large datasets where traditional inverted indexes may be inefficient due to storage overhead or where approximate matching with acceptable false positive rates is sufficient.

## Glossary

- **Data_Skipping_Index**: A specialized index structure that allows the query engine to skip reading data blocks that do not contain relevant data based on block-level metadata
- **Bloom_Filter**: A space-efficient probabilistic data structure that tests whether an element is a member of a set, with a configurable false positive rate but zero false negatives
- **Granule**: A contiguous block of data rows (typically 8192 rows) that serves as the minimum unit for skip index evaluation
- **False_Positive_Rate (FPR)**: The probability that a bloom filter incorrectly reports an element is present when it is not
- **Skip_Index_Expression**: The field or expression on which the skip index is built
- **Index_Metadata_File**: A file containing the skip index data structures (e.g., bloom filters) for each granule
- **Index_Marks_File**: A file containing pointers to the locations of skip index metadata for each granule
- **MightContain_Query**: A query operation that uses bloom filter indexes to identify documents that might contain the specified values
- **Expected_Insertions**: The anticipated number of distinct elements to be inserted into a bloom filter, used to optimize filter size
- **Lucene_Segment**: The fundamental unit of storage in Lucene, containing a subset of the index data
- **Inverted_Index**: The traditional full-text index structure in Lucene that maps terms to document IDs

## Requirements

### Requirement 1: Bloom Filter Index Creation

**User Story:** As a database administrator, I want to create bloom filter indexes on high-cardinality fields, so that I can optimize query performance for non-primary key searches.

#### Acceptance Criteria

1. WHEN a user specifies a bloom filter index in the index mapping, THE Index_Mapper SHALL accept the bloom filter field type with configuration parameters
2. THE Index_Mapper SHALL support the following bloom filter parameters: expected_insertions (integer) and fpp (false positive probability, float between 0 and 1)
3. WHEN expected_insertions is not specified, THE Index_Mapper SHALL use a default value of 1000
4. WHEN fpp is not specified, THE Index_Mapper SHALL use a default value of 0.01
5. THE Index_Mapper SHALL validate that fpp is between 0 and 1 (exclusive)
6. THE Index_Mapper SHALL validate that expected_insertions is a positive integer
7. WHEN an invalid parameter is provided, THE Index_Mapper SHALL return a descriptive error message

### Requirement 2: Bloom Filter Index Storage

**User Story:** As a system architect, I want bloom filter indexes to be stored efficiently in Lucene segments, so that storage overhead is minimized while maintaining query performance.

#### Acceptance Criteria

1. WHEN a document is indexed with a bloom filter field, THE Indexing_Engine SHALL create a bloom filter for each granule of data
2. THE Indexing_Engine SHALL store bloom filter metadata in a separate Index_Metadata_File within each Lucene_Segment
3. THE Indexing_Engine SHALL store granule pointers in an Index_Marks_File within each Lucene_Segment
4. THE Indexing_Engine SHALL support array values in bloom filter fields, where each array element is added to the bloom filter
5. WHEN a field value is null or empty, THE Indexing_Engine SHALL handle it gracefully without adding to the bloom filter
6. THE Indexing_Engine SHALL use the Guava BloomFilter implementation for creating and managing bloom filters
7. THE Indexing_Engine SHALL configure each bloom filter with the specified expected_insertions and fpp parameters

### Requirement 3: MightContain Query Support

**User Story:** As an application developer, I want to query bloom filter indexes using a mightContain query, so that I can quickly identify documents that might contain specific values.

#### Acceptance Criteria

1. WHEN a user submits a mightContain query, THE Query_Parser SHALL parse the query and identify the target bloom filter field
2. THE Query_Parser SHALL accept single values or arrays of values in the mightContain query
3. WHEN executing a mightContain query, THE Query_Engine SHALL evaluate the bloom filter for each granule
4. WHEN a granule's bloom filter indicates the value might be present, THE Query_Engine SHALL read and evaluate that granule's data
5. WHEN a granule's bloom filter indicates the value is definitely not present, THE Query_Engine SHALL skip reading that granule entirely
6. THE Query_Engine SHALL return results with a relation field indicating "mightContain" to signal potential false positives
7. THE Query_Engine SHALL include all true positives and may include false positives based on the configured fpp

### Requirement 4: Query Performance Optimization

**User Story:** As a performance engineer, I want data skipping indexes to significantly reduce query latency for non-primary key searches, so that analytical queries execute faster.

#### Acceptance Criteria

1. WHEN a mightContain query is executed, THE Query_Engine SHALL skip reading granules where the bloom filter indicates no match
2. THE Query_Engine SHALL log the number of granules skipped and granules read for performance analysis
3. WHEN trace logging is enabled, THE Query_Engine SHALL output detailed skip index usage information
4. THE Query_Engine SHALL apply skip indexes only when the query condition matches the skip index expression
5. WHEN multiple skip indexes exist on a segment, THE Query_Engine SHALL evaluate all applicable indexes before reading data

### Requirement 5: Index Metadata Management

**User Story:** As a system administrator, I want to manage skip index metadata efficiently, so that index operations do not significantly impact cluster performance.

#### Acceptance Criteria

1. WHEN a segment is merged, THE Segment_Merger SHALL rebuild bloom filters for the merged segment
2. THE Segment_Merger SHALL preserve the original expected_insertions and fpp parameters during merge operations
3. WHEN a segment is deleted, THE Cleanup_Process SHALL remove associated Index_Metadata_File and Index_Marks_File
4. THE Index_Manager SHALL expose APIs to retrieve skip index statistics (size, granule count, filter parameters)
5. THE Index_Manager SHALL support disabling skip indexes at query time for performance comparison

### Requirement 6: Backward Compatibility

**User Story:** As a cluster operator, I want data skipping indexes to be backward compatible with existing OpenSearch deployments, so that upgrades do not break existing functionality.

#### Acceptance Criteria

1. WHEN an index without skip indexes is queried, THE Query_Engine SHALL execute queries normally using inverted indexes
2. WHEN a cluster is upgraded with skip indexes enabled, THE Upgrade_Process SHALL not require reindexing existing data
3. THE Index_Mapper SHALL allow adding skip indexes to existing index mappings without data loss
4. WHEN a node without skip index support reads a segment with skip indexes, THE Node SHALL ignore the skip index files and use inverted indexes

### Requirement 7: Error Handling and Validation

**User Story:** As a developer, I want clear error messages when skip index operations fail, so that I can quickly diagnose and resolve issues.

#### Acceptance Criteria

1. WHEN a bloom filter field is queried with an incompatible query type, THE Query_Engine SHALL return a descriptive error message
2. WHEN bloom filter creation fails due to memory constraints, THE Indexing_Engine SHALL log the error and fall back to standard indexing
3. WHEN skip index files are corrupted, THE Query_Engine SHALL detect the corruption and fall back to full granule scans
4. THE Error_Handler SHALL include the field name and index type in all skip index error messages
5. WHEN a mightContain query targets a non-bloom-filter field, THE Query_Parser SHALL return an error indicating the field type mismatch

### Requirement 8: Benchmarking and Performance Measurement

**User Story:** As a performance engineer, I want to benchmark skip indexes against inverted indexes, so that I can quantify performance improvements and storage tradeoffs.

#### Acceptance Criteria

1. THE Benchmark_Suite SHALL support loading the Big5 benchmark dataset for performance testing
2. THE Benchmark_Suite SHALL measure query latency for mightContain queries with skip indexes enabled
3. THE Benchmark_Suite SHALL measure query latency for equivalent term queries with inverted indexes
4. THE Benchmark_Suite SHALL measure storage overhead (index size) for skip indexes versus inverted indexes
5. THE Benchmark_Suite SHALL test both text and keyword field types with varying cardinality
6. THE Benchmark_Suite SHALL report the number of granules skipped and granules read for each query
7. THE Benchmark_Suite SHALL measure indexing throughput with skip indexes enabled versus disabled

### Requirement 9: Configuration and Tuning

**User Story:** As a database administrator, I want to configure skip index parameters at the cluster and index level, so that I can optimize performance for different workloads.

#### Acceptance Criteria

1. THE Configuration_Manager SHALL support a cluster-level setting to enable or disable skip indexes globally
2. THE Configuration_Manager SHALL support an index-level setting to control granule size (number of rows per granule)
3. WHEN granule size is not specified, THE Configuration_Manager SHALL use a default value of 8192 rows
4. THE Configuration_Manager SHALL validate that granule size is a power of 2 between 1024 and 65536
5. THE Configuration_Manager SHALL allow updating skip index settings on existing indexes with a reindex operation

### Requirement 10: Monitoring and Observability

**User Story:** As a site reliability engineer, I want to monitor skip index usage and effectiveness, so that I can identify optimization opportunities.

#### Acceptance Criteria

1. THE Metrics_Collector SHALL track the total number of skip indexes created per index
2. THE Metrics_Collector SHALL track the number of queries that used skip indexes
3. THE Metrics_Collector SHALL track the average number of granules skipped per query
4. THE Metrics_Collector SHALL track the false positive rate observed in production queries
5. THE Metrics_Collector SHALL expose these metrics via the OpenSearch stats API
6. WHEN a query uses a skip index, THE Query_Logger SHALL include skip index statistics in the slow query log
