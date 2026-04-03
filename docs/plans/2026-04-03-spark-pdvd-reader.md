# Spark PDVD Reader — Design Doc

## Problem

OpenSearch's `.pdvd` files contain valid Parquet content wrapped inside Lucene's CodecUtil framing:

```
[CodecUtil header 55 bytes] [PAR1 ...parquet content... PAR1] [CodecUtil footer 16 bytes]
```

Standard Parquet readers (Spark, Trino, DuckDB) expect PAR1 at byte 0. They cannot read `.pdvd` files directly because the first bytes are Lucene's codec header, not PAR1.

## Solution

Create a thin Spark-side JAR that provides an `InputFile` wrapper. This wrapper presents a `.pdvd` file as a standard Parquet file by applying a fixed offset:

- **Read offset**: skip first N bytes (CodecUtil header) to reach PAR1
- **Length adjustment**: subtract CodecUtil footer (16 bytes) from the end
- **Zero copy**: no temp files, no byte extraction — reads directly from the original `.pdvd`

## Architecture

```
Spark SQL                        .pdvd file on disk
    |                                 |
    v                                 |
ParquetFileReader                     |
    |                                 |
    v                                 |
PdvdInputFile (our code)             |
    |  offset = 55                    |
    |  length = fileSize - 55 - 16    |
    v                                 v
SeekableInputStream -----------> RandomAccessFile
    seek(pos) -> file.seek(pos + 55)
    read()    -> file.read()
```

## Key Insight

Parquet's `ParquetFileReader` accepts any `InputFile` implementation. It calls:
- `inputFile.getLength()` — we return `fileSize - headerSize - footerSize`
- `inputFile.newStream()` — we return a `SeekableInputStream` that adds `headerSize` to all seeks

This is ~60 lines of Java. No Parquet internals modified. No Spark internals modified.

## Implementation

### `PdvdInputFile.java`

Implements `org.apache.parquet.io.InputFile`. Wraps a local file path.

```java
public class PdvdInputFile implements InputFile {
    private final Path path;
    private final long parquetStart;  // offset to PAR1 magic
    private final long parquetLength; // PAR1 to trailing PAR1 (inclusive)

    public PdvdInputFile(Path path) {
        // Read first 100 bytes, find PAR1 offset
        // parquetStart = index of PAR1
        // parquetLength = fileSize - parquetStart - CODEC_FOOTER_SIZE
    }

    @Override
    public long getLength() { return parquetLength; }

    @Override
    public SeekableInputStream newStream() {
        return new PdvdSeekableInputStream(path, parquetStart, parquetLength);
    }
}
```

### `PdvdSeekableInputStream.java`

Extends `DelegatingSeekableInputStream`. All positions are shifted by `parquetStart`.

```java
public class PdvdSeekableInputStream extends DelegatingSeekableInputStream {
    private final RandomAccessFile raf;
    private final long offset;
    private final long length;

    @Override
    public long getPos() { return raf.getFilePointer() - offset; }

    @Override
    public void seek(long newPos) { raf.seek(newPos + offset); }

    @Override
    public int read() { return raf.read(); }

    @Override
    public int read(byte[] b, int off, int len) {
        // Clamp to not read past parquet content
        long remaining = length - getPos();
        int toRead = (int) Math.min(len, remaining);
        return raf.read(b, off, toRead);
    }
}
```

### Usage from Spark

**PySpark:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars", "/path/to/opensearch-spark-pdvd.jar") \
    .getOrCreate()

# Read .pdvd files directly — no extraction
df = spark.read.format("org.opensearch.spark.PdvdParquetSource") \
    .load("/path/to/opensearch/data/**/*.pdvd")

df.show()
```

**spark-sql:**
```sql
-- Add JAR at startup
ADD JAR /path/to/opensearch-spark-pdvd.jar;

-- Read directly
SELECT * FROM pdvd.`/path/to/*.pdvd` LIMIT 10;
```

## File Structure

```
opensearch-spark-pdvd/
  src/main/java/org/opensearch/spark/
    PdvdInputFile.java              -- InputFile wrapper (offset reads)
    PdvdSeekableInputStream.java    -- SeekableInputStream with offset
    PdvdParquetSource.java          -- Spark DataSource V2 registration
  build.gradle                      -- builds opensearch-spark-pdvd.jar
```

## Dependencies

- `parquet-common` (for `InputFile`, `SeekableInputStream`, `DelegatingSeekableInputStream`)
- `spark-sql` (for DataSource V2 API)

No OpenSearch dependencies. No Lucene dependencies. The JAR only needs to know the byte offsets.

## How PAR1 Offset is Determined

The `PdvdInputFile` constructor scans the first 100 bytes for the PAR1 magic (`0x50415231`). The CodecUtil header size varies slightly depending on codec name length and segment suffix, but PAR1 always appears within the first 100 bytes. The trailing CodecUtil footer is always exactly 16 bytes.

## Limitations

- Local filesystem only (no HDFS/S3 in v1 — can be added with Hadoop `FileSystem`)
- Read-only (Spark cannot write `.pdvd`)
- Each `.pdvd` file is one Parquet row group from one Lucene segment — Spark unions them
