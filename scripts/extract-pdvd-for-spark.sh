#!/usr/bin/env bash
set -euo pipefail

# Extract Parquet content from .pdvd files for Spark SQL testing.
# Strips the CodecUtil header (before PAR1) and footer (after trailing PAR1).
#
# Usage: ./scripts/extract-pdvd-for-spark.sh [output_dir]
#
# After extraction, start spark-sql:
#   SPARK_HOME=$(python3 -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))")
#   $SPARK_HOME/bin/spark-sql --master local[*] --driver-memory 4g
#
# Then run SQL:
#   SELECT * FROM parquet.`/tmp/opensearch-parquet/` LIMIT 10;
#   SELECT status, COUNT(*) FROM parquet.`/tmp/opensearch-parquet/` GROUP BY status;

DATA_DIR="/local/home/penghuo/oss/OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT/data"
OUTPUT_DIR="${1:-/tmp/opensearch-parquet}"

mkdir -p "$OUTPUT_DIR"

echo "Scanning for .pdvd files in $DATA_DIR ..."

# Find all .pdvd files
PDVD_FILES=$(find "$DATA_DIR" -name "*.pdvd" -type f 2>/dev/null)
COUNT=$(echo "$PDVD_FILES" | grep -c "pdvd" || true)

if [ "$COUNT" -eq 0 ]; then
    echo "ERROR: No .pdvd files found. Is OpenSearch running with parquet codec?"
    exit 1
fi

echo "Found $COUNT .pdvd files"

# Extract each .pdvd file
EXTRACTED=0
for pdvd in $PDVD_FILES; do
    size=$(stat -c%s "$pdvd" 2>/dev/null || echo 0)
    # Skip tiny files (< 100 bytes — likely empty segments)
    if [ "$size" -lt 100 ]; then
        continue
    fi

    basename=$(basename "$pdvd" .pdvd)
    dirname=$(basename "$(dirname "$(dirname "$pdvd")")")
    outfile="$OUTPUT_DIR/${dirname}_${basename}.parquet"

    # Find PAR1 magic offset (skip CodecUtil header)
    par1_offset=$(python3 -c "
data = open('$pdvd', 'rb').read()
idx = data.find(b'PAR1')
print(idx if idx >= 0 else -1)
")

    if [ "$par1_offset" -lt 0 ]; then
        echo "  SKIP $basename (no PAR1 magic)"
        continue
    fi

    # Extract: from first PAR1 to (EOF - 16 bytes CodecUtil footer)
    python3 -c "
data = open('$pdvd', 'rb').read()
par1_start = $par1_offset
# CodecUtil footer is 16 bytes at the very end
parquet_end = len(data) - 16
# Verify trailing PAR1
if data[parquet_end-4:parquet_end] == b'PAR1':
    content = data[par1_start:parquet_end]
    open('$outfile', 'wb').write(content)
    print(f'  OK {len(content)} bytes -> $(basename $outfile)')
else:
    print(f'  WARN no trailing PAR1, trying without footer strip')
    if data[-4:] == b'PAR1':
        content = data[par1_start:]
        open('$outfile', 'wb').write(content)
        print(f'  OK {len(content)} bytes -> $(basename $outfile)')
    else:
        print(f'  SKIP no valid Parquet framing')
"
    EXTRACTED=$((EXTRACTED + 1))
done

echo ""
echo "Extracted $EXTRACTED files to $OUTPUT_DIR"
echo ""
echo "=== Start spark-sql ==="
echo ""
echo "  SPARK_HOME=\$(python3 -c \"import pyspark; import os; print(os.path.dirname(pyspark.__file__))\")"
echo "  \$SPARK_HOME/bin/spark-sql --master local[*] --driver-memory 4g"
echo ""
echo "=== Then run queries ==="
echo ""
echo "  -- List files"
echo "  SELECT * FROM parquet.\`$OUTPUT_DIR/\` LIMIT 5;"
echo ""
echo "  -- Count rows"
echo "  SELECT COUNT(*) FROM parquet.\`$OUTPUT_DIR/\`;"
echo ""
echo "  -- Aggregation"
echo "  SELECT status, COUNT(*) as cnt FROM parquet.\`$OUTPUT_DIR/\` GROUP BY status ORDER BY cnt DESC;"
