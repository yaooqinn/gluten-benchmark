#!/bin/bash
# Run daily benchmarks and archive results with today's date
# Usage: ./scripts/run-daily-benchmark.sh [benchmark-filter]
#
# Results are saved to: benchmarks/YYYY-MM-DD/
#
# Examples:
#   ./scripts/run-daily-benchmark.sh              # Run all benchmarks
#   ./scripts/run-daily-benchmark.sh Aggregate    # Run only AggregateBenchmark
#
# Schedule with cron:
#   0 2 * * * cd /path/to/gluten-benchmark && ./scripts/run-daily-benchmark.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Get today's date
TODAY=$(date +%Y-%m-%d)
FILTER="${1:-}"

echo "========================================"
echo "Gluten Daily Benchmark - $TODAY"
echo "========================================"
echo ""

# Create dated results directory
RESULTS_DIR="benchmarks/$TODAY"
mkdir -p "$RESULTS_DIR"

echo "Results will be saved to: $RESULTS_DIR"
echo ""

# Check for Gluten JAR
GLUTEN_JAR=$(find lib -name "gluten-velox-bundle*.jar" 2>/dev/null | head -1)
if [[ -z "$GLUTEN_JAR" ]]; then
    echo "Downloading Gluten nightly..."
    ./scripts/download-gluten-nightly.sh
    GLUTEN_JAR=$(find lib -name "gluten-velox-bundle*.jar" 2>/dev/null | head -1)
fi

if [[ -z "$GLUTEN_JAR" ]]; then
    echo "ERROR: Gluten JAR not found. Please run: ./scripts/download-gluten-nightly.sh"
    exit 1
fi

echo "Using Gluten JAR: $GLUTEN_JAR"
echo ""

# Record metadata
cat > "$RESULTS_DIR/metadata.json" << EOF
{
  "date": "$TODAY",
  "timestamp": "$(date -Iseconds)",
  "gluten_jar": "$GLUTEN_JAR",
  "spark_version": "3.5.5",
  "java_version": "$(java -version 2>&1 | head -1)",
  "hostname": "$(hostname)",
  "cpu_model": "$(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)",
  "cpu_cores": $(nproc),
  "memory_gb": $(free -g | awk '/Mem:/{print $2}')
}
EOF

echo "Metadata saved to: $RESULTS_DIR/metadata.json"
echo ""

# Run benchmarks
echo "Running benchmarks..."
SPARK_GENERATE_BENCHMARK_FILES=1 \
SPARK_BENCHMARK_DATE="$TODAY" \
./build/sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks $FILTER" 2>&1 | \
    tee "$RESULTS_DIR/run.log" | \
    grep -E "(Running benchmark|completed)" || true

echo ""
echo "========================================"
echo "Daily benchmark completed!"
echo "========================================"
echo ""
echo "Results saved to: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"
echo ""
echo "To compare with previous runs:"
echo "  ./scripts/compare-daily.sh"
