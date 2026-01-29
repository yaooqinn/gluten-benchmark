#!/bin/bash
# Run benchmarks across multiple Spark versions
# Usage: ./scripts/run-multi-version.sh [benchmark-class-filter]
#
# Examples:
#   ./scripts/run-multi-version.sh             # Run all benchmarks on all versions
#   ./scripts/run-multi-version.sh Aggregate   # Run only AggregateBenchmark

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Spark versions to benchmark
SPARK_VERSIONS=("3.4.4" "3.5.5" "4.0.0")

# Gluten nightly builds per Spark version
declare -A GLUTEN_JARNAMES
GLUTEN_JARNAMES["3.4.4"]="gluten-velox-bundle-spark3.4_2.12-linux_amd64-1.6.0-SNAPSHOT.jar"
GLUTEN_JARNAMES["3.5.5"]="gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.6.0-SNAPSHOT.jar"
GLUTEN_JARNAMES["4.0.0"]="gluten-velox-bundle-spark4.0_2.13-linux_amd64-1.6.0-SNAPSHOT.jar"

# Scala versions per Spark version
declare -A SCALA_VERSIONS
SCALA_VERSIONS["3.4.4"]="2.12.18"
SCALA_VERSIONS["3.5.5"]="2.12.18"
SCALA_VERSIONS["4.0.0"]="2.13.14"

FILTER="${1:-}"
GLUTEN_NIGHTLY_URL="https://nightlies.apache.org/gluten/nightly-release-jdk17"

echo "========================================"
echo "Gluten Multi-Version Benchmark Runner"
echo "========================================"
echo ""

# Download Gluten JARs if not present
download_gluten() {
    local spark_ver="$1"
    local jar_name="${GLUTEN_JARNAMES[$spark_ver]}"
    local jar_path="lib/$jar_name"
    
    if [[ ! -f "$jar_path" ]]; then
        echo "Downloading Gluten nightly for Spark $spark_ver..."
        mkdir -p lib
        wget -q --show-progress -O "$jar_path" "${GLUTEN_NIGHTLY_URL}/${jar_name}" || {
            echo "WARNING: Failed to download Gluten for Spark $spark_ver"
            return 1
        }
    fi
    echo "$jar_path"
}

# Run benchmarks for a specific Spark version
run_version() {
    local spark_ver="$1"
    local scala_ver="${SCALA_VERSIONS[$spark_ver]}"
    local jar_name="${GLUTEN_JARNAMES[$spark_ver]}"
    local jar_path="lib/$jar_name"
    
    echo ""
    echo "----------------------------------------"
    echo "Running benchmarks on Spark $spark_ver"
    echo "----------------------------------------"
    
    # Download Gluten if needed
    if [[ ! -f "$jar_path" ]]; then
        download_gluten "$spark_ver" || {
            echo "Skipping Spark $spark_ver (Gluten not available)"
            return 0
        }
    fi
    
    # Build and run with specific Spark version
    SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt \
        -Dspark.version="$spark_ver" \
        -Dscala.version="$scala_ver" \
        -Dgluten.jar="$jar_path" \
        -Dspark.benchmark.version="$spark_ver" \
        "runMain org.apache.gluten.benchmark.RunAllBenchmarks $FILTER" 2>&1 | \
        grep -v "^\[info\].*Running task" || true
    
    echo "Results saved to: benchmarks/*-spark${spark_ver}-results.txt"
}

# Main execution
echo "Benchmark filter: ${FILTER:-'(all)'}"
echo "Versions to test: ${SPARK_VERSIONS[*]}"
echo ""

for ver in "${SPARK_VERSIONS[@]}"; do
    run_version "$ver"
done

echo ""
echo "========================================"
echo "All versions completed!"
echo "========================================"
echo ""
echo "Result files:"
ls -la benchmarks/*.txt 2>/dev/null || echo "(no result files found)"
echo ""
echo "To compare results, run:"
echo "  ./scripts/compare-versions.sh"
