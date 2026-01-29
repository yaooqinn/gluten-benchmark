#!/bin/bash
# Compare benchmark results across Spark versions
# Usage: ./scripts/compare-versions.sh [benchmark-name-filter]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BENCHMARKS_DIR="$PROJECT_DIR/benchmarks"

FILTER="${1:-}"

echo "========================================"
echo "Gluten Benchmark Version Comparison"
echo "========================================"
echo ""

# Find all result files
mapfile -t RESULT_FILES < <(find "$BENCHMARKS_DIR" -name "*-spark*-results.txt" -type f 2>/dev/null | sort)

if [[ ${#RESULT_FILES[@]} -eq 0 ]]; then
    echo "No result files found in $BENCHMARKS_DIR"
    echo ""
    echo "Run benchmarks first with:"
    echo "  SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt 'runMain org.apache.gluten.benchmark.RunAllBenchmarks'"
    exit 1
fi

echo "Found result files:"
for f in "${RESULT_FILES[@]}"; do
    basename "$f"
done
echo ""

# Extract version from filename
get_version() {
    basename "$1" | sed -E 's/.*-spark([0-9.]+)-results\.txt/\1/'
}

# Get sorted list of versions
declare -a SORTED_VERSIONS=()
for file in "${RESULT_FILES[@]}"; do
    SORTED_VERSIONS+=("$(get_version "$file")")
done
mapfile -t SORTED_VERSIONS < <(printf '%s\n' "${SORTED_VERSIONS[@]}" | sort -Vu)

# Print header
WIDTH=50
printf "%-${WIDTH}s" "Benchmark"
for ver in "${SORTED_VERSIONS[@]}"; do
    printf " | %-20s" "Spark $ver (V/G)"
done
echo ""
echo "$(printf '=%.0s' $(seq 1 $((WIDTH + 25 * ${#SORTED_VERSIONS[@]}))))"

# Parse each result file and build data
declare -A DATA  # DATA["benchmark::version"]="vanilla/gluten"
declare -A BENCHNAMES

for file in "${RESULT_FILES[@]}"; do
    ver=$(get_version "$file")
    current_bench=""
    vanilla_time=""
    gluten_time=""
    
    while IFS= read -r line; do
        # New benchmark section (line ending with colon, not all dashes)
        if [[ "$line" =~ ^[A-Za-z0-9_\(\)].*:$ && ! "$line" =~ ^-+ ]]; then
            # Save previous benchmark if we have data
            if [[ -n "$current_bench" && (-n "$vanilla_time" || -n "$gluten_time") ]]; then
                key="${current_bench}::${ver}"
                DATA["$key"]="${vanilla_time:-?}/${gluten_time:-?}"
                BENCHNAMES["$current_bench"]=1
            fi
            current_bench="${line%:}"
            vanilla_time=""
            gluten_time=""
        fi
        
        # Result line for Vanilla Spark
        if [[ "$line" =~ ^Vanilla\ Spark[[:space:]]+ ]]; then
            vanilla_time=$(echo "$line" | awk '{print $3}')
        fi
        
        # Result line for Gluten + Velox
        if [[ "$line" =~ ^Gluten\ \+\ Velox[[:space:]]+ ]]; then
            gluten_time=$(echo "$line" | awk '{print $4}')
        fi
    done < "$file"
    
    # Don't forget the last benchmark
    if [[ -n "$current_bench" && (-n "$vanilla_time" || -n "$gluten_time") ]]; then
        key="${current_bench}::${ver}"
        DATA["$key"]="${vanilla_time:-?}/${gluten_time:-?}"
        BENCHNAMES["$current_bench"]=1
    fi
done

# Print data rows
while IFS= read -r bench; do
    [[ -z "$bench" ]] && continue
    
    # Apply filter if specified
    if [[ -n "$FILTER" && ! "$bench" =~ $FILTER ]]; then
        continue
    fi
    
    # Truncate long names
    display_name="${bench:0:$((WIDTH-2))}"
    printf "%-${WIDTH}s" "$display_name"
    
    for ver in "${SORTED_VERSIONS[@]}"; do
        key="${bench}::${ver}"
        value="${DATA[$key]:--/-}"
        printf " | %-20s" "$value"
    done
    echo ""
done < <(printf '%s\n' "${!BENCHNAMES[@]}" | sort)

echo "$(printf '=%.0s' $(seq 1 $((WIDTH + 25 * ${#SORTED_VERSIONS[@]}))))"
echo ""
echo "Legend: V = Vanilla Spark best time (ms), G = Gluten+Velox best time (ms)"
echo "Lower is better. Format: vanilla_time/gluten_time"
echo ""

# Summary
echo "Summary:"
echo "--------"
for ver in "${SORTED_VERSIONS[@]}"; do
    vanilla_wins=0
    gluten_wins=0
    
    for bench in "${!BENCHNAMES[@]}"; do
        key="${bench}::${ver}"
        value="${DATA[$key]:--/-}"
        v="${value%/*}"
        g="${value#*/}"
        
        if [[ "$v" =~ ^[0-9]+$ && "$g" =~ ^[0-9]+$ ]]; then
            if (( v < g )); then
                vanilla_wins=$((vanilla_wins + 1))
            elif (( g < v )); then
                gluten_wins=$((gluten_wins + 1))
            fi
        fi
    done
    
    if (( vanilla_wins + gluten_wins > 0 )); then
        echo "  Spark $ver: Vanilla faster in $vanilla_wins, Gluten faster in $gluten_wins benchmarks"
    fi
done
