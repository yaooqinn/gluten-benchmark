#!/bin/bash
# Compare benchmark results across dates (daily trend analysis)
# Usage: ./scripts/compare-daily.sh [days] [benchmark-filter]
#
# Examples:
#   ./scripts/compare-daily.sh           # Compare last 7 days
#   ./scripts/compare-daily.sh 30        # Compare last 30 days
#   ./scripts/compare-daily.sh 7 SUM     # Compare SUM benchmarks over 7 days

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BENCHMARKS_DIR="$PROJECT_DIR/benchmarks"

DAYS="${1:-7}"
FILTER="${2:-}"

echo "========================================"
echo "Gluten Benchmark Daily Comparison"
echo "========================================"
echo "Looking back: $DAYS days"
echo ""

# Find dated directories (YYYY-MM-DD format)
mapfile -t DATE_DIRS < <(find "$BENCHMARKS_DIR" -maxdepth 1 -type d -name "20*-*-*" 2>/dev/null | sort -r | head -n "$DAYS")

if [[ ${#DATE_DIRS[@]} -eq 0 ]]; then
    echo "No dated result directories found."
    echo ""
    echo "Run daily benchmarks first:"
    echo "  ./scripts/run-daily-benchmark.sh"
    echo ""
    echo "Or check for non-dated results in benchmarks/"
    exit 1
fi

echo "Found ${#DATE_DIRS[@]} dated result sets:"
for d in "${DATE_DIRS[@]}"; do
    basename "$d"
done
echo ""

# Extract version from filename
get_version() {
    basename "$1" | sed -E 's/.*-spark([0-9.]+)-results\.txt/\1/'
}

# Parse results from a file
# Returns: benchmark_name|||vanilla_time|||gluten_time
parse_results() {
    local file="$1"
    local current_bench=""
    local vanilla_time=""
    local gluten_time=""
    
    while IFS= read -r line; do
        # New benchmark section
        if [[ "$line" =~ ^[A-Za-z0-9_\(\)].*:$ && ! "$line" =~ ^-+ ]]; then
            # Output previous benchmark
            if [[ -n "$current_bench" && (-n "$vanilla_time" || -n "$gluten_time") ]]; then
                echo "${current_bench}|||${vanilla_time:-?}|||${gluten_time:-?}"
            fi
            current_bench="${line%:}"
            vanilla_time=""
            gluten_time=""
        fi
        
        if [[ "$line" =~ ^Vanilla\ Spark[[:space:]]+ ]]; then
            vanilla_time=$(echo "$line" | awk '{print $3}')
        fi
        
        if [[ "$line" =~ ^Gluten\ \+\ Velox[[:space:]]+ ]]; then
            gluten_time=$(echo "$line" | awk '{print $4}')
        fi
    done < "$file"
    
    # Don't forget the last one
    if [[ -n "$current_bench" && (-n "$vanilla_time" || -n "$gluten_time") ]]; then
        echo "${current_bench}|||${vanilla_time:-?}|||${gluten_time:-?}"
    fi
}

# Collect data
declare -A DATA      # DATA["bench::date"]="gluten_time"
declare -A BENCHNAMES
declare -a SORTED_DATES

for dir in "${DATE_DIRS[@]}"; do
    date_name=$(basename "$dir")
    SORTED_DATES+=("$date_name")
    
    # Find result files in this directory
    for file in "$dir"/*-results.txt; do
        [[ -f "$file" ]] || continue
        
        while IFS= read -r line; do
            bench="${line%%|||*}"
            rest="${line#*|||}"
            vanilla="${rest%%|||*}"
            gluten="${rest#*|||}"
            
            [[ -z "$bench" ]] && continue
            
            key="${bench}::${date_name}"
            DATA["$key"]="$gluten"
            BENCHNAMES["$bench"]=1
        done < <(parse_results "$file")
    done
done

# Sort dates from oldest to newest
mapfile -t SORTED_DATES < <(printf '%s\n' "${SORTED_DATES[@]}" | sort)

# Print header
WIDTH=45
printf "%-${WIDTH}s" "Benchmark (Gluten time ms)"
for date in "${SORTED_DATES[@]}"; do
    printf " | %-10s" "$date"
done
echo " | Trend"
echo "$(printf '=%.0s' $(seq 1 $((WIDTH + 15 * ${#SORTED_DATES[@]} + 10))))"

# Print data rows with trend
for bench in $(printf '%s\n' "${!BENCHNAMES[@]}" | sort); do
    # Apply filter
    if [[ -n "$FILTER" && ! "$bench" =~ $FILTER ]]; then
        continue
    fi
    
    printf "%-${WIDTH}s" "${bench:0:$((WIDTH-2))}"
    
    first_val=""
    last_val=""
    
    for date in "${SORTED_DATES[@]}"; do
        key="${bench}::${date}"
        value="${DATA[$key]:--}"
        printf " | %10s" "$value"
        
        if [[ "$value" =~ ^[0-9]+$ ]]; then
            [[ -z "$first_val" ]] && first_val="$value"
            last_val="$value"
        fi
    done
    
    # Calculate trend
    if [[ -n "$first_val" && -n "$last_val" && "$first_val" -ne 0 ]]; then
        pct_change=$(( (last_val - first_val) * 100 / first_val ))
        if (( pct_change > 5 )); then
            printf " | ⬆️ +%d%% (regression)" "$pct_change"
        elif (( pct_change < -5 )); then
            printf " | ⬇️ %d%% (improvement)" "$pct_change"
        else
            printf " | ➡️ stable"
        fi
    else
        printf " | -"
    fi
    echo ""
done

echo "$(printf '=%.0s' $(seq 1 $((WIDTH + 15 * ${#SORTED_DATES[@]} + 10))))"
echo ""
echo "Legend:"
echo "  ⬆️ = Performance regression (>5% slower)"
echo "  ⬇️ = Performance improvement (>5% faster)"
echo "  ➡️ = Stable (within ±5%)"
echo ""

# Summary
echo "Summary:"
echo "--------"
if [[ ${#SORTED_DATES[@]} -ge 2 ]]; then
    oldest="${SORTED_DATES[0]}"
    newest="${SORTED_DATES[-1]}"
    
    regressions=0
    improvements=0
    stable=0
    
    for bench in "${!BENCHNAMES[@]}"; do
        old_key="${bench}::${oldest}"
        new_key="${bench}::${newest}"
        old_val="${DATA[$old_key]}"
        new_val="${DATA[$new_key]}"
        
        if [[ "$old_val" =~ ^[0-9]+$ && "$new_val" =~ ^[0-9]+$ && "$old_val" -ne 0 ]]; then
            pct=$(( (new_val - old_val) * 100 / old_val ))
            if (( pct > 5 )); then
                regressions=$((regressions + 1))
            elif (( pct < -5 )); then
                improvements=$((improvements + 1))
            else
                stable=$((stable + 1))
            fi
        fi
    done
    
    echo "  Comparing $oldest to $newest:"
    echo "    Regressions:   $regressions"
    echo "    Improvements:  $improvements"
    echo "    Stable:        $stable"
fi
