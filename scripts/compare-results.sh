#!/bin/bash
# Compare benchmark results between two directories
# Usage: ./compare-results.sh <current-dir> <baseline-dir>

set -euo pipefail

CURRENT_DIR="${1:-.}"
BASELINE_DIR="${2:-baseline}"
THRESHOLD="${THRESHOLD:-0.10}"  # 10% regression threshold

echo "Comparing benchmark results..."
echo "Current: $CURRENT_DIR"
echo "Baseline: $BASELINE_DIR"
echo "Threshold: ${THRESHOLD}x regression"
echo "=========================================="

REGRESSIONS=0

for current_file in "$CURRENT_DIR"/*.txt; do
    filename=$(basename "$current_file")
    baseline_file="$BASELINE_DIR/$filename"
    
    if [ ! -f "$baseline_file" ]; then
        echo "[NEW] $filename (no baseline)"
        continue
    fi
    
    echo ""
    echo "=== $filename ==="
    
    # Extract Gluten times (simplified - real impl would parse properly)
    current_gluten=$(grep "Gluten + Velox" "$current_file" | awk '{print $4}' | head -1)
    baseline_gluten=$(grep "Gluten + Velox" "$baseline_file" | awk '{print $4}' | head -1)
    
    if [ -n "$current_gluten" ] && [ -n "$baseline_gluten" ]; then
        # Calculate change (requires bc)
        if command -v bc &> /dev/null; then
            change=$(echo "scale=2; ($current_gluten - $baseline_gluten) / $baseline_gluten" | bc)
            echo "Gluten: ${baseline_gluten}ms -> ${current_gluten}ms (${change}x change)"
            
            # Check for regression
            is_regression=$(echo "$change > $THRESHOLD" | bc)
            if [ "$is_regression" -eq 1 ]; then
                echo "⚠️  REGRESSION DETECTED!"
                REGRESSIONS=$((REGRESSIONS + 1))
            fi
        else
            echo "Gluten: ${baseline_gluten}ms -> ${current_gluten}ms"
        fi
    fi
done

echo ""
echo "=========================================="
if [ $REGRESSIONS -gt 0 ]; then
    echo "❌ Found $REGRESSIONS regression(s)"
    exit 1
else
    echo "✅ No regressions detected"
    exit 0
fi
