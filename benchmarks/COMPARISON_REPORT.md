# Gluten Benchmark Comparison Report

**Date:** 2026-01-30  
**Spark Version:** 3.5.5  
**Gluten Version:** 1.6.0-SNAPSHOT (nightly)

## Executive Summary

This report compares Vanilla Spark vs Gluten+Velox performance across two types of benchmarks:

1. **Range-based (synthetic)** - Uses `spark.range()` for data generation
2. **Parquet-based (I/O)** - Uses Parquet file input (100M rows)

### Key Finding

Gluten performs **significantly better with Parquet I/O** workloads:
- Parquet benchmarks show **1.2-2.0x speedup** for most operations
- Range-based benchmarks show overhead due to row→columnar conversion

---

## Parquet-Based Benchmarks (Recommended)

These benchmarks use real file I/O which is the intended use case for Gluten/Velox.

| Benchmark | Vanilla (ms) | Gluten (ms) | Speedup | Result |
|-----------|-------------|-------------|---------|--------|
| Parquet SUM(id) | 341 | 273 | **1.2x** | ✅ Faster |
| Parquet COUNT(*) | 149 | 154 | 0.9x | ➡️ Similar |
| Parquet AVG(value) | 231 | 227 | 1.0x | ➡️ Similar |
| Parquet MIN/MAX | 366 | 406 | 0.9x | ➡️ Similar |
| Parquet SUM GROUP BY (low) | 390 | 300 | **1.3x** | ✅ Faster |
| Parquet SUM GROUP BY (med) | 433 | 320 | **1.4x** | ✅ Faster |
| Parquet SUM GROUP BY (high) | 2773 | 1884 | **1.5x** | ✅ Faster |
| Parquet Multi-agg GROUP BY | 690 | 442 | **1.6x** | ✅ Faster |
| Parquet Filter + SUM | 216 | 237 | 0.9x | ➡️ Similar |
| Parquet COUNT DISTINCT str | 536 | 252 | **2.1x** | ✅ Faster |

**Summary:** 6 benchmarks faster, 4 similar, 0 slower

---

## Range-Based Benchmarks (Synthetic)

These benchmarks use `spark.range()` which generates row-based data requiring conversion.

| Benchmark | Vanilla (ms) | Gluten (ms) | Speedup | Result |
|-----------|-------------|-------------|---------|--------|
| SUM(id) | 427 | 451 | 0.9x | ➡️ Similar |
| COUNT(*) | 362 | 428 | 0.8x | ⚠️ Overhead |
| AVG(id) | 351 | 440 | 0.8x | ⚠️ Overhead |
| MIN/MAX | 343 | 390 | 0.9x | ➡️ Similar |
| STDDEV | 275 | 435 | 0.6x | ⚠️ Overhead |
| SUM GROUP BY (low) | 357 | 424 | 0.8x | ⚠️ Overhead |
| SUM GROUP BY (med) | 457 | 483 | 0.9x | ➡️ Similar |
| SUM GROUP BY (high) | 1773 | 1557 | **1.1x** | ✅ Faster |
| COUNT DISTINCT (low) | 318 | 479 | 0.7x | ⚠️ Overhead |
| COUNT DISTINCT (high) | 1353 | 565 | **2.4x** | ✅ Faster |
| Multiple aggregations | 330 | 474 | 0.7x | ⚠️ Overhead |

**Summary:** 2 benchmarks faster, 3 similar, 6 with overhead

---

## Analysis

### Why Parquet is Faster

1. **Native Parquet Reader**: Gluten's Velox backend reads Parquet directly into columnar format
2. **No Conversion Overhead**: Data stays columnar from disk to output
3. **Vectorized I/O**: Velox's SIMD-optimized I/O operations

### Why Range Shows Overhead

1. **Row-to-Columnar Conversion**: `spark.range()` generates row-based data
2. **JNI Overhead**: Each conversion requires JVM↔Native boundary crossing
3. **Small Data Size**: For operations completing in <500ms, fixed overhead dominates

### When Gluten Excels

Even with Range data, Gluten shows benefits for:
- **High-cardinality operations** (GROUP BY, COUNT DISTINCT with many groups)
- **Large data volumes** (where actual processing time dominates overhead)

---

## Recommendations

1. **Use Parquet input** for realistic benchmarking
2. **Focus on TPC-H/TPC-DS** for production evaluation
3. **Range benchmarks** are useful for measuring JNI overhead, not query performance
4. **Minimum data size**: 10+ seconds of Vanilla execution for fair comparison

---

## Environment

- **OS**: Ubuntu 22.04
- **Java**: OpenJDK 17.0.17
- **Spark**: 3.5.5
- **Gluten**: 1.6.0-SNAPSHOT
- **Velox Backend**: bundled with Gluten
- **Data Size**: 100M rows
- **Warmup Iterations**: 5
- **Measurement Iterations**: 5
