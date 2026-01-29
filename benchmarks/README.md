# Benchmark Results

This directory contains benchmark results comparing Vanilla Spark vs Gluten+Velox across Spark versions.

## Result File Naming

Files are named with Spark version: `{BenchmarkClass}-spark{version}-results.txt`

```
benchmarks/
├── AggregateBenchmark-spark3.4.4-results.txt
├── AggregateBenchmark-spark3.5.5-results.txt
├── AggregateBenchmark-spark4.0.0-results.txt
├── StringFunctionsBenchmark-spark3.5.5-results.txt
└── README.md
```

## Running Benchmarks

### Single Version

```bash
# Generate result files for current Spark version
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"

# With specific Spark version
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt \
  -Dspark.version=3.4.4 \
  "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
```

### Multi-Version Comparison

```bash
# Run benchmarks on all supported Spark versions (3.4, 3.5, 4.0)
./scripts/run-multi-version.sh

# Run only aggregate benchmarks
./scripts/run-multi-version.sh Aggregate
```

## Comparing Results

```bash
# Compare results across all versions
./scripts/compare-versions.sh

# Filter to specific benchmarks
./scripts/compare-versions.sh SUM
./scripts/compare-versions.sh "GROUP BY"
```

### Sample Output

```
==================================================================================
Benchmark                                | Spark 3.4.4 (V/G)  | Spark 3.5.5 (V/G)  
==================================================================================
SUM(id)                                  | 502/451            | 427/451            
COUNT(*)                                 | 398/428            | 362/428            
COUNT DISTINCT (high cardinality)        | 1405/621           | 1353/565           
==================================================================================

Legend: V = Vanilla Spark best time (ms), G = Gluten+Velox best time (ms)
```

## Result Format

Results follow Spark's standard benchmark format:

```
SUM(id):
--------------------------------------------------------------------------------
                                         Best Time(ms) Avg Time(ms)    Stdev(ms)   Relative
--------------------------------------------------------------------------------
Vanilla Spark                                     427          483         47.3        1.0X
Gluten + Velox                                    451          570         81.2        0.8X
--------------------------------------------------------------------------------
```
