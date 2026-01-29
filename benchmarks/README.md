# Benchmark Results

This directory contains benchmark results comparing Vanilla Spark vs Gluten+Velox across Spark versions.

## Directory Structure

```
benchmarks/
├── AggregateBenchmark-spark3.5.5-results.txt     # Current results (no date)
├── 2026-01-30/                                    # Daily results
│   ├── AggregateBenchmark-spark3.5.5-results.txt
│   ├── StringFunctionsBenchmark-spark3.5.5-results.txt
│   └── metadata.json
├── 2026-01-29/
│   └── ...
└── README.md
```

## Running Benchmarks

### Single Run (current directory)

```bash
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
```

### Daily Run (dated directory)

```bash
# Results saved to benchmarks/YYYY-MM-DD/
./scripts/run-daily-benchmark.sh

# Or manually with date
SPARK_GENERATE_BENCHMARK_FILES=1 \
SPARK_BENCHMARK_DATE=$(date +%Y-%m-%d) \
./build/sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
```

### Multi-Version Run

```bash
./scripts/run-multi-version.sh
```

## Comparing Results

### Across Spark Versions

```bash
./scripts/compare-versions.sh
./scripts/compare-versions.sh SUM  # Filter
```

### Across Dates (Daily Trend)

```bash
./scripts/compare-daily.sh         # Last 7 days
./scripts/compare-daily.sh 30      # Last 30 days
./scripts/compare-daily.sh 7 SUM   # Filter to SUM benchmarks
```

**Sample Output:**

```
Benchmark (Gluten time ms)                        | 2026-01-28 | 2026-01-29 | 2026-01-30 | Trend
=================================================================================================
COUNT DISTINCT (high cardinality)                 |        621 |        589 |        565 | ⬇️ -9% (improvement)
SUM(id)                                           |        455 |        451 |        451 | ➡️ stable
STDDEV                                            |        420 |        435 |        450 | ⬆️ +7% (regression)
=================================================================================================
```

### Scheduled (GitHub Actions)

The workflow runs at 2 AM UTC daily and commits results automatically.

```bash
# Trigger manually
gh workflow run benchmark.yml
```
