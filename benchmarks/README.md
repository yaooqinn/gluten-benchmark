# Benchmark Results

This directory contains benchmark results tracked over time.

Results are organized by date and Spark version:

```
benchmarks/
├── 2026-01-29/
│   ├── spark-3.4.4/
│   │   ├── AggregateBenchmark-results.txt
│   │   ├── StringFunctionsBenchmark-results.txt
│   │   └── metadata.json
│   └── spark-3.5.5/
│       ├── AggregateBenchmark-results.txt
│       ├── StringFunctionsBenchmark-results.txt
│       └── metadata.json
└── baseline/
    └── (reference results for regression detection)
```

## Viewing Results

Results follow Spark's standard benchmark format:

```
SUM(id):                                  Best Time(ms)   Avg Time(ms)   Relative
----------------------------------------------------------------------------------
Vanilla Spark                                      1523           1587       1.0X
Gluten + Velox                                      342            358       4.5X
```

## Regression Detection

Run `scripts/compare-results.sh` to compare against baseline:

```bash
./scripts/compare-results.sh benchmarks/2026-01-29/spark-3.5.5 benchmarks/baseline
```
