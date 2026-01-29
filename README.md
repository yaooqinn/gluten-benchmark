# Gluten Micro-Benchmark

Fine-grained performance comparison between **Gluten + Velox** and **Vanilla Spark**.

## Overview

This project provides micro-benchmarks for SQL functions, operators, and scans to track performance regressions in Gluten across commits and Spark versions.

## Quick Start

### Prerequisites

- JDK 8 or 11
- sbt 1.9+
- Gluten native libraries (auto-loaded from JAR)

### Run Benchmarks

```bash
# Run all benchmarks
sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"

# Run specific benchmark suite
sbt "runMain org.apache.gluten.benchmark.aggregate.AggregateBenchmark"

# Filter by name
sbt "runMain org.apache.gluten.benchmark.aggregate.AggregateBenchmark SUM"

# Generate results files (tracked in git)
SPARK_GENERATE_BENCHMARK_FILES=1 sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
```

### Cross-Version Testing

```bash
# Test with different Spark versions
sbt -Dspark.version=3.4.4 "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
sbt -Dspark.version=3.5.5 "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
sbt -Dspark.version=4.0.0 "runMain org.apache.gluten.benchmark.RunAllBenchmarks"

# Test with specific Gluten version
sbt -Dgluten.version=1.5.0 "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
```

## Adding New Benchmarks

1. Create a new object extending `GlutenBenchmarkBase`:

```scala
package org.apache.gluten.benchmark.myfeature

import org.apache.gluten.benchmark.GlutenBenchmarkBase

object MyFeatureBenchmark extends GlutenBenchmarkBase {

  override protected def defaultCardinality: Long = 10_000_000L

  private val N = defaultCardinality

  override def benchmarks = Seq(
    // Simple tuple syntax: "name" -> { spark => DataFrame }
    "my operation" -> { _.range(N).selectExpr("my_function(id)") },

    // SQL string syntax
    "my sql query" -> s"SELECT my_function(id) FROM range($N)",

    // With explicit BenchmarkDef
    BenchmarkDef(
      name = "complex case",
      cardinality = N,
      workload = spark => spark.range(N).filter("id % 2 = 0"),
      setup = Some(spark => spark.sql("SET some.config = true"))
    )
  )
}
```

2. Register in `RunAllBenchmarks`:

```scala
private val allBenchmarks = Seq(
  aggregate.AggregateBenchmark,
  functions.StringFunctionsBenchmark,
  myfeature.MyFeatureBenchmark  // Add here
)
```

## Benchmark Categories

| Category | Module | Description |
|----------|--------|-------------|
| Aggregates | `aggregate/` | SUM, COUNT, AVG, GROUP BY, etc. |
| String Functions | `functions/` | length, substring, regex, etc. |
| Math Functions | `functions/` | (TODO) abs, round, power, etc. |
| DateTime | `functions/` | (TODO) date_add, date_format, etc. |
| Joins | `join/` | (TODO) Inner, Left, Semi, Anti |
| Scans | `scan/` | (TODO) Parquet, filtering, pruning |

## Output Format

Benchmark results follow Spark's standard format:

```
SUM(id):                                  Best Time(ms)   Avg Time(ms)   Relative
----------------------------------------------------------------------------------
Vanilla Spark                                      1523           1587       1.0X
Gluten + Velox                                      342            358       4.5X
```

## CI/CD Integration

Results are tracked in `benchmarks/` directory for regression detection.

```yaml
# GitHub Actions example
- name: Run benchmarks
  run: |
    SPARK_GENERATE_BENCHMARK_FILES=1 sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"

- name: Check for regressions
  run: ./scripts/compare-results.sh benchmarks/ baseline/
```

## Project Structure

```
gluten-benchmark/
├── build.sbt
├── src/main/scala/org/apache/gluten/benchmark/
│   ├── GlutenBenchmarkBase.scala   # Core framework
│   ├── BenchmarkDef.scala          # Benchmark definition
│   ├── RunAllBenchmarks.scala      # Entry point
│   ├── aggregate/                  # Aggregate benchmarks
│   └── functions/                  # Function benchmarks
├── benchmarks/                     # Results (git-tracked)
└── .github/workflows/              # CI configuration
```

## License

Apache License 2.0
