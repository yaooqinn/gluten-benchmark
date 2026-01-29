/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.benchmark.aggregate

import org.apache.gluten.benchmark.{BenchmarkDef, GlutenBenchmarkBase}

/**
 * Benchmark for aggregate operations.
 *
 * To run:
 *   sbt "runMain org.apache.gluten.benchmark.aggregate.AggregateBenchmark"
 *
 * To generate results file:
 *   SPARK_GENERATE_BENCHMARK_FILES=1 sbt "runMain org.apache.gluten.benchmark.aggregate.AggregateBenchmark"
 */
object AggregateBenchmark extends GlutenBenchmarkBase {

  override protected def defaultCardinality: Long = 10_000_000L

  private val N = defaultCardinality

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // Simple aggregations (no grouping)
    "SUM(id)" -> { _.range(N).selectExpr("sum(id)") },

    "COUNT(*)" -> { _.range(N).selectExpr("count(*)") },

    "AVG(id)" -> { _.range(N).selectExpr("avg(id)") },

    "MIN/MAX" -> { _.range(N).selectExpr("min(id)", "max(id)") },

    "STDDEV" -> { _.range(N).selectExpr("stddev(id)") },

    // Aggregations with GROUP BY
    "SUM with GROUP BY (low cardinality)" -> { spark =>
      spark.range(N)
        .selectExpr("id", "id % 100 as key")
        .groupBy("key")
        .sum("id")
    },

    "SUM with GROUP BY (medium cardinality)" -> { spark =>
      spark.range(N)
        .selectExpr("id", "id % 10000 as key")
        .groupBy("key")
        .sum("id")
    },

    "SUM with GROUP BY (high cardinality)" -> { spark =>
      spark.range(N)
        .selectExpr("id", "id % 1000000 as key")
        .groupBy("key")
        .sum("id")
    },

    // COUNT DISTINCT
    "COUNT DISTINCT (low cardinality)" -> { spark =>
      spark.range(N).selectExpr("count(distinct id % 100)")
    },

    "COUNT DISTINCT (high cardinality)" -> { spark =>
      spark.range(N).selectExpr("count(distinct id)")
    },

    // Multiple aggregations
    "Multiple aggregations" -> { spark =>
      spark.range(N)
        .selectExpr("id", "id % 100 as key")
        .groupBy("key")
        .agg(
          "id" -> "sum",
          "id" -> "avg",
          "id" -> "min",
          "id" -> "max",
          "id" -> "count"
        )
    }
  )
}
