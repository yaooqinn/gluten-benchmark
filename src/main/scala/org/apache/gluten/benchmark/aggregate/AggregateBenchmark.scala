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
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

/**
 * Benchmark for aggregate operations using Parquet input.
 * Uses file I/O which is a realistic workload for Gluten/Velox.
 *
 * To run:
 *   ./build/sbt "runMain org.apache.gluten.benchmark.aggregate.AggregateBenchmark"
 */
object AggregateBenchmark extends GlutenBenchmarkBase {

  // 50M rows for basic tests
  override def defaultCardinality: Long = 50000000L

  private val N = defaultCardinality
  private val dataPath = s"/tmp/gluten-benchmark-data-$N"
  private val nestedDataPath = s"/tmp/gluten-benchmark-nested-$N"
  
  /** Generate test Parquet data if it doesn't exist */
  private def ensureDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(dataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating test data at $dataPath ...")
      // scalastyle:on println
      
      // Create test data with various columns
      spark.range(N)
        .selectExpr(
          "id",
          "id % 100 as key_low",
          "id % 10000 as key_med",
          "id % 500000 as key_high",
          "cast(rand() * 1000 as double) as value",
          "concat('str_', id % 1000) as str_col"
        )
        .write
        .mode(SaveMode.Overwrite)
        .parquet(dataPath)
      
      // scalastyle:off println
      println(s"  Test data generated.")
      // scalastyle:on println
    }
  }

  /** Generate nested type test data */
  private def ensureNestedDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(nestedDataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating nested test data at $nestedDataPath ...")
      // scalastyle:on println
      
      spark.range(N)
        .selectExpr(
          "id",
          "id % 100 as key_low",
          "id % 10000 as key_med",
          // Array type
          "array(id % 10, id % 20, id % 30) as arr_col",
          // Struct type
          "named_struct('a', id % 100, 'b', concat('s', id % 50)) as struct_col",
          // Map type
          "map('k1', id % 100, 'k2', id % 200) as map_col"
        )
        .write
        .mode(SaveMode.Overwrite)
        .parquet(nestedDataPath)
      
      // scalastyle:off println
      println(s"  Nested test data generated.")
      // scalastyle:on println
    }
  }

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // Simple aggregations (no grouping)
    BenchmarkDef("SUM(id)", N, 
      spark => spark.read.parquet(dataPath).selectExpr("sum(id)")
    ).withSetup(ensureDataExists),

    BenchmarkDef("COUNT(*)", N,
      spark => spark.read.parquet(dataPath).selectExpr("count(*)")
    ),

    BenchmarkDef("AVG(value)", N,
      spark => spark.read.parquet(dataPath).selectExpr("avg(value)")
    ),

    BenchmarkDef("MIN/MAX", N,
      spark => spark.read.parquet(dataPath).selectExpr("min(id)", "max(id)", "min(value)", "max(value)")
    ),

    // Aggregations with GROUP BY
    BenchmarkDef("SUM GROUP BY (low card)", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_low")
        .sum("id")
    ),

    BenchmarkDef("SUM GROUP BY (med card)", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_med")
        .sum("id")
    ),

    BenchmarkDef("SUM GROUP BY (high card)", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_high")
        .sum("id")
    ),

    // Multiple aggregations
    BenchmarkDef("Multi-agg GROUP BY", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_low")
        .agg(
          "id" -> "sum",
          "value" -> "avg",
          "id" -> "min",
          "id" -> "max",
          "id" -> "count"
        )
    ),

    // Filter + Aggregate (pushdown test)
    BenchmarkDef("Filter + SUM", N, spark =>
      spark.read.parquet(dataPath)
        .filter("id > 25000000")
        .selectExpr("sum(id)")
    ),

    // String aggregation
    BenchmarkDef("COUNT DISTINCT str", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("count(distinct str_col)")
    ),

    // Collection aggregations - use high cardinality key to limit array sizes
    BenchmarkDef("collect_list(int)", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_high")
        .agg("key_low" -> "collect_list")
    ),

    BenchmarkDef("collect_set(int)", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_high")
        .agg("key_low" -> "collect_set")
    ),

    BenchmarkDef("collect_set(string)", N, spark =>
      spark.read.parquet(dataPath)
        .groupBy("key_high")
        .agg("str_col" -> "collect_set")
    ),

    // Nested type aggregations
    BenchmarkDef("SUM array element", N, spark =>
      spark.read.parquet(nestedDataPath)
        .selectExpr("sum(arr_col[0])")
    ).withSetup(ensureNestedDataExists),

    BenchmarkDef("SUM struct field", N, spark =>
      spark.read.parquet(nestedDataPath)
        .selectExpr("sum(struct_col.a)")
    ),

    BenchmarkDef("SUM map value", N, spark =>
      spark.read.parquet(nestedDataPath)
        .selectExpr("sum(map_col['k1'])")
    ),

    BenchmarkDef("GROUP BY array element", N, spark =>
      spark.read.parquet(nestedDataPath)
        .selectExpr("arr_col[0] as arr_elem", "*")
        .groupBy("arr_elem")
        .count()
    ),

    BenchmarkDef("GROUP BY struct field", N, spark =>
      spark.read.parquet(nestedDataPath)
        .selectExpr("struct_col.a as struct_field", "*")
        .groupBy("struct_field")
        .count()
    ),

    BenchmarkDef("collect_list(array)", N, spark =>
      spark.read.parquet(nestedDataPath)
        .groupBy("key_med")
        .agg("arr_col" -> "collect_list")
    ),

    BenchmarkDef("collect_list(struct)", N, spark =>
      spark.read.parquet(nestedDataPath)
        .groupBy("key_med")
        .agg("struct_col" -> "collect_list")
    )
  )
}
