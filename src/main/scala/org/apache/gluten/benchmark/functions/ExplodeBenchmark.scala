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

package org.apache.gluten.benchmark.functions

import org.apache.gluten.benchmark.{BenchmarkDef, GlutenBenchmarkBase}

import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * Benchmark for explode/generator functions:
 * - explode, explode_outer
 * - inline, inline_outer
 * - posexplode, posexplode_outer
 */
object ExplodeBenchmark extends GlutenBenchmarkBase {

  // 10M rows (explode multiplies rows so we use smaller dataset)
  override def defaultCardinality: Long = 10000000L

  private val N = defaultCardinality
  private val dataPath = s"/tmp/gluten-benchmark-explode-$N"

  private def ensureDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(dataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating explode test data at $dataPath ...")
      // scalastyle:on println

      spark.range(N)
        .selectExpr(
          "id",
          // Arrays for explode
          "array(cast(id % 100 as int), cast((id + 1) % 100 as int), cast((id + 2) % 100 as int)) as arr_int",
          "array(concat('s', id % 100), concat('s', (id + 1) % 100)) as arr_str",
          // Array with nulls for explode_outer
          "if(id % 5 = 0, null, array(id % 50, (id + 1) % 50)) as arr_nullable",
          // Maps for explode
          "map('k1', id % 100, 'k2', (id + 1) % 100) as map_str_int",
          "map(cast(id % 10 as int), concat('v', id % 100)) as map_int_str",
          // Map with nulls for explode_outer
          "if(id % 4 = 0, null, map('a', id % 30)) as map_nullable",
          // Array of structs for inline
          "array(named_struct('x', id % 100, 'y', id % 50), named_struct('x', (id+1) % 100, 'y', (id+1) % 50)) as arr_struct",
          // Nullable array of structs for inline_outer
          "if(id % 3 = 0, null, array(named_struct('a', id % 20, 'b', concat('v', id % 20)))) as arr_struct_nullable"
        )
        .write.parquet(dataPath)
    }
  }

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // ========================================
    // explode() - Expand array/map to rows
    // ========================================
    BenchmarkDef("explode(array[int])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode(arr_int) as val")
        .selectExpr("sum(val)")
    ).withSetup(ensureDataExists),

    BenchmarkDef("explode(array[string])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode(arr_str) as val")
        .selectExpr("count(val)")
    ),

    BenchmarkDef("explode(map[str,int])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode(map_str_int) as (k, v)")
        .selectExpr("sum(v)")
    ),

    BenchmarkDef("explode(map[int,str])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode(map_int_str) as (k, v)")
        .selectExpr("count(v)")
    ),

    // ========================================
    // explode_outer() - Preserves nulls
    // ========================================
    BenchmarkDef("explode_outer(array)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode_outer(arr_nullable) as val")
        .selectExpr("count(*)", "count(val)")
    ),

    BenchmarkDef("explode_outer(map)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode_outer(map_nullable) as (k, v)")
        .selectExpr("count(*)", "count(k)")
    ),

    // ========================================
    // posexplode() - Explode with position
    // ========================================
    BenchmarkDef("posexplode(array[int])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("posexplode(arr_int) as (pos, val)")
        .selectExpr("sum(pos)", "sum(val)")
    ),

    BenchmarkDef("posexplode(array[string])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("posexplode(arr_str) as (pos, val)")
        .selectExpr("max(pos)", "count(val)")
    ),

    BenchmarkDef("posexplode(map)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("posexplode(map_str_int) as (pos, k, v)")
        .selectExpr("sum(pos)", "sum(v)")
    ),

    // ========================================
    // posexplode_outer() - With position, preserves nulls
    // ========================================
    BenchmarkDef("posexplode_outer(array)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("posexplode_outer(arr_nullable) as (pos, val)")
        .selectExpr("count(*)", "sum(coalesce(pos, -1))")
    ),

    BenchmarkDef("posexplode_outer(map)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("posexplode_outer(map_nullable) as (pos, k, v)")
        .selectExpr("count(*)", "count(k)")
    ),

    // ========================================
    // inline() - Explode array of structs
    // ========================================
    BenchmarkDef("inline(array[struct])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("inline(arr_struct) as (x, y)")
        .selectExpr("sum(x)", "sum(y)")
    ),

    // ========================================
    // inline_outer() - Preserves nulls
    // ========================================
    BenchmarkDef("inline_outer(array[struct])", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("inline_outer(arr_struct_nullable) as (a, b)")
        .selectExpr("count(*)", "sum(coalesce(a, 0))")
    ),

    // ========================================
    // Combined operations
    // ========================================
    BenchmarkDef("explode + filter + agg", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode(arr_int) as val")
        .where("val > 50")
        .selectExpr("sum(val)", "count(*)")
    ),

    BenchmarkDef("explode + group by", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("explode(arr_int) as val")
        .groupBy("val")
        .count()
        .selectExpr("sum(count)")
    ),

    BenchmarkDef("posexplode + lateral view", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("id", "posexplode(arr_int) as (pos, val)")
        .where("pos = 0")
        .selectExpr("sum(val)")
    )
  )
}
