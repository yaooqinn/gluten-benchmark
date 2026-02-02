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
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

/**
 * Benchmark for map functions with comprehensive type validation.
 * Tests: map, map_concat, map_contains_key, map_entries, map_from_arrays,
 *        map_from_entries, map_keys, map_values, str_to_map
 *
 * To run:
 *   ./build/sbt "runMain org.apache.gluten.benchmark.functions.MapFunctionsBenchmark"
 */
object MapFunctionsBenchmark extends GlutenBenchmarkBase {

  // 30M rows - balance between meaningful results and reasonable runtime
  override def defaultCardinality: Long = 30000000L

  private val N = defaultCardinality
  private val dataPath = s"/tmp/gluten-benchmark-maps-$N"

  /** Generate test Parquet data with various types for map operations */
  private def ensureDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(dataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating map test data at $dataPath ...")
      // scalastyle:on println

      spark.range(N)
        .selectExpr(
          "id",
          "id % 100 as key_int",
          "id % 1000 as key_int2",
          "cast(id % 100 as string) as key_str",
          "cast(id % 1000 as double) as val_double",
          "concat('val_', id % 500) as val_str",
          // Pre-built maps with different key/value types
          "map('k1', id % 100, 'k2', id % 200) as map_str_int",
          "map(id % 10, concat('v', id % 100)) as map_int_str",
          "map(cast(id % 5 as string), cast(id % 100 as double)) as map_str_double",
          // Arrays for map_from_arrays (ensure unique keys)
          "array(cast(id as int), cast(id + 1 as int), cast(id + 2 as int)) as arr_keys",
          "array(concat('a', id), concat('b', id), concat('c', id)) as arr_vals",
          // String for str_to_map
          "concat('a:', id % 100, ',b:', id % 200, ',c:', id % 300) as map_string",
          // Entries for map_from_entries
          "array(struct('x', id % 50), struct('y', id % 100)) as entries_str_int"
        )
        .write
        .mode(SaveMode.Overwrite)
        .parquet(dataPath)

      // scalastyle:off println
      println(s"  Map test data generated.")
      // scalastyle:on println
    }
  }

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // ========================================
    // map() - Create map from key-value pairs
    // ========================================
    BenchmarkDef("map(int, int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map(key_int, key_int2) as m", "key_int")
        .selectExpr("sum(m[key_int])")
    ).withSetup(ensureDataExists),

    BenchmarkDef("map(string, int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map(key_str, key_int) as m", "key_str")
        .selectExpr("sum(m[key_str])")
    ),

    BenchmarkDef("map(string, double)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map(key_str, val_double) as m", "key_str")
        .selectExpr("sum(m[key_str])")
    ),

    BenchmarkDef("map(string, string)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map(key_str, val_str) as m", "key_str")
        .selectExpr("count(m[key_str])")
    ),

    // ========================================
    // map_concat() - Concatenate maps
    // ========================================
    BenchmarkDef("map_concat(str_int, str_int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_concat(map_str_int, map('k3', id % 300)) as m")
        .selectExpr("sum(m['k1'])")
    ),

    BenchmarkDef("map_concat(int_str, int_str)", N, spark =>
      spark.read.parquet(dataPath)
        // Access key that exists in all rows (id % 10 is always in map_int_str)
        .selectExpr("map_concat(map_int_str, map(100 + id % 10, 'new')) as m", "id % 10 as orig_key")
        .selectExpr("count(m[orig_key])")
    ),

    // ========================================
    // map_contains_key() - Check key existence
    // ========================================
    BenchmarkDef("map_contains_key(str)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_contains_key(map_str_int, 'k1') as has_key")
        .selectExpr("count(*) filter (where has_key)")
    ),

    BenchmarkDef("map_contains_key(int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_contains_key(map_int_str, 5) as has_key")
        .selectExpr("count(*) filter (where has_key)")
    ),

    // ========================================
    // map_entries() - Get array of entries
    // ========================================
    BenchmarkDef("map_entries(str_int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_entries(map_str_int) as entries")
        .selectExpr("sum(entries[0].value)")
    ),

    BenchmarkDef("map_entries(int_str)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_entries(map_int_str) as entries")
        .selectExpr("count(entries[0].value)")
    ),

    // ========================================
    // map_from_arrays() - Create map from arrays
    // ========================================
    BenchmarkDef("map_from_arrays(int, str)", N, spark =>
      spark.read.parquet(dataPath)
        // Access first key from the same array used to construct the map
        .selectExpr("map_from_arrays(arr_keys, arr_vals) as m", "cast(id as int) as first_key")
        .selectExpr("count(m[first_key])")
    ),

    BenchmarkDef("map_from_arrays(str, int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr(
          "array(key_str, concat(key_str, '_2')) as str_keys",
          "array(key_int, key_int2) as int_vals",
          "key_str"
        )
        // Access first key directly rather than via array
        .selectExpr("map_from_arrays(str_keys, int_vals) as m", "key_str")
        .selectExpr("sum(m[key_str])")
    ),

    // ========================================
    // map_from_entries() - Create map from entries array
    // ========================================
    BenchmarkDef("map_from_entries(str_int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_from_entries(entries_str_int) as m")
        .selectExpr("sum(m['x'])")
    ),

    BenchmarkDef("map_from_entries(dynamic)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_from_entries(array(struct(key_str, key_int))) as m", "key_str")
        .selectExpr("sum(m[key_str])")
    ),

    // ========================================
    // map_keys() - Get array of keys
    // ========================================
    BenchmarkDef("map_keys(str_int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_keys(map_str_int) as keys")
        .selectExpr("count(keys[0])")
    ),

    BenchmarkDef("map_keys(int_str)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_keys(map_int_str) as keys")
        .selectExpr("sum(keys[0])")
    ),

    // ========================================
    // map_values() - Get array of values
    // ========================================
    BenchmarkDef("map_values(str_int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_values(map_str_int) as vals")
        .selectExpr("sum(vals[0])")
    ),

    BenchmarkDef("map_values(int_str)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_values(map_int_str) as vals")
        .selectExpr("count(vals[0])")
    ),

    BenchmarkDef("map_values(str_double)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_values(map_str_double) as vals")
        .selectExpr("sum(vals[0])")
    ),

    // ========================================
    // str_to_map() - Parse string to map
    // ========================================
    BenchmarkDef("str_to_map default delim", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("str_to_map(map_string) as m")
        .selectExpr("sum(cast(m['a'] as int))")
    ),

    BenchmarkDef("str_to_map custom delim", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("str_to_map(map_string, ',', ':') as m")
        .selectExpr("sum(cast(m['a'] as int))")
    ),

    // ========================================
    // Combined operations
    // ========================================
    BenchmarkDef("map + keys + values", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr(
          "map(key_str, key_int) as m"
        )
        .selectExpr(
          "map_keys(m) as k",
          "map_values(m) as v"
        )
        .selectExpr("count(k[0])", "sum(v[0])")
    ),

    BenchmarkDef("map_concat + map_entries", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_concat(map_str_int, map('k3', id % 300)) as m")
        .selectExpr("map_entries(m) as entries")
        .selectExpr("sum(entries[0].value)")
    ),

    BenchmarkDef("map_from_arrays + map_contains_key", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_from_arrays(arr_keys, arr_vals) as m", "arr_keys")
        .selectExpr("map_contains_key(m, arr_keys[0]) as has_key")
        .selectExpr("count(*) filter (where has_key)")
    )
  )
}
