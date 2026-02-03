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
 * Benchmark for higher-order functions with lambda expressions:
 * - aggregate, array_sort, exists, filter, forall
 * - map_filter, map_zip_with, reduce, transform
 * - transform_keys, transform_values, zip_with
 */
object HigherOrderFunctionsBenchmark extends GlutenBenchmarkBase {

  override def defaultCardinality: Long = 20000000L // 20M rows

  private val N = defaultCardinality
  private val dataPath = s"/tmp/gluten-benchmark-hof-$N"

  private def ensureDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(dataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating higher-order functions test data at $dataPath ...")
      // scalastyle:on println

      spark.range(N)
        .selectExpr(
          "id",
          // Arrays for array functions
          "array(cast(id % 100 as int), cast((id + 1) % 100 as int), cast((id + 2) % 100 as int), cast((id + 3) % 100 as int)) as arr_int",
          "array(cast(id % 50 as double), cast((id + 1) % 50 as double), cast((id + 2) % 50 as double)) as arr_double",
          "array(concat('s', id % 100), concat('s', (id + 1) % 100)) as arr_str",
          // Second array for zip_with
          "array(cast((id + 10) % 100 as int), cast((id + 11) % 100 as int), cast((id + 12) % 100 as int), cast((id + 13) % 100 as int)) as arr_int2",
          // Maps for map functions
          "map('k1', id % 100, 'k2', (id + 1) % 100, 'k3', (id + 2) % 100) as map_str_int",
          "map(cast(id % 10 as int), cast(id % 100 as double), cast((id + 1) % 10 as int), cast((id + 1) % 100 as double)) as map_int_double",
          // Second map for map_zip_with
          "map('k1', (id + 50) % 100, 'k2', (id + 51) % 100) as map_str_int2"
        )
        .write.parquet(dataPath)
    }
  }

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // ========================================
    // transform() - Apply function to each element
    // ========================================
    BenchmarkDef("transform(arr, x -> x * 2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform(arr_int, x -> x * 2) as doubled")
        .selectExpr("sum(doubled[0])")
    ).withSetup(ensureDataExists),

    BenchmarkDef("transform(arr, x -> x + 10)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform(arr_double, x -> x + 10.0) as added")
        .selectExpr("sum(added[0])")
    ),

    BenchmarkDef("transform(arr, (x, i) -> x + i)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform(arr_int, (x, i) -> x + i) as indexed")
        .selectExpr("sum(indexed[0])", "sum(indexed[1])")
    ),

    // ========================================
    // filter() - Filter array elements
    // ========================================
    BenchmarkDef("filter(arr, x -> x > 50)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("filter(arr_int, x -> x > 50) as filtered")
        .selectExpr("sum(size(filtered))")
    ),

    BenchmarkDef("filter(arr, x -> x < 25.0)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("filter(arr_double, x -> x < 25.0) as filtered")
        .selectExpr("count(*) filter (where size(filtered) > 0)")
    ),

    // ========================================
    // exists() - Check if any element matches
    // ========================================
    BenchmarkDef("exists(arr, x -> x > 90)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("exists(arr_int, x -> x > 90) as has_large")
        .selectExpr("count(*) filter (where has_large)")
    ),

    BenchmarkDef("exists(arr, x -> x = 0)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("exists(arr_int, x -> x = 0) as has_zero")
        .selectExpr("count(*) filter (where has_zero)")
    ),

    // ========================================
    // forall() - Check if all elements match
    // ========================================
    BenchmarkDef("forall(arr, x -> x >= 0)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("forall(arr_int, x -> x >= 0) as all_positive")
        .selectExpr("count(*) filter (where all_positive)")
    ),

    BenchmarkDef("forall(arr, x -> x < 100)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("forall(arr_int, x -> x < 100) as all_small")
        .selectExpr("count(*) filter (where all_small)")
    ),

    // ========================================
    // aggregate() / reduce() - Fold array
    // ========================================
    BenchmarkDef("aggregate(arr, 0, (acc, x) -> acc + x)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("aggregate(arr_int, 0, (acc, x) -> acc + x) as total")
        .selectExpr("sum(total)")
    ),

    BenchmarkDef("aggregate with finish", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("aggregate(arr_int, 0, (acc, x) -> acc + x, acc -> acc * 2) as doubled_total")
        .selectExpr("sum(doubled_total)")
    ),

    BenchmarkDef("reduce(arr, (acc, x) -> acc + x)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("reduce(arr_int, 0, (acc, x) -> acc + x) as total")
        .selectExpr("sum(total)")
    ),

    // ========================================
    // array_sort() - Sort with custom comparator
    // ========================================
    BenchmarkDef("array_sort(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_sort(arr_int) as sorted")
        .selectExpr("sum(sorted[0])")
    ),

    BenchmarkDef("array_sort(arr, (a, b) -> b - a)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_sort(arr_int, (a, b) -> cast(b - a as int)) as sorted_desc")
        .selectExpr("sum(sorted_desc[0])")
    ),

    // ========================================
    // zip_with() - Combine two arrays
    // ========================================
    BenchmarkDef("zip_with(a, b, (x, y) -> x + y)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("zip_with(arr_int, arr_int2, (x, y) -> x + y) as combined")
        .selectExpr("sum(combined[0])")
    ),

    BenchmarkDef("zip_with(a, b, (x, y) -> x * y)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("zip_with(arr_int, arr_int2, (x, y) -> x * y) as product")
        .selectExpr("sum(product[0])")
    ),

    // ========================================
    // transform_keys() - Transform map keys
    // ========================================
    BenchmarkDef("transform_keys(map, (k, v) -> upper(k))", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform_keys(map_str_int, (k, v) -> upper(k)) as upper_keys")
        .selectExpr("sum(upper_keys['K1'])")
    ),

    BenchmarkDef("transform_keys(map, (k, v) -> concat(k, '_new'))", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform_keys(map_str_int, (k, v) -> concat(k, '_new')) as new_keys")
        .selectExpr("count(new_keys['k1_new'])")
    ),

    // ========================================
    // transform_values() - Transform map values
    // ========================================
    BenchmarkDef("transform_values(map, (k, v) -> v * 2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform_values(map_str_int, (k, v) -> v * 2) as doubled")
        .selectExpr("sum(doubled['k1'])")
    ),

    BenchmarkDef("transform_values(map, (k, v) -> v + 100)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform_values(map_int_double, (k, v) -> v + 100) as added")
        .selectExpr("sum(element_at(added, 0))")
    ),

    // ========================================
    // map_filter() - Filter map entries
    // ========================================
    BenchmarkDef("map_filter(map, (k, v) -> v > 50)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_filter(map_str_int, (k, v) -> v > 50) as filtered")
        .selectExpr("sum(size(filtered))")
    ),

    BenchmarkDef("map_filter(map, (k, v) -> k = 'k1')", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_filter(map_str_int, (k, v) -> k = 'k1') as filtered")
        .selectExpr("sum(filtered['k1'])")
    ),

    // ========================================
    // map_zip_with() - Combine two maps
    // ========================================
    BenchmarkDef("map_zip_with(m1, m2, (k, v1, v2) -> v1 + v2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_zip_with(map_str_int, map_str_int2, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)) as combined")
        .selectExpr("sum(combined['k1'])")
    ),

    BenchmarkDef("map_zip_with(m1, m2, (k, v1, v2) -> v1 * v2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("map_zip_with(map_str_int, map_str_int2, (k, v1, v2) -> coalesce(v1, 1) * coalesce(v2, 1)) as product")
        .selectExpr("sum(product['k1'])")
    ),

    // ========================================
    // Combined operations
    // ========================================
    BenchmarkDef("transform + filter", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("filter(transform(arr_int, x -> x * 2), x -> x > 100) as result")
        .selectExpr("sum(size(result))")
    ),

    BenchmarkDef("filter + aggregate", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("aggregate(filter(arr_int, x -> x > 50), 0, (acc, x) -> acc + x) as total")
        .selectExpr("sum(total)")
    ),

    BenchmarkDef("transform_keys + transform_values", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("transform_values(transform_keys(map_str_int, (k, v) -> upper(k)), (k, v) -> v * 2) as result")
        .selectExpr("sum(result['K1'])")
    )
  )
}
