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
 * Benchmark for array functions:
 * array, array_append, array_compact, array_contains, array_distinct,
 * array_except, array_insert, array_intersect, array_join, array_max,
 * array_min, array_position, array_prepend, array_remove, array_repeat,
 * array_size, array_union, arrays_overlap, array_zip, cardinality,
 * concat, element_at, try_element_at, flatten, get, reverse, sequence,
 * shuffle, size, slice, sort_array
 */
object ArrayFunctionsBenchmark extends GlutenBenchmarkBase {

  override def defaultCardinality: Long = 20000000L // 20M rows

  private val N = defaultCardinality
  private val dataPath = s"/tmp/gluten-benchmark-arrays-$N"

  private def ensureDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(dataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating array functions test data at $dataPath ...")
      // scalastyle:on println

      spark.range(N)
        .selectExpr(
          "id",
          // Integer arrays
          "array(cast(id % 100 as int), cast((id + 1) % 100 as int), cast((id + 2) % 100 as int)) as arr_int",
          "array(cast((id + 10) % 100 as int), cast((id + 11) % 100 as int)) as arr_int2",
          // Arrays with nulls for array_compact
          "array(cast(id % 50 as int), null, cast((id + 1) % 50 as int), null) as arr_with_nulls",
          // String arrays
          "array(concat('s', id % 100), concat('s', (id + 1) % 100)) as arr_str",
          "array(concat('s', (id + 50) % 100), concat('s', (id + 51) % 100)) as arr_str2",
          // Double arrays
          "array(cast(id % 100 as double), cast((id + 1) % 100 as double)) as arr_double",
          // Nested arrays for flatten
          "array(array(cast(id % 10 as int), cast((id + 1) % 10 as int)), array(cast((id + 2) % 10 as int))) as nested_arr",
          // Scalar values for various operations
          "cast(id % 100 as int) as val_int",
          "concat('s', id % 100) as val_str",
          // For sequence
          "cast(id % 10 as int) as seq_start",
          "cast(10 + id % 10 as int) as seq_end"
        )
        .write.parquet(dataPath)
    }
  }

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // ========================================
    // array() - Create array
    // ========================================
    BenchmarkDef("array(int, int, int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array(val_int, val_int + 1, val_int + 2) as arr")
        .selectExpr("sum(arr[0])")
    ).withSetup(ensureDataExists),

    // ========================================
    // array_append() - Append element
    // ========================================
    BenchmarkDef("array_append(arr, int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_append(arr_int, 999) as arr")
        .selectExpr("sum(arr[3])")
    ),

    BenchmarkDef("array_append(arr, str)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_append(arr_str, 'new') as arr")
        .selectExpr("count(arr[2])")
    ),

    // ========================================
    // array_compact() - Remove nulls
    // ========================================
    BenchmarkDef("array_compact(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_compact(arr_with_nulls) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // array_contains() - Check element existence
    // ========================================
    BenchmarkDef("array_contains(arr, int)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_contains(arr_int, 50) as contains")
        .selectExpr("count(*) filter (where contains)")
    ),

    BenchmarkDef("array_contains(arr, str)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_contains(arr_str, 's50') as contains")
        .selectExpr("count(*) filter (where contains)")
    ),

    // ========================================
    // array_distinct() - Remove duplicates
    // ========================================
    BenchmarkDef("array_distinct(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_distinct(array(val_int, val_int, val_int + 1)) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // array_except() - Set difference
    // ========================================
    BenchmarkDef("array_except(arr1, arr2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_except(arr_int, arr_int2) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // array_insert() - Insert at position
    // ========================================
    BenchmarkDef("array_insert(arr, pos, val)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_insert(arr_int, 2, 999) as arr")
        .selectExpr("sum(arr[1])")
    ),

    // ========================================
    // array_intersect() - Set intersection
    // ========================================
    BenchmarkDef("array_intersect(arr1, arr2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_intersect(arr_int, arr_int2) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // array_join() - Join elements to string
    // ========================================
    BenchmarkDef("array_join(arr, delim)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_join(arr_str, ',') as joined")
        .selectExpr("count(joined)")
    ),

    BenchmarkDef("array_join(arr, delim, null)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_join(arr_with_nulls, ',', 'NULL') as joined")
        .selectExpr("count(joined)")
    ),

    // ========================================
    // array_max() / array_min() - Extremes
    // ========================================
    BenchmarkDef("array_max(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_max(arr_int) as mx")
        .selectExpr("sum(mx)")
    ),

    BenchmarkDef("array_min(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_min(arr_int) as mn")
        .selectExpr("sum(mn)")
    ),

    // ========================================
    // array_position() - Find element position
    // ========================================
    BenchmarkDef("array_position(arr, val)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_position(arr_int, 50) as pos")
        .selectExpr("sum(pos)")
    ),

    // ========================================
    // array_prepend() - Prepend element
    // ========================================
    BenchmarkDef("array_prepend(arr, val)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_prepend(arr_int, 999) as arr")
        .selectExpr("sum(arr[0])")
    ),

    // ========================================
    // array_remove() - Remove element
    // ========================================
    BenchmarkDef("array_remove(arr, val)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_remove(arr_int, arr_int[0]) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // array_repeat() - Repeat element
    // ========================================
    BenchmarkDef("array_repeat(val, count)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_repeat(val_int, 3) as arr")
        .selectExpr("sum(arr[0])")
    ),

    // ========================================
    // array_size() / size() / cardinality()
    // ========================================
    BenchmarkDef("array_size(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_size(arr_int) as sz")
        .selectExpr("sum(sz)")
    ),

    BenchmarkDef("size(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("size(arr_int) as sz")
        .selectExpr("sum(sz)")
    ),

    BenchmarkDef("cardinality(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("cardinality(arr_int) as sz")
        .selectExpr("sum(sz)")
    ),

    // ========================================
    // array_union() - Set union
    // ========================================
    BenchmarkDef("array_union(arr1, arr2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_union(arr_int, arr_int2) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // arrays_overlap() - Check overlap
    // ========================================
    BenchmarkDef("arrays_overlap(arr1, arr2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("arrays_overlap(arr_int, arr_int2) as overlaps")
        .selectExpr("count(*) filter (where overlaps)")
    ),

    // ========================================
    // arrays_zip() - Zip arrays (note: function is arrays_zip, not array_zip)
    // ========================================
    BenchmarkDef("arrays_zip(arr1, arr2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("arrays_zip(arr_int, arr_int2) as zipped")
        .selectExpr("sum(size(zipped))")
    ),

    // ========================================
    // concat() - Concatenate arrays
    // ========================================
    BenchmarkDef("concat(arr1, arr2)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("concat(arr_int, arr_int2) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // element_at() / try_element_at() / get()
    // ========================================
    BenchmarkDef("element_at(arr, idx)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("element_at(arr_int, 2) as elem")
        .selectExpr("sum(elem)")
    ),

    BenchmarkDef("try_element_at(arr, idx)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("try_element_at(arr_int, 10) as elem")
        .selectExpr("count(elem)")
    ),

    BenchmarkDef("get(arr, idx)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("get(arr_int, 1) as elem")
        .selectExpr("sum(elem)")
    ),

    // ========================================
    // flatten() - Flatten nested arrays
    // ========================================
    BenchmarkDef("flatten(nested_arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("flatten(nested_arr) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // reverse() - Reverse array
    // ========================================
    BenchmarkDef("reverse(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("reverse(arr_int) as arr")
        .selectExpr("sum(arr[0])")
    ),

    // ========================================
    // sequence() - Generate sequence
    // ========================================
    BenchmarkDef("sequence(start, end)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("sequence(seq_start, seq_end) as arr")
        .selectExpr("sum(size(arr))")
    ),

    BenchmarkDef("sequence(start, end, step)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("sequence(seq_start, seq_end, 2) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // shuffle() - Random shuffle
    // ========================================
    BenchmarkDef("shuffle(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("shuffle(arr_int) as arr")
        .selectExpr("sum(arr[0])")
    ),

    // ========================================
    // slice() - Extract slice
    // ========================================
    BenchmarkDef("slice(arr, start, len)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("slice(arr_int, 1, 2) as arr")
        .selectExpr("sum(size(arr))")
    ),

    // ========================================
    // sort_array() - Sort array
    // ========================================
    BenchmarkDef("sort_array(arr)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("sort_array(arr_int) as arr")
        .selectExpr("sum(arr[0])")
    ),

    BenchmarkDef("sort_array(arr, desc)", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("sort_array(arr_int, false) as arr")
        .selectExpr("sum(arr[0])")
    ),

    // ========================================
    // Combined operations
    // ========================================
    BenchmarkDef("array_distinct + size", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("size(array_distinct(concat(arr_int, arr_int2))) as distinct_size")
        .selectExpr("sum(distinct_size)")
    ),

    BenchmarkDef("flatten + array_max", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("array_max(flatten(nested_arr)) as mx")
        .selectExpr("sum(mx)")
    ),

    BenchmarkDef("slice + reverse", N, spark =>
      spark.read.parquet(dataPath)
        .selectExpr("reverse(slice(arr_int, 1, 2)) as arr")
        .selectExpr("sum(arr[0])")
    )
  )
}
