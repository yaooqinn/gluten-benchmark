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
import org.apache.spark.sql.functions._

/**
 * Benchmark for string functions.
 *
 * To run:
 *   ./build/sbt "runMain org.apache.gluten.benchmark.functions.StringFunctionsBenchmark"
 */
object StringFunctionsBenchmark extends GlutenBenchmarkBase {

  // Use 50M rows - balance between amortizing overhead and reasonable runtime
  // Target: each benchmark should run for 5-15 seconds
  override def defaultCardinality: Long = 50000000L

  private val N = defaultCardinality

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // Basic string functions
    BenchmarkDef("length()", N, spark => {
      import spark.implicits._
      spark.range(N).select(length($"id".cast("string")))
    }),

    BenchmarkDef("substring()", N, spark => {
      import spark.implicits._
      spark.range(N).select(substring($"id".cast("string"), 1, 3))
    }),

    BenchmarkDef("concat()", N, spark => {
      import spark.implicits._
      spark.range(N).select(concat($"id".cast("string"), lit("-suffix")))
    }),

    BenchmarkDef("upper()", N, spark => {
      import spark.implicits._
      spark.range(N).select(upper($"id".cast("string")))
    }),

    BenchmarkDef("lower()", N, spark => {
      import spark.implicits._
      spark.range(N).select(lower(concat(lit("ABC"), $"id".cast("string"))))
    }),

    BenchmarkDef("trim()", N, spark => {
      import spark.implicits._
      spark.range(N).select(trim(concat(lit("  "), $"id".cast("string"), lit("  "))))
    }),

    // Pattern matching
    BenchmarkDef("like()", N, spark => {
      import spark.implicits._
      spark.range(N)
        .select($"id".cast("string").as("s"))
        .filter($"s".like("%1%"))
    }),

    BenchmarkDef("regexp_replace()", N, spark => {
      import spark.implicits._
      spark.range(N).select(regexp_replace($"id".cast("string"), "1", "X"))
    }),

    BenchmarkDef("regexp_extract()", N, spark => {
      import spark.implicits._
      spark.range(N).select(regexp_extract($"id".cast("string"), "(\\d)", 1))
    }),

    // String manipulation
    BenchmarkDef("split()", N, spark => {
      import spark.implicits._
      spark.range(N)
        .select(concat($"id".cast("string"), lit(","), ($"id" + 1).cast("string")).as("s"))
        .select(split($"s", ","))
    }),

    BenchmarkDef("reverse()", N, spark => {
      import spark.implicits._
      spark.range(N).select(reverse($"id".cast("string")))
    }),

    BenchmarkDef("repeat()", N, spark => {
      import spark.implicits._
      spark.range(N).select(repeat($"id".cast("string"), 3))
    }),

    // Comparison
    BenchmarkDef("contains()", N, spark => {
      import spark.implicits._
      spark.range(N)
        .select($"id".cast("string").as("s"))
        .filter($"s".contains("42"))
    }),

    BenchmarkDef("startsWith()", N, spark => {
      import spark.implicits._
      spark.range(N)
        .select($"id".cast("string").as("s"))
        .filter($"s".startsWith("1"))
    })
  )
}
