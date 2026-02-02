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
import org.apache.spark.sql.functions._

import java.io.File

/**
 * Benchmark for string functions using Parquet input.
 *
 * To run:
 *   ./build/sbt "runMain org.apache.gluten.benchmark.functions.StringFunctionsBenchmark"
 */
object StringFunctionsBenchmark extends GlutenBenchmarkBase {

  // 50M rows - balance between amortizing overhead and reasonable runtime
  override def defaultCardinality: Long = 50000000L

  private val N = defaultCardinality
  private val dataPath = s"/tmp/gluten-benchmark-strings-$N"

  /** Generate test Parquet data with string columns */
  private def ensureDataExists(spark: SparkSession): Unit = {
    val dataDir = new File(dataPath)
    if (!dataDir.exists()) {
      // scalastyle:off println
      println(s"  Generating string test data at $dataPath ...")
      // scalastyle:on println

      spark.range(N)
        .selectExpr(
          "id",
          "cast(id as string) as id_str",
          "concat('prefix_', id, '_suffix') as mixed_str",
          "concat('  ', id, '  ') as padded_str",
          "upper(concat('abc', id)) as upper_str",
          "concat(id, ',', id + 1, ',', id + 2) as csv_str"
        )
        .write
        .mode(SaveMode.Overwrite)
        .parquet(dataPath)

      // scalastyle:off println
      println(s"  String test data generated.")
      // scalastyle:on println
    }
  }

  override def benchmarks: Seq[BenchmarkDef] = Seq(
    // Basic string functions
    BenchmarkDef("length()", N,
      spark => spark.read.parquet(dataPath).select(length(col("id_str")))
    ).withSetup(ensureDataExists),

    BenchmarkDef("substring()", N,
      spark => spark.read.parquet(dataPath).select(substring(col("id_str"), 1, 3))
    ),

    BenchmarkDef("concat()", N,
      spark => spark.read.parquet(dataPath).select(concat(col("id_str"), lit("-extra")))
    ),

    BenchmarkDef("upper()", N,
      spark => spark.read.parquet(dataPath).select(upper(col("id_str")))
    ),

    BenchmarkDef("lower()", N,
      spark => spark.read.parquet(dataPath).select(lower(col("upper_str")))
    ),

    BenchmarkDef("trim()", N,
      spark => spark.read.parquet(dataPath).select(trim(col("padded_str")))
    ),

    // Pattern matching
    BenchmarkDef("like()", N,
      spark => spark.read.parquet(dataPath).filter(col("id_str").like("%1%"))
    ),

    BenchmarkDef("regexp_replace()", N,
      spark => spark.read.parquet(dataPath).select(regexp_replace(col("id_str"), "1", "X"))
    ),

    BenchmarkDef("regexp_extract()", N,
      spark => spark.read.parquet(dataPath).select(regexp_extract(col("id_str"), "(\\d)", 1))
    ),

    // String manipulation
    BenchmarkDef("split()", N,
      spark => spark.read.parquet(dataPath).select(split(col("csv_str"), ","))
    ),

    BenchmarkDef("reverse()", N,
      spark => spark.read.parquet(dataPath).select(reverse(col("id_str")))
    ),

    BenchmarkDef("repeat()", N,
      spark => spark.read.parquet(dataPath).select(repeat(col("id_str"), 3))
    ),

    // Comparison
    BenchmarkDef("contains()", N,
      spark => spark.read.parquet(dataPath).filter(col("id_str").contains("42"))
    ),

    BenchmarkDef("startsWith()", N,
      spark => spark.read.parquet(dataPath).filter(col("id_str").startsWith("1"))
    )
  )
}
