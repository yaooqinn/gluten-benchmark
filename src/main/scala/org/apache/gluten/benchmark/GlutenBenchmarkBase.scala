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

package org.apache.gluten.benchmark

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.internal.SQLConf

/**
 * Base trait for Gluten micro-benchmarks.
 *
 * Provides automatic comparison between Vanilla Spark and Gluten+Velox.
 * Define benchmarks using the simple DSL:
 *
 * {{{
 * object MyBenchmark extends GlutenBenchmarkBase {
 *   val N = 10_000_000L
 *
 *   override def benchmarks = Seq(
 *     "SUM(id)" -> { _.range(N).selectExpr("sum(id)") },
 *     "COUNT DISTINCT" -> { _.range(N).selectExpr("count(distinct id % 1000)") }
 *   )
 * }
 * }}}
 */
trait GlutenBenchmarkBase extends BenchmarkBase {

  /** Default cardinality for benchmarks */
  protected def defaultCardinality: Long = 10_000_000L

  /** Number of warmup iterations */
  protected def numWarmupIters: Int = 2

  /** Number of measurement iterations */
  protected def numIters: Int = 5

  /** Define your benchmarks here */
  def benchmarks: Seq[BenchmarkDef]

  // ============================================================
  // Implicit conversions for clean DSL syntax
  // ============================================================

  /** Convert (String, SparkSession => DataFrame) to BenchmarkDef */
  implicit def tupleToBenchmarkDef(
      t: (String, SparkSession => DataFrame)
  ): BenchmarkDef =
    BenchmarkDef(t._1, defaultCardinality, t._2)

  /** Convert (String, String) to BenchmarkDef (SQL query) */
  implicit def sqlTupleToBenchmarkDef(t: (String, String)): BenchmarkDef =
    BenchmarkDef(t._1, defaultCardinality, spark => spark.sql(t._2))

  // ============================================================
  // Engine definitions
  // ============================================================

  private val VANILLA_SPARK = "Vanilla Spark"
  private val GLUTEN_VELOX = "Gluten + Velox"

  // ============================================================
  // Main benchmark runner
  // ============================================================

  final override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Optional filter from command line
    val filter = mainArgs.headOption

    val toRun = filter match {
      case Some(pattern) => benchmarks.filter(_.name.toLowerCase.contains(pattern.toLowerCase))
      case None          => benchmarks
    }

    if (toRun.isEmpty) {
      // scalastyle:off println
      println(s"No benchmarks matched filter: ${filter.getOrElse("(none)")}")
      println(s"Available benchmarks: ${benchmarks.map(_.name).mkString(", ")}")
      // scalastyle:on println
      return
    }

    toRun.foreach(runSingleBenchmark)
  }

  private def runSingleBenchmark(benchDef: BenchmarkDef): Unit = {
    runBenchmark(benchDef.name) {
      val benchmark = new Benchmark(
        benchDef.name,
        benchDef.cardinality,
        minNumIters = numWarmupIters,
        output = output
      )

      // Phase 1: Run all iterations on Vanilla Spark
      withSparkSession(glutenEnabled = false) { spark =>
        benchDef.setup.foreach(_(spark))

        benchmark.addCase(VANILLA_SPARK, numIters) { _ =>
          benchDef.workload(spark).noop()
        }
      }

      // Phase 2: Run all iterations on Gluten + Velox
      withSparkSession(glutenEnabled = true) { spark =>
        benchDef.setup.foreach(_(spark))

        benchmark.addCase(GLUTEN_VELOX, numIters) { _ =>
          benchDef.workload(spark).noop()
        }
      }

      benchmark.run()
    }
  }

  // ============================================================
  // SparkSession management
  // ============================================================

  private def withSparkSession[T](glutenEnabled: Boolean)(f: SparkSession => T): T = {
    val spark = createSparkSession(glutenEnabled)
    try {
      f(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def createSparkSession(glutenEnabled: Boolean): SparkSession = {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName} - ${if (glutenEnabled) "Gluten" else "Vanilla"}")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "4g")

    if (glutenEnabled) {
      builder
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.gluten.enabled", "true")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "4g")
    }

    builder.getOrCreate()
  }

  // ============================================================
  // DataFrame helpers
  // ============================================================

  implicit class DataFrameOps(df: DataFrame) {
    /** Write to noop sink for benchmarking */
    def noop(): Unit = {
      df.write.format("noop").mode(SaveMode.Overwrite).save()
    }
  }
}
