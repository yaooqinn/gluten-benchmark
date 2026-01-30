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

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.internal.SQLConf

/**
 * Base trait for Gluten micro-benchmarks.
 *
 * Provides automatic comparison between Vanilla Spark and Gluten+Velox.
 * Define benchmarks by implementing the `benchmarks` method.
 */
trait GlutenBenchmarkBase extends BenchmarkBase {

  /** Default cardinality for benchmarks */
  def defaultCardinality: Long = 10000000L

  /** Number of warmup iterations */
  protected def numWarmupIters: Int = 2

  /** Number of measurement iterations */
  protected def numIters: Int = 5

  /** Define your benchmarks here */
  def benchmarks: Seq[BenchmarkDef]

  /** Check if Gluten is available on classpath */
  protected lazy val glutenAvailable: Boolean = {
    try {
      Class.forName("org.apache.gluten.GlutenPlugin")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  // ============================================================
  // Engine labels
  // ============================================================

  private val VANILLA_SPARK = "Vanilla Spark"
  private val GLUTEN_VELOX = "Gluten + Velox"

  // ============================================================
  // Main benchmark runner
  // ============================================================

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Check Gluten availability first
    if (!glutenAvailable) {
      System.err.println(
        """
          |================================================================================
          |ERROR: Gluten is not available on the classpath!
          |================================================================================
          |
          |The Gluten JAR is required to run performance comparisons.
          |
          |To download the nightly build, run:
          |  ./scripts/download-gluten-nightly.sh
          |
          |Or specify a custom JAR path:
          |  ./build/sbt -Dgluten.jar=/path/to/gluten-velox-bundle.jar "runMain ..."
          |
          |After downloading, the JAR will be auto-detected from lib/ directory.
          |================================================================================
        """.stripMargin)
      throw new RuntimeException("Gluten JAR not found. Run: ./scripts/download-gluten-nightly.sh")
    }

    // Optional filter from command line
    val filter = mainArgs.headOption

    val toRun = filter match {
      case Some(pattern) => benchmarks.filter(_.name.toLowerCase.contains(pattern.toLowerCase))
      case None => benchmarks
    }

    if (toRun.isEmpty) {
      println(s"No benchmarks matched filter: ${filter.getOrElse("(none)")}")
      println(s"Available benchmarks: ${benchmarks.map(_.name).mkString(", ")}")
      return
    }

    toRun.foreach(runSingleBenchmark)
  }

  private def runSingleBenchmark(benchDef: BenchmarkDef): Unit = {
    runBenchmark(benchDef.name) {
      val benchmark = new Benchmark(
        benchDef.name,
        benchDef.cardinality,
        numIters = numIters,
        warmupIters = numWarmupIters,
        output = output
      )

      // Run on Vanilla Spark (session created once, only query is timed)
      benchmark.addManagedCase(VANILLA_SPARK) {
        () => createSessionWithSetup(glutenEnabled = false, benchDef)
      } { spark =>
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      } { spark =>
        benchDef.workload(spark).noop()
      }

      // Run on Gluten + Velox (session created once, only query is timed)
      benchmark.addManagedCase(GLUTEN_VELOX) {
        () => createSessionWithSetup(glutenEnabled = true, benchDef)
      } { spark =>
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      } { spark =>
        benchDef.workload(spark).noop()
      }

      benchmark.runManaged()
    }
  }

  /** Create session and run setup (tables, etc.) - this is NOT timed */
  private def createSessionWithSetup(glutenEnabled: Boolean, benchDef: BenchmarkDef): SparkSession = {
    val spark = createSparkSession(glutenEnabled)
    benchDef.setup.foreach(_(spark))
    spark
  }

  // ============================================================
  // SparkSession management
  // ============================================================

  private def createSparkSession(glutenEnabled: Boolean): SparkSession = {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName} - ${if (glutenEnabled) "Gluten" else "Vanilla"}")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "8g")

    if (glutenEnabled) {
      builder
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.gluten.enabled", "true")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "6g")
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
