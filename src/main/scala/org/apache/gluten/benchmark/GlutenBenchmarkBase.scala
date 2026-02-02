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

  /** Number of warmup iterations - increased to amortize Velox JIT compilation */
  protected def numWarmupIters: Int = 5

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
    // Check if running in Vanilla-only mode (Gluten not available or explicitly disabled)
    val vanillaOnly = !glutenAvailable || sys.props.getOrElse("benchmark.vanilla.only", "false").toBoolean
    
    if (!glutenAvailable) {
      System.err.println(
        """
          |================================================================================
          |WARNING: Gluten is not available on the classpath!
          |================================================================================
          |
          |Running in Vanilla-only mode. To enable Gluten comparison:
          |  ./scripts/download-gluten-nightly.sh
          |
          |Or specify a custom JAR path:
          |  ./build/sbt -Dgluten.jar=/path/to/gluten-velox-bundle.jar "runMain ..."
          |================================================================================
        """.stripMargin)
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

    // Run all benchmarks with shared sessions
    runAllBenchmarksWithSharedSessions(toRun, vanillaOnly)
  }

  /**
   * Run all benchmarks with shared SparkSessions.
   * Creates one Vanilla session for all Vanilla runs, one Gluten session for all Gluten runs.
   */
  private def runAllBenchmarksWithSharedSessions(
      benchDefs: Seq[BenchmarkDef],
      vanillaOnly: Boolean = false): Unit = {
    // Phase 1: Run all benchmarks on Vanilla Spark
    println(s"\n${"=" * 80}")
    println("Running benchmarks with Vanilla Spark")
    println("=" * 80)
    
    val vanillaSpark = createSparkSession(glutenEnabled = false)
    val vanillaResults = try {
      benchDefs.map { benchDef =>
        runBenchmarkWithSession(benchDef, vanillaSpark, VANILLA_SPARK)
      }
    } finally {
      vanillaSpark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

    // Phase 2: Run all benchmarks on Gluten + Velox (if available)
    val glutenResults: Seq[BenchmarkResult] = if (vanillaOnly) {
      println(s"\n${"=" * 80}")
      println("Skipping Gluten benchmarks (Vanilla-only mode)")
      println("=" * 80)
      Seq.empty[BenchmarkResult]
    } else {
      println(s"\n${"=" * 80}")
      println("Running benchmarks with Gluten + Velox")
      println("=" * 80)
      val glutenSpark = createSparkSession(glutenEnabled = true)
      try {
        benchDefs.map { benchDef =>
          runBenchmarkWithSession(benchDef, glutenSpark, GLUTEN_VELOX)
        }
      } finally {
        glutenSpark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }

    // Phase 3: Print combined results
    printCombinedResults(benchDefs, vanillaResults, glutenResults, vanillaOnly)
  }

  /** Run a single benchmark with an existing session */
  private def runBenchmarkWithSession(
      benchDef: BenchmarkDef,
      spark: SparkSession,
      engineName: String): BenchmarkResult = {
    
    // Run setup (not timed)
    benchDef.setup.foreach(_(spark))
    
    // Profile the first run in detail
    val df = benchDef.workload(spark)
    
    // Measure query planning time
    val planStart = System.nanoTime()
    df.queryExecution.executedPlan // Force plan generation
    val planEnd = System.nanoTime()
    val planTimeMs = (planEnd - planStart) / 1e6
    
    // Execute once to get timing breakdown
    val execStart = System.nanoTime()
    df.noop()
    val execEnd = System.nanoTime()
    val firstExecMs = (execEnd - execStart) / 1e6
    
    // Remaining warmup iterations
    (2 to numWarmupIters).foreach { _ =>
      benchDef.workload(spark).noop()
    }
    
    // Measurement iterations
    val times = (1 to numIters).map { _ =>
      val start = System.nanoTime()
      benchDef.workload(spark).noop()
      val end = System.nanoTime()
      (end - start) / 1e6
    }
    
    val best = times.min
    val avg = times.sum / times.length
    val stddev = math.sqrt(times.map(t => math.pow(t - avg, 2)).sum / times.length)
    
    // Print profiling info
    println(f"  ${benchDef.name}%-45s $best%10.0f ms (best)  $avg%10.0f ms (avg)  [plan: $planTimeMs%.0fms, first: $firstExecMs%.0fms]")
    
    BenchmarkResult(benchDef.name, best, avg, stddev, 1.0)
  }

  /** Print combined results comparing Vanilla vs Gluten */
  private def printCombinedResults(
      benchDefs: Seq[BenchmarkDef],
      vanillaResults: Seq[BenchmarkResult],
      glutenResults: Seq[BenchmarkResult],
      vanillaOnly: Boolean = false): Unit = {
    
    val out = output.map(new java.io.PrintStream(_)).getOrElse(System.out)
    
    if (vanillaOnly) {
      // Vanilla-only mode: just print vanilla results
      benchDefs.zip(vanillaResults).foreach {
        case (benchDef, vanilla) =>
          out.println()
          out.println(s"${benchDef.name}:")
          out.println("-" * 80)
          out.printf("%-40s %12s %12s %12s %10s\n",
            "", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Relative")
          out.println("-" * 80)
          out.println(f"$VANILLA_SPARK%-40s ${vanilla.bestTimeMs}%12.0f ${vanilla.avgTimeMs}%12.0f ${vanilla.stddevMs}%12.1f ${1.0}%10.1fX")
          out.println("-" * 80)
      }
    } else {
      benchDefs.zip(vanillaResults.zip(glutenResults)).foreach {
        case (benchDef, (vanilla, gluten)) =>
          out.println()
          out.println(s"${benchDef.name}:")
          out.println("-" * 80)
          out.printf("%-40s %12s %12s %12s %10s\n",
            "", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Relative")
          out.println("-" * 80)
          
          val relative = vanilla.avgTimeMs / gluten.avgTimeMs
          
          out.println(f"$VANILLA_SPARK%-40s ${vanilla.bestTimeMs}%12.0f ${vanilla.avgTimeMs}%12.0f ${vanilla.stddevMs}%12.1f ${1.0}%10.1fX")
          out.println(f"$GLUTEN_VELOX%-40s ${gluten.bestTimeMs}%12.0f ${gluten.avgTimeMs}%12.0f ${gluten.stddevMs}%12.1f $relative%10.1fX")
          out.println("-" * 80)
      }
    }
    out.println()
  }

  // ============================================================
  // SparkSession management
  // ============================================================

  private def createSparkSession(glutenEnabled: Boolean): SparkSession = {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName} - ${if (glutenEnabled) "Gluten" else "Vanilla"}")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "8")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "8g")

    if (glutenEnabled) {
      builder
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.gluten.enabled", "true")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
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
