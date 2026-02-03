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

import org.apache.spark.sql.SparkSession

/**
 * Optimized runner that keeps SparkSessions warm across all benchmark suites.
 * 
 * This avoids the overhead of creating/destroying SparkContext for each suite,
 * which can save 30-60 seconds per suite.
 *
 * Usage:
 *   sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarksFast"
 *   sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarksFast aggregate"
 */
object RunAllBenchmarksFast {

  // Registry of all benchmark objects
  private val allBenchmarks: Seq[GlutenBenchmarkBase] = Seq(
    aggregate.AggregateBenchmark,
    functions.StringFunctionsBenchmark,
    functions.MapFunctionsBenchmark,
    functions.ExplodeBenchmark,
    functions.HigherOrderFunctionsBenchmark,
    functions.ArrayFunctionsBenchmark
  )

  def main(args: Array[String]): Unit = {
    val filter = args.headOption

    val toRun = filter match {
      case Some(pattern) =>
        allBenchmarks.filter(_.getClass.getSimpleName.toLowerCase.contains(pattern.toLowerCase))
      case None =>
        allBenchmarks
    }

    if (toRun.isEmpty) {
      // scalastyle:off println
      println(s"No benchmarks matched filter: ${filter.getOrElse("(none)")}")
      println(s"Available: ${allBenchmarks.map(_.getClass.getSimpleName).mkString(", ")}")
      // scalastyle:on println
      return
    }

    // scalastyle:off println
    println(s"Running ${toRun.size} benchmark suite(s) with shared sessions...")
    println("=" * 80)
    // scalastyle:on println

    val startTime = System.currentTimeMillis()

    // Check Gluten availability
    val glutenAvailable = try {
      Class.forName("org.apache.gluten.GlutenPlugin")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

    // Phase 1: Run all suites with Vanilla Spark (single session)
    println("\n" + "=" * 80)
    println("Phase 1: Running all benchmarks with Vanilla Spark")
    println("=" * 80)
    
    val vanillaSpark = createVanillaSession()
    val vanillaResults = try {
      toRun.flatMap { benchmark =>
        println(s"\n>>> ${benchmark.getClass.getSimpleName}")
        runBenchmarksWithSession(benchmark, vanillaSpark, isGluten = false)
      }
    } finally {
      vanillaSpark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

    // Phase 2: Run all suites with Gluten (single session)
    val glutenResults = if (glutenAvailable) {
      println("\n" + "=" * 80)
      println("Phase 2: Running all benchmarks with Gluten + Velox")
      println("=" * 80)
      
      val glutenSpark = createGlutenSession()
      try {
        toRun.flatMap { benchmark =>
          println(s"\n>>> ${benchmark.getClass.getSimpleName}")
          runBenchmarksWithSession(benchmark, glutenSpark, isGluten = true)
        }
      } finally {
        glutenSpark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    } else {
      println("\n[WARN] Gluten not available - skipping Gluten benchmarks")
      Seq.empty
    }

    // Print summary
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    println("\n" + "=" * 80)
    println(f"All ${toRun.size} suites completed in $totalTime%.1f seconds")
    println(s"Total benchmarks: ${vanillaResults.size} Vanilla, ${glutenResults.size} Gluten")
    println("=" * 80)
  }

  private def createVanillaSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Gluten Benchmark - Vanilla")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "8g")
      .getOrCreate()
  }

  private def createGlutenSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Gluten Benchmark - Gluten")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "8g")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config("spark.gluten.enabled", "true")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "6g")
      .config("spark.gluten.ui.enabled", "true")
      .getOrCreate()
  }

  private def runBenchmarksWithSession(
      benchmark: GlutenBenchmarkBase,
      spark: SparkSession,
      isGluten: Boolean): Seq[(String, Double)] = {
    
    val numWarmupIters = sys.props.getOrElse("benchmark.warmup.iters", "5").toInt
    val numIters = sys.props.getOrElse("benchmark.iters", "5").toInt
    
    benchmark.benchmarks.map { benchDef =>
      // Run setup if defined
      benchDef.setup.foreach(_(spark))
      
      // Warmup
      (1 to numWarmupIters).foreach { _ =>
        benchDef.workload(spark).write.format("noop").mode("overwrite").save()
      }
      
      // Measurement
      val times = (1 to numIters).map { _ =>
        val start = System.nanoTime()
        benchDef.workload(spark).write.format("noop").mode("overwrite").save()
        val end = System.nanoTime()
        (end - start) / 1e6
      }
      
      val best = times.min
      val avg = times.sum / times.length
      val label = if (isGluten) "Gluten" else "Vanilla"
      
      // scalastyle:off println
      println(f"  ${benchDef.name}%-45s $best%8.0f ms (best)  $avg%8.0f ms (avg)  [$label]")
      // scalastyle:on println
      
      (benchDef.name, avg)
    }
  }
}
