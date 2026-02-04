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

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

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

  // ANSI color codes for terminal output
  private object Colors {
    // Check if colors should be disabled
    private val noColor = sys.env.get("NO_COLOR").isDefined || 
      sys.env.getOrElse("TERM", "") == "dumb"
    
    private def color(code: String): String = if (noColor) "" else code
    
    val RESET: String = color("\u001b[0m")
    val BOLD: String = color("\u001b[1m")
    val DIM: String = color("\u001b[2m")
    
    // Bright colors for better visibility on dark terminals
    val CYAN: String = color("\u001b[96m")       // Bright cyan
    val GREEN: String = color("\u001b[92m")      // Bright green
    val YELLOW: String = color("\u001b[93m")     // Bright yellow
    val MAGENTA: String = color("\u001b[95m")    // Bright magenta
    val WHITE: String = color("\u001b[97m")      // Bright white
    val RED: String = color("\u001b[91m")        // Bright red
    val BLUE: String = color("\u001b[94m")       // Bright blue
  }

  import Colors._

  // Registry of all benchmark objects
  private val allBenchmarks: Seq[GlutenBenchmarkBase] = Seq(
    aggregate.AggregateBenchmark,
    functions.StringFunctionsBenchmark,
    functions.MapFunctionsBenchmark,
    functions.ExplodeBenchmark,
    functions.HigherOrderFunctionsBenchmark,
    functions.ArrayFunctionsBenchmark
  )

  // Check if we should save results to files
  private val generateFiles: Boolean = 
    sys.env.getOrElse("SPARK_GENERATE_BENCHMARK_FILES", "0") == "1"
  
  private val benchmarkDate: String = 
    sys.env.getOrElse("SPARK_BENCHMARK_DATE", LocalDate.now().format(DateTimeFormatter.ISO_DATE))

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
      println(s"${YELLOW}No benchmarks matched filter: ${filter.getOrElse("(none)")}${RESET}")
      println(s"Available: ${allBenchmarks.map(_.getClass.getSimpleName).mkString(", ")}")
      // scalastyle:on println
      return
    }

    // scalastyle:off println
    println(s"${BOLD}${CYAN}Running ${toRun.size} benchmark suite(s) with shared sessions...${RESET}")
    if (generateFiles) {
      println(s"${DIM}Results will be saved to benchmarks/$benchmarkDate/${RESET}")
    }
    println(s"${CYAN}${"=" * 80}${RESET}")
    // scalastyle:on println

    val startTime = System.currentTimeMillis()

    // Check Gluten availability
    val glutenAvailable = try {
      Class.forName("org.apache.gluten.GlutenPlugin")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

    // Collect all results
    val allResults = scala.collection.mutable.Map[String, (Seq[BenchmarkTiming], Seq[BenchmarkTiming])]()

    // Phase 1: Run all suites with Vanilla Spark (single session)
    println(s"\n${CYAN}${"=" * 80}${RESET}")
    println(s"${BOLD}${BLUE}Phase 1: Running all benchmarks with Vanilla Spark${RESET}")
    println(s"${CYAN}${"=" * 80}${RESET}")
    
    val vanillaSpark = createVanillaSession()
    val vanillaResults = try {
      toRun.map { benchmark =>
        println(s"\n${BOLD}${WHITE}>>> ${benchmark.getClass.getSimpleName}${RESET}")
        val results = runBenchmarksWithSession(benchmark, vanillaSpark, isGluten = false)
        (benchmark.getClass.getSimpleName.replace("$", ""), results)
      }.toMap
    } finally {
      vanillaSpark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

    // Phase 2: Run all suites with Gluten (single session)
    val glutenResults = if (glutenAvailable) {
      println(s"\n${CYAN}${"=" * 80}${RESET}")
      println(s"${BOLD}${MAGENTA}Phase 2: Running all benchmarks with Gluten + Velox${RESET}")
      println(s"${CYAN}${"=" * 80}${RESET}")
      
      val glutenSpark = createGlutenSession()
      try {
        toRun.map { benchmark =>
          println(s"\n${BOLD}${WHITE}>>> ${benchmark.getClass.getSimpleName}${RESET}")
          val results = runBenchmarksWithSession(benchmark, glutenSpark, isGluten = true)
          (benchmark.getClass.getSimpleName.replace("$", ""), results)
        }.toMap
      } finally {
        glutenSpark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    } else {
      println(s"\n${YELLOW}[WARN] Gluten not available - skipping Gluten benchmarks${RESET}")
      Map.empty[String, Seq[BenchmarkTiming]]
    }

    // Save results to files if enabled
    if (generateFiles) {
      saveResults(vanillaResults, glutenResults)
    }

    // Print summary
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"\n${CYAN}${"=" * 80}${RESET}")
    println(f"${BOLD}${GREEN}All ${toRun.size} suites completed in $totalTime%.1f seconds${RESET}")
    println(s"${WHITE}Total benchmarks: ${vanillaResults.values.map(_.size).sum} Vanilla, ${glutenResults.values.map(_.size).sum} Gluten${RESET}")
    println(s"${CYAN}${"=" * 80}${RESET}")
  }

  case class BenchmarkTiming(name: String, bestMs: Double, avgMs: Double, stddevMs: Double)

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
      isGluten: Boolean): Seq[BenchmarkTiming] = {
    
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
      val stddev = math.sqrt(times.map(t => math.pow(t - avg, 2)).sum / times.length)
      val (labelColor, label) = if (isGluten) (MAGENTA, "Gluten") else (BLUE, "Vanilla")
      
      // scalastyle:off println
      println(f"  ${DIM}${benchDef.name}%-45s${RESET} ${GREEN}$best%8.0f ms${RESET} (best)  $avg%8.0f ms (avg)  ${labelColor}[$label]${RESET}")
      // scalastyle:on println
      
      BenchmarkTiming(benchDef.name, best, avg, stddev)
    }
  }

  private def saveResults(
      vanillaResults: Map[String, Seq[BenchmarkTiming]],
      glutenResults: Map[String, Seq[BenchmarkTiming]]): Unit = {
    
    val outputDir = new File(s"benchmarks/$benchmarkDate")
    outputDir.mkdirs()
    
    // scalastyle:off println
    println(s"\n${DIM}Saving results to ${outputDir.getAbsolutePath}/${RESET}")
    // scalastyle:on println
    
    vanillaResults.foreach { case (suiteName, vanillaTimes) =>
      val glutenTimes = glutenResults.getOrElse(suiteName, Seq.empty)
      val outputFile = new File(outputDir, s"$suiteName-results.txt")
      
      val writer = new PrintWriter(outputFile)
      try {
        writer.println(s"$suiteName Benchmark Results")
        writer.println(s"Date: $benchmarkDate")
        writer.println("=" * 100)
        writer.println()
        
        // Group by benchmark name
        val vanillaMap = vanillaTimes.map(t => t.name -> t).toMap
        val glutenMap = glutenTimes.map(t => t.name -> t).toMap
        
        vanillaTimes.foreach { vanilla =>
          writer.println(s"${vanilla.name}:")
          writer.println("-" * 100)
          writer.printf("%-40s %12s %12s %12s %10s%n",
            "", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Relative")
          writer.println("-" * 100)
          writer.println(f"${"Vanilla Spark"}%-40s ${vanilla.bestMs}%12.0f ${vanilla.avgMs}%12.0f ${vanilla.stddevMs}%12.1f ${1.0}%10.1fX")
          
          glutenMap.get(vanilla.name).foreach { gluten =>
            val relative = if (gluten.avgMs > 0.0) vanilla.avgMs / gluten.avgMs else 0.0
            writer.println(f"${"Gluten + Velox"}%-40s ${gluten.bestMs}%12.0f ${gluten.avgMs}%12.0f ${gluten.stddevMs}%12.1f $relative%10.1fX")
          }
          writer.println("-" * 100)
          writer.println()
        }
      } finally {
        writer.close()
      }
      
      // scalastyle:off println
      println(s"  ${GREEN}Saved:${RESET} ${outputFile.getName}")
      // scalastyle:on println
    }
  }
}
