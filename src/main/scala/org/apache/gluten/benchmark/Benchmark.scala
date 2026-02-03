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

import java.io.{File, FileOutputStream, OutputStream, PrintStream}

/**
 * A simple benchmark utility inspired by Spark's Benchmark class.
 * Measures execution time of workloads and produces formatted output.
 *
 * @param name        Name of the benchmark
 * @param cardinality Number of rows/items (for rate calculation)
 * @param numIters    Number of iterations to run
 * @param warmupIters Number of warmup iterations
 * @param output      Optional output stream for results
 */
class Benchmark(
    name: String,
    cardinality: Long,
    numIters: Int = 5,
    warmupIters: Int = 2,
    output: Option[OutputStream] = None) {

  private val cases = scala.collection.mutable.ArrayBuffer[(String, () => Unit)]()
  private val phasedCases = scala.collection.mutable.ArrayBuffer[(String, () => PhasedTiming)]()
  private val managedCases = scala.collection.mutable.ArrayBuffer[ManagedCase[_]]()
  private val results = scala.collection.mutable.ArrayBuffer[BenchmarkResult]()
  private val phasedResults = scala.collection.mutable.ArrayBuffer[PhasedBenchmarkResult]()

  private val out: PrintStream = output.map(new PrintStream(_)).getOrElse(System.out)

  /** Add a benchmark case */
  def addCase(caseName: String)(f: => Unit): Unit = {
    cases += ((caseName, () => f))
  }

  /** Add a benchmark case with phased timing (startup vs query) */
  def addPhasedCase(caseName: String)(f: => PhasedTiming): Unit = {
    phasedCases += ((caseName, () => f))
  }

  /** Run all benchmark cases */
  def run(): Unit = {
    out.println()
    out.println(s"$name:")
    out.println("-" * 80)
    out.printf("%-40s %12s %12s %12s %10s\n",
      "", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Relative")
    out.println("-" * 80)

    var baseline: Option[Double] = None

    cases.foreach { case (caseName, workload) =>
      // Warmup
      (1 to warmupIters).foreach { _ =>
        workload()
      }

      // Measurement
      val times = (1 to numIters).map { _ =>
        val start = System.nanoTime()
        workload()
        val end = System.nanoTime()
        (end - start) / 1e6  // Convert to milliseconds
      }

      val best = times.min
      val avg = times.sum / times.length
      val stddev = math.sqrt(times.map(t => math.pow(t - avg, 2)).sum / times.length)

      if (baseline.isEmpty) baseline = Some(avg)
      val relative = baseline.get / avg

      results += BenchmarkResult(caseName, best, avg, stddev, relative)

      out.println(f"$caseName%-40s $best%12.0f $avg%12.0f $stddev%12.1f $relative%10.1fX")
    }

    out.println("-" * 80)
    out.println()
  }

  /**
   * Add a benchmark case with managed session lifecycle.
   * Session is created once before all iterations, query runs multiple times.
   * @param caseName Name of the benchmark case
   * @param createSession Function to create the session/context
   * @param cleanup Function to cleanup the session/context
   * @param query Function that takes the session and executes the query
   */
  def addManagedCase[S](caseName: String)(
      createSession: () => S)(
      cleanup: S => Unit)(
      query: S => Unit): Unit = {
    managedCases += ManagedCase(caseName, () => createSession(), cleanup, query)
  }

  /** Run all managed benchmark cases (session created once, query timed per iteration) */
  def runManaged(): Unit = {
    out.println()
    out.println(s"$name:")
    out.println("-" * 80)
    out.printf("%-40s %12s %12s %12s %10s\n",
      "", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Relative")
    out.println("-" * 80)

    var baseline: Option[Double] = None

    managedCases.foreach { case ManagedCase(caseName, createSession, cleanup, query) =>
      // Create session once
      val session = createSession()
      try {
        // Warmup iterations (using same session)
        (1 to warmupIters).foreach { _ =>
          query(session)
        }

        // Measurement iterations (using same session)
        val times = (1 to numIters).map { _ =>
          val start = System.nanoTime()
          query(session)
          val end = System.nanoTime()
          (end - start) / 1e6
        }

        val best = times.min
        val avg = times.sum / times.length
        val stddev = math.sqrt(times.map(t => math.pow(t - avg, 2)).sum / times.length)

        if (baseline.isEmpty) baseline = Some(avg)
        val relative = baseline.get / avg

        results += BenchmarkResult(caseName, best, avg, stddev, relative)

        out.println(f"$caseName%-40s $best%12.0f $avg%12.0f $stddev%12.1f $relative%10.1fX")
      } finally {
        cleanup(session)
      }
    }

    out.println("-" * 80)
    out.println()
  }

  /** Run all phased benchmark cases (with separate startup/query timing) */
  def runPhased(): Unit = {
    out.println()
    out.println(s"$name:")
    out.println("-" * 100)
    out.printf("%-30s %14s %14s %14s %14s %10s\n",
      "", "Startup(ms)", "Query Best(ms)", "Query Avg(ms)", "Query Stdev(ms)", "Relative")
    out.println("-" * 100)

    var baselineQueryAvg: Option[Double] = None

    phasedCases.foreach { case (caseName, workload) =>
      // Warmup iterations
      (1 to warmupIters).foreach { _ =>
        workload()
      }

      // Measurement iterations
      val timings = (1 to numIters).map { _ =>
        workload()
      }

      val startupTimes = timings.map(_.startupMs)
      val queryTimes = timings.map(_.queryMs)

      val startupAvg = startupTimes.sum / startupTimes.length
      val queryBest = queryTimes.min
      val queryAvg = queryTimes.sum / queryTimes.length
      val queryStddev = math.sqrt(queryTimes.map(t => math.pow(t - queryAvg, 2)).sum / queryTimes.length)

      if (baselineQueryAvg.isEmpty) baselineQueryAvg = Some(queryAvg)
      val relative = baselineQueryAvg.get / queryAvg

      phasedResults += PhasedBenchmarkResult(caseName, startupAvg, queryBest, queryAvg, queryStddev, relative)

      out.println(f"$caseName%-30s $startupAvg%14.0f $queryBest%14.0f $queryAvg%14.0f $queryStddev%14.1f $relative%10.1fX")
    }

    out.println("-" * 100)
    out.println()
  }

  def getResults: Seq[BenchmarkResult] = results.toSeq
  def getPhasedResults: Seq[PhasedBenchmarkResult] = phasedResults.toSeq
}

case class BenchmarkResult(
    name: String,
    bestTimeMs: Double,
    avgTimeMs: Double,
    stddevMs: Double,
    relative: Double,
    fallbackInfo: Option[FallbackInfo] = None)

/** Information about Gluten fallback */
case class FallbackInfo(
    glutenNodes: Int,
    totalNodes: Int,
    fallbackNodes: Seq[String],
    fallbackReasons: Map[String, String] = Map.empty) {
  def hasFallback: Boolean = fallbackNodes.nonEmpty || fallbackReasons.nonEmpty
  def fallbackRatio: Double = if (totalNodes > 0) fallbackNodes.size.toDouble / totalNodes else 0.0
  def summary: String = {
    if (fallbackReasons.nonEmpty) {
      // Use Gluten's detailed fallback reasons
      val reasons = fallbackReasons.values.toSeq.distinct.take(3)
      s"${fallbackReasons.size} fallbacks: ${reasons.mkString("; ")}"
    } else if (fallbackNodes.nonEmpty) {
      s"${fallbackNodes.size} fallback nodes: ${fallbackNodes.distinct.mkString(", ")}"
    } else {
      "No fallback"
    }
  }
}

/** Timing result for a single phased run */
case class PhasedTiming(
    startupMs: Double,
    queryMs: Double)

/** Benchmark result with separate startup and query times */
case class PhasedBenchmarkResult(
    name: String,
    startupAvgMs: Double,
    queryBestMs: Double,
    queryAvgMs: Double,
    queryStddevMs: Double,
    relative: Double)

/** Helper class for managed benchmark cases */
private[benchmark] case class ManagedCase[S](
    name: String,
    createSession: () => S,
    cleanup: S => Unit,
    query: S => Unit)

/**
 * Base trait for benchmark applications.
 */
trait BenchmarkBase {

  /** Get Spark major.minor version for result file naming (e.g., "3.5") */
  protected lazy val sparkVersion: String = {
    sys.props.getOrElse("spark.benchmark.version", {
      val fullVersion = org.apache.spark.SPARK_VERSION
      // Extract major.minor (e.g., "3.5.5" -> "3.5")
      fullVersion.split("\\.").take(2).mkString(".")
    })
  }

  /** Whether Gluten is enabled for this run */
  protected def glutenEnabled: Boolean = true

  /** Engine suffix for result files */
  protected def engineSuffix: String = if (glutenEnabled) "gluten" else "vanilla"

  protected lazy val output: Option[OutputStream] = {
    val generateFiles = sys.env.getOrElse("SPARK_GENERATE_BENCHMARK_FILES", "0") == "1"
    if (generateFiles) {
      val className = this.getClass.getSimpleName.replace("$", "")
      
      // Support dated subdirectories: SPARK_BENCHMARK_DATE=2026-01-30
      val dateSubdir = sys.env.get("SPARK_BENCHMARK_DATE")
      val baseDir = new File("benchmarks")
      val dir = dateSubdir match {
        case Some(date) => new File(baseDir, date)
        case None => baseDir
      }
      dir.mkdirs()
      
      // Include Spark major.minor version in filename: AggregateBenchmark-spark3.5-results.txt
      val file = new File(dir, s"$className-spark$sparkVersion-results.txt")
      Some(new FileOutputStream(file))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    runBenchmarkSuite(args)
    output.foreach(_.close())
  }

  def runBenchmarkSuite(args: Array[String]): Unit

  protected def runBenchmark(name: String)(f: => Unit): Unit = {
    println(s"Running benchmark: $name")
    f
  }
}
