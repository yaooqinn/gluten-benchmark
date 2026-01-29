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
  private val results = scala.collection.mutable.ArrayBuffer[BenchmarkResult]()

  private val out: PrintStream = output.map(new PrintStream(_)).getOrElse(System.out)

  /** Add a benchmark case */
  def addCase(caseName: String)(f: => Unit): Unit = {
    cases += ((caseName, () => f))
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

  def getResults: Seq[BenchmarkResult] = results.toSeq
}

case class BenchmarkResult(
    name: String,
    bestTimeMs: Double,
    avgTimeMs: Double,
    stddevMs: Double,
    relative: Double)

/**
 * Base trait for benchmark applications.
 */
trait BenchmarkBase {
  
  protected lazy val output: Option[OutputStream] = {
    val generateFiles = sys.env.getOrElse("SPARK_GENERATE_BENCHMARK_FILES", "0") == "1"
    if (generateFiles) {
      val className = this.getClass.getSimpleName.replace("$", "")
      val dir = new File("benchmarks")
      dir.mkdirs()
      val file = new File(dir, s"$className-results.txt")
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
