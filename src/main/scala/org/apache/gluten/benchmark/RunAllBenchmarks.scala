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

/**
 * Runner to execute all registered benchmarks.
 *
 * Usage:
 *   sbt "testOnly *RunAllBenchmarks"
 *   or
 *   sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
 */
object RunAllBenchmarks {

  // Registry of all benchmark objects
  private val allBenchmarks: Seq[GlutenBenchmarkBase] = Seq(
    aggregate.AggregateBenchmark,
    functions.StringFunctionsBenchmark,
    functions.MapFunctionsBenchmark,
    functions.ExplodeBenchmark,
    functions.HigherOrderFunctionsBenchmark
    // Add more benchmarks here as they are created
    // join.JoinBenchmark,
    // scan.ScanBenchmark,
  )

  def main(args: Array[String]): Unit = {
    val filter = args.headOption

    val toRun = filter match {
      case Some(pattern) =>
        allBenchmarks.filter(_.getClass.getSimpleName.toLowerCase.contains(pattern.toLowerCase))
      case None =>
        allBenchmarks
    }

    // scalastyle:off println
    println(s"Running ${toRun.size} benchmark suite(s)...")
    println("=" * 80)
    // scalastyle:on println

    toRun.foreach { benchmark =>
      // scalastyle:off println
      println(s"\n>>> Running: ${benchmark.getClass.getSimpleName}")
      println("-" * 80)
      // scalastyle:on println

      benchmark.main(args.drop(1))
    }

    // scalastyle:off println
    println("\n" + "=" * 80)
    println("All benchmarks completed!")
    // scalastyle:on println
  }
}
