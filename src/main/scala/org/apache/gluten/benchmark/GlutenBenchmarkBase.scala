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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

/**
 * Base trait for Gluten micro-benchmarks.
 *
 * Provides automatic comparison between Vanilla Spark and Gluten+Velox.
 * Define benchmarks by implementing the `benchmarks` method.
 */
trait GlutenBenchmarkBase extends BenchmarkBase {

  /** Default cardinality for benchmarks */
  def defaultCardinality: Long = 10000000L

  /** Number of warmup iterations - configurable via system property */
  protected def numWarmupIters: Int = 
    sys.props.getOrElse("benchmark.warmup.iters", "5").toInt

  /** Number of measurement iterations - configurable via system property */
  protected def numIters: Int = 
    sys.props.getOrElse("benchmark.iters", "5").toInt

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

  /** Check if GlutenPlan class is available (for fallback detection) */
  private lazy val glutenPlanClass: Option[Class[_]] = {
    try {
      Some(Class.forName("org.apache.gluten.execution.GlutenPlan"))
    } catch {
      case _: ClassNotFoundException => None
    }
  }

  /** Check if GlutenPlanFallbackEvent class is available */
  private lazy val glutenFallbackEventClass: Option[Class[_]] = {
    try {
      Some(Class.forName("org.apache.gluten.events.GlutenPlanFallbackEvent"))
    } catch {
      case _: ClassNotFoundException => None
    }
  }

  /** Listener to capture Gluten fallback events */
  private class GlutenFallbackListener extends SparkListener {
    val fallbackReasons: mutable.Map[String, String] = mutable.Map.empty
    var numFallbackNodes: Int = 0
    var numGlutenNodes: Int = 0

    override def onOtherEvent(event: SparkListenerEvent): Unit = {
      glutenFallbackEventClass.foreach { eventClass =>
        if (eventClass.isInstance(event)) {
          // Use reflection to access GlutenPlanFallbackEvent fields
          try {
            val numFallbackMethod = eventClass.getMethod("numFallbackNodes")
            val numGlutenMethod = eventClass.getMethod("numGlutenNodes")
            val fallbackReasonsMethod = eventClass.getMethod("fallbackNodeToReason")

            numFallbackNodes = numFallbackMethod.invoke(event).asInstanceOf[Int]
            numGlutenNodes = numGlutenMethod.invoke(event).asInstanceOf[Int]
            val reasons = fallbackReasonsMethod.invoke(event).asInstanceOf[Map[String, String]]
            fallbackReasons ++= reasons
          } catch {
            case _: Exception => // Ignore reflection errors
          }
        }
      }
    }

    def clear(): Unit = {
      fallbackReasons.clear()
      numFallbackNodes = 0
      numGlutenNodes = 0
    }

    def toFallbackInfo: FallbackInfo = {
      FallbackInfo(
        numGlutenNodes,
        numGlutenNodes + numFallbackNodes,
        fallbackReasons.keys.toSeq,
        fallbackReasons.toMap
      )
    }
  }

  /** Thread-local fallback listener */
  private val fallbackListener = new ThreadLocal[GlutenFallbackListener]()

  /** Register fallback listener on Spark session */
  private def registerFallbackListener(spark: SparkSession): GlutenFallbackListener = {
    val listener = new GlutenFallbackListener()
    spark.sparkContext.addSparkListener(listener)
    fallbackListener.set(listener)
    listener
  }

  /** Get current fallback info and clear listener */
  private def getFallbackInfo(): FallbackInfo = {
    Option(fallbackListener.get()).map { listener =>
      val info = listener.toFallbackInfo
      listener.clear()
      info
    }.getOrElse(FallbackInfo(0, 0, Seq.empty))
  }

  /** FallbackTags class for extracting fallback reasons from plan nodes */
  private lazy val fallbackTagsClass: Option[(Class[_], java.lang.reflect.Method, java.lang.reflect.Method)] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.FallbackTags$")
      val instance = clazz.getField("MODULE$").get(null)
      val getMethod = clazz.getMethod("get", classOf[org.apache.spark.sql.catalyst.trees.TreeNode[_]])
      val nonEmptyMethod = clazz.getMethod("nonEmpty", classOf[SparkPlan])
      Some((clazz, getMethod, nonEmptyMethod))
    } catch {
      case _: Exception => None
    }
  }

  /** Extract fallback reason from a plan node using FallbackTags */
  private def getFallbackReason(node: SparkPlan): Option[String] = {
    fallbackTagsClass.flatMap { case (clazz, getMethod, nonEmptyMethod) =>
      try {
        val instance = clazz.getField("MODULE$").get(null)
        val hasTag = nonEmptyMethod.invoke(instance, node).asInstanceOf[Boolean]
        if (hasTag) {
          val tag = getMethod.invoke(instance, node)
          // Get the reason from the tag
          val reasonMethod = tag.getClass.getMethod("reason")
          Some(reasonMethod.invoke(tag).asInstanceOf[String])
        } else {
          None
        }
      } catch {
        case _: Exception => None
      }
    }
  }

  /** Analyze plan for fallback - combines plan analysis with FallbackTags detection */
  protected def analyzeFallback(plan: SparkPlan): FallbackInfo = {
    // First check if we have event-based fallback info
    val eventInfo = getFallbackInfo()
    if (eventInfo.hasFallback) {
      return eventInfo
    }

    // Fall back to plan-based analysis with FallbackTags
    glutenPlanClass match {
      case None => FallbackInfo(0, 0, Seq.empty)
      case Some(glutenClass) =>
        var glutenNodes = 0
        var totalNodes = 0
        val fallbackNodes = scala.collection.mutable.ArrayBuffer[String]()
        val fallbackReasons = mutable.Map[String, String]()
        
        plan.foreachUp { node =>
          totalNodes += 1
          if (glutenClass.isInstance(node)) {
            glutenNodes += 1
          } else if (isRealFallback(node.nodeName)) {
            // This is a real fallback - a Spark node that should have been Gluten
            val nodeName = node.nodeName
            fallbackNodes += nodeName
            // Try to get fallback reason from FallbackTags
            getFallbackReason(node).foreach { reason =>
              fallbackReasons(nodeName) = reason
            }
          }
          // Note: isExpectedNonGlutenNode nodes are neither counted as Gluten nor fallback
        }
        FallbackInfo(glutenNodes, totalNodes, fallbackNodes.toSeq, fallbackReasons.toMap)
    }
  }

  /** Analyze logical plan for FallbackTags */
  private def analyzeLogicalFallback(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): FallbackInfo = {
    val fallbackReasons = mutable.Map[String, String]()
    
    fallbackTagsClass.foreach { case (clazz, _, nonEmptyMethod) =>
      try {
        val instance = clazz.getField("MODULE$").get(null)
        val getMethod = clazz.getMethod("get", classOf[org.apache.spark.sql.catalyst.trees.TreeNode[_]])
        
        plan.foreachUp { node =>
          try {
            // Use reflection since TreeNode is the common parent
            val hasTagMethod = clazz.getMethod("nonEmpty", classOf[org.apache.spark.sql.catalyst.trees.TreeNode[_]])
            val hasTag = hasTagMethod.invoke(instance, node.asInstanceOf[org.apache.spark.sql.catalyst.trees.TreeNode[_]]).asInstanceOf[Boolean]
            if (hasTag) {
              val tag = getMethod.invoke(instance, node.asInstanceOf[org.apache.spark.sql.catalyst.trees.TreeNode[_]])
              val reasonMethod = tag.getClass.getMethod("reason")
              val reason = reasonMethod.invoke(tag).asInstanceOf[String]
              fallbackReasons(node.nodeName) = reason
            }
          } catch {
            case _: Exception => // Ignore individual node errors
          }
        }
      } catch {
        case _: Exception => // Ignore reflection errors
      }
    }
    
    FallbackInfo(0, 0, fallbackReasons.keys.toSeq, fallbackReasons.toMap)
  }

  /** Nodes that are expected to not be Gluten nodes */
  private def isExpectedNonGlutenNode(nodeName: String): Boolean = {
    nodeName match {
      // AQE infrastructure nodes
      case "AdaptiveSparkPlan" | "ResultQueryStage" | "ShuffleQueryStage" |
           "BroadcastQueryStage" | "TableCacheQueryStage" | "ReusedExchange" |
           "Subquery" | "SubqueryBroadcast" => true
      // Columnar-to-row conversion (expected when Gluten is used)
      case name if name.contains("ColumnarToRow") || name.contains("RowToColumnar") => true
      // Gluten's own infrastructure nodes
      case name if name.contains("Carrier") || name.contains("Velox") => true
      case _ => false
    }
  }

  /** Check if a non-Gluten node represents a real fallback (not just infrastructure) */
  private def isRealFallback(nodeName: String): Boolean = {
    // These Spark nodes indicate actual fallback when they appear in a Gluten plan
    nodeName match {
      case "Project" | "ProjectExec" | "Filter" | "FilterExec" |
           "HashAggregate" | "HashAggregateExec" | "SortAggregate" | "SortAggregateExec" |
           "Sort" | "SortExec" | "Generate" | "GenerateExec" => true
      case _ => false
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
    
    // Analyze fallback for Gluten sessions
    val fallbackInfo = if (engineName == GLUTEN_VELOX) {
      // Check both executed plan and logical plan for fallback info
      val executedPlan = df.queryExecution.executedPlan
      val logicalPlan = df.queryExecution.analyzed
      
      val execFallback = analyzeFallback(executedPlan)
      val logicalFallback = analyzeLogicalFallback(logicalPlan)
      
      // Merge fallback info - prefer logical plan reasons if available
      if (logicalFallback.hasFallback) {
        Some(logicalFallback)
      } else {
        Some(execFallback)
      }
    } else {
      None
    }
    
    // Print profiling info with fallback indicator
    val fallbackStr = fallbackInfo match {
      case Some(info) if info.hasFallback => s" [FALLBACK: ${info.fallbackNodes.distinct.size} types]"
      case _ => ""
    }
    println(f"  ${benchDef.name}%-45s $best%10.0f ms (best)  $avg%10.0f ms (avg)  [plan: $planTimeMs%.0fms, first: $firstExecMs%.0fms]$fallbackStr")
    
    BenchmarkResult(benchDef.name, best, avg, stddev, 1.0, fallbackInfo)
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
          
          // Print fallback info if any
          gluten.fallbackInfo.foreach { info =>
            if (info.hasFallback) {
              out.println(s"  ** FALLBACK: ${info.summary}")
            }
          }
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
        // Enable fallback reporting for better diagnostics
        .config("spark.gluten.ui.enabled", "true")
    }

    val spark = builder.getOrCreate()
    
    // Register fallback listener for Gluten sessions
    if (glutenEnabled && glutenFallbackEventClass.isDefined) {
      registerFallbackListener(spark)
    }
    
    spark
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
