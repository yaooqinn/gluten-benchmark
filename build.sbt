// Gluten Micro-Benchmark Project
// Compare Gluten+Velox with Vanilla Spark for fine-grained SQL operations

lazy val sparkVersion = settingKey[String]("Spark version")
lazy val glutenVersion = settingKey[String]("Gluten version")

// Default versions - can be overridden via -Dspark.version=x.y.z
sparkVersion := sys.props.getOrElse("spark.version", "3.5.5")
glutenVersion := sys.props.getOrElse("gluten.version", "1.6.0-SNAPSHOT")

lazy val root = (project in file("."))
  .settings(
    name := "gluten-benchmark",
    organization := "org.apache.gluten",
    version := "0.1.0-SNAPSHOT",
    
    scalaVersion := "2.12.18",
    
    // Spark dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
      
      // Spark test jars for Benchmark utilities
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      
      // Gluten package (includes native libs)
      "org.apache.gluten" % s"gluten-package-${sparkVersion.value.take(3)}_2.12" % glutenVersion.value % "test",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % "test"
    ),
    
    // Resolvers for Gluten SNAPSHOT
    resolvers ++= Seq(
      "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
      "Gluten Snapshots" at "https://repository.apache.org/content/groups/snapshots/"
    ),
    
    // Fork JVM to properly load native libraries
    fork := true,
    
    // JVM options for benchmarks
    javaOptions ++= Seq(
      "-Xmx8g",
      "-XX:+UseG1GC",
      "-XX:+UnlockDiagnosticVMOptions",
      "-XX:+DebugNonSafepoints"  // Better profiling support
    ),
    
    // Enable benchmark result generation
    Test / envVars += "SPARK_GENERATE_BENCHMARK_FILES" -> sys.env.getOrElse("SPARK_GENERATE_BENCHMARK_FILES", "0"),
    
    // Output benchmark results to benchmarks/ directory
    Test / baseDirectory := (ThisBuild / baseDirectory).value,
    
    // Compile options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    )
  )

// Custom task to run all benchmarks
lazy val runBenchmarks = taskKey[Unit]("Run all benchmarks")
runBenchmarks := {
  (Test / runMain).toTask(" org.apache.gluten.benchmark.RunAllBenchmarks").value
}

// Custom task to run specific benchmark
lazy val runBenchmark = inputKey[Unit]("Run a specific benchmark")
runBenchmark := {
  import complete.DefaultParsers._
  val args = spaceDelimited("<benchmark>").parsed
  args.headOption.foreach { benchmarkName =>
    (Test / runMain).toTask(s" org.apache.gluten.benchmark.$benchmarkName").value
  }
}
