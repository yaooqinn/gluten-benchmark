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
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "org.apache.spark" %% "spark-core" % sparkVersion.value,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value,
      
      // Spark test jars for Benchmark utilities
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % "test"
    ),
    
    // Gluten package - optional, add via local JAR or when published
    // To use local Gluten build:
    //   ./build/sbt -Dgluten.jar=/path/to/gluten-package.jar compile
    libraryDependencies ++= {
      val glutenJar = sys.props.get("gluten.jar")
      if (glutenJar.isDefined) Seq.empty  // Will be added via unmanagedJars
      else Seq.empty  // Skip Gluten for now - not published to Maven
    },
    
    // Support for local Gluten JAR
    Compile / unmanagedJars ++= {
      sys.props.get("gluten.jar").map(path => Attributed.blank(file(path))).toSeq
    },
    
    // Fork JVM to properly load native libraries
    fork := true,
    
    // JVM options for benchmarks
    javaOptions ++= Seq(
      "-Xmx8g",
      "-XX:+UseG1GC",
      "-XX:+UnlockDiagnosticVMOptions",
      "-XX:+DebugNonSafepoints",  // Better profiling support
      // Java 17 module access for Spark
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
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

// To run benchmarks:
//   ./build/sbt "runMain org.apache.gluten.benchmark.RunAllBenchmarks"
//   ./build/sbt "runMain org.apache.gluten.benchmark.aggregate.AggregateBenchmark"
