// Gluten Micro-Benchmark Project
// Compare Gluten+Velox with Vanilla Spark for fine-grained SQL operations

lazy val sparkVersion = settingKey[String]("Spark version")
lazy val glutenVersion = settingKey[String]("Gluten version")

// Default versions - can be overridden via -Dspark.version=x.y.z
sparkVersion := sys.props.getOrElse("spark.version", "3.5.5")
glutenVersion := sys.props.getOrElse("gluten.version", "1.6.0-SNAPSHOT")

// Spark 4.0+ uses Scala 2.13, earlier versions use Scala 2.12
def scalaVersionForSpark(sparkVer: String): String = {
  if (sparkVer.startsWith("4.1")) "2.13.17"
  else if (sparkVer.startsWith("4.")) "2.13.16"
  else "2.12.18"
}

lazy val root = (project in file("."))
  .settings(
    name := "gluten-benchmark",
    organization := "org.apache.gluten",
    version := "0.1.0-SNAPSHOT",
    
    scalaVersion := scalaVersionForSpark(sparkVersion.value),
    
    // Spark dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "org.apache.spark" %% "spark-core" % sparkVersion.value,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value,
      "org.apache.spark" %% "spark-hive" % sparkVersion.value  // Required by Gluten for Hive support
    ),
    
    // Spark 4.0+ requires spark-sql-api as a separate artifact
    libraryDependencies ++= {
      if (sparkVersion.value.startsWith("4."))
        Seq("org.apache.spark" %% "spark-sql-api" % sparkVersion.value)
      else Seq.empty
    },
    
    // Spark test jars for Benchmark utilities
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % "test"
    ),
    
    // Gluten package - loaded from lib/ directory or via -Dgluten.jar
    // Download nightly: ./scripts/download-gluten-nightly.sh
    // Or specify: ./build/sbt -Dgluten.jar=/path/to/gluten-package.jar
    
    // Support for Gluten JAR (from -Dgluten.jar or lib/ directory)
    Compile / unmanagedJars ++= {
      val explicitJar = sys.props.get("gluten.jar").map(path => Attributed.blank(file(path)))
      val libJar = {
        val libDir = baseDirectory.value / "lib"
        if (libDir.exists()) {
          // Prefer symlink, then any gluten JAR
          val symlink = libDir / "gluten-velox-bundle.jar"
          if (symlink.exists()) Some(Attributed.blank(symlink))
          else (libDir ** "gluten-velox-bundle-*.jar").get.headOption.map(Attributed.blank)
        } else None
      }
      explicitJar.orElse(libJar).toSeq
    },
    
    // Fork JVM to properly load native libraries
    fork := true,
    
    // JVM options for benchmarks
    javaOptions ++= Seq(
      "-Xmx8g",
      "-XX:+UseG1GC",
      "-XX:+UnlockDiagnosticVMOptions",
      "-XX:+DebugNonSafepoints",  // Better profiling support
      // Java 17 module access for Spark and Gluten
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
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      // Additional opens required for Gluten ColumnarShuffleManager
      "--add-opens=java.base/sun.misc=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ),
    
    // Handle dependency version conflicts (Spark 4.0 has stricter requirements)
    libraryDependencySchemes += "com.github.luben" % "zstd-jni" % VersionScheme.Always,
    
    // Enable benchmark result generation (pass env vars to forked JVM)
    Compile / run / envVars ++= Map(
      "SPARK_GENERATE_BENCHMARK_FILES" -> sys.env.getOrElse("SPARK_GENERATE_BENCHMARK_FILES", "0"),
      "SPARK_BENCHMARK_DATE" -> sys.env.getOrElse("SPARK_BENCHMARK_DATE", "")
    ),
    Test / envVars ++= Map(
      "SPARK_GENERATE_BENCHMARK_FILES" -> sys.env.getOrElse("SPARK_GENERATE_BENCHMARK_FILES", "0"),
      "SPARK_BENCHMARK_DATE" -> sys.env.getOrElse("SPARK_BENCHMARK_DATE", "")
    ),
    
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
