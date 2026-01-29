#!/bin/bash
#
# Download Gluten nightly build from Apache Nightlies
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LIB_DIR="$PROJECT_DIR/lib"

# Default values
SPARK_VERSION="${SPARK_VERSION:-3.5}"
GLUTEN_VERSION="${GLUTEN_VERSION:-1.6.0-SNAPSHOT}"
ARCH="${ARCH:-linux_amd64}"

# Determine Scala version based on Spark version
if [[ "$SPARK_VERSION" == "4.0" ]]; then
  SCALA_VERSION="2.13"
else
  SCALA_VERSION="2.12"
fi

# Construct JAR name and URL
JAR_NAME="gluten-velox-bundle-spark${SPARK_VERSION}_${SCALA_VERSION}-${ARCH}-${GLUTEN_VERSION}.jar"
NIGHTLY_URL="https://nightlies.apache.org/gluten/nightly-release-jdk17/${JAR_NAME}"

echo "============================================"
echo "Downloading Gluten nightly build"
echo "============================================"
echo "Spark Version: ${SPARK_VERSION}"
echo "Gluten Version: ${GLUTEN_VERSION}"
echo "Scala Version: ${SCALA_VERSION}"
echo "Architecture: ${ARCH}"
echo "JAR: ${JAR_NAME}"
echo "URL: ${NIGHTLY_URL}"
echo "============================================"

# Create lib directory
mkdir -p "$LIB_DIR"

# Download if not exists or force refresh
TARGET_PATH="$LIB_DIR/$JAR_NAME"
if [[ -f "$TARGET_PATH" && "$1" != "--force" ]]; then
  echo "JAR already exists: $TARGET_PATH"
  echo "Use --force to re-download"
else
  echo "Downloading..."
  curl -L -o "$TARGET_PATH" "$NIGHTLY_URL"
  echo "Downloaded to: $TARGET_PATH"
fi

# Create symlink for easy access
SYMLINK_PATH="$LIB_DIR/gluten-velox-bundle.jar"
rm -f "$SYMLINK_PATH"
ln -s "$JAR_NAME" "$SYMLINK_PATH"
echo "Symlink created: $SYMLINK_PATH -> $JAR_NAME"

echo ""
echo "To run benchmarks with Gluten:"
echo "  ./build/sbt -Dgluten.jar=$TARGET_PATH \"runMain org.apache.gluten.benchmark.RunAllBenchmarks\""
echo ""
echo "Or use the symlink:"
echo "  ./build/sbt -Dgluten.jar=$SYMLINK_PATH \"runMain org.apache.gluten.benchmark.RunAllBenchmarks\""
