#!/bin/bash
#
# Quick build script using Maven from the submodule
# Use this if the main build-bytebuf-tracker.sh fails
#

set -e

echo "Building ByteBuf Flow Tracker using Maven from submodule..."

cd "$(dirname "$0")"

if [ ! -d "modules/bytebuddy-bytebuf-tracer" ]; then
    echo "Error: Submodule not found at modules/bytebuddy-bytebuf-tracer"
    echo "Run: git submodule update --init --recursive"
    exit 1
fi

if ! command -v mvn &> /dev/null; then
    echo "Error: Maven not found. Please install Maven:"
    echo "  macOS:    brew install maven"
    echo "  Ubuntu:   sudo apt-get install maven"
    echo "  Other:    https://maven.apache.org/download.cgi"
    exit 1
fi

echo "Building with Maven..."
cd modules/bytebuddy-bytebuf-tracer

mvn clean install -DskipTests

# Find and copy the agent JAR
AGENT_JAR=$(find bytebuf-flow-tracker/target -name "*-agent.jar" -type f | head -1)

if [ -z "$AGENT_JAR" ] || [ ! -f "$AGENT_JAR" ]; then
    echo "Error: Agent JAR not found after build"
    exit 1
fi

echo "Found agent JAR: $AGENT_JAR"

mkdir -p ../../bytebuf-tracker/build/libs
cp "$AGENT_JAR" ../../bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar

cd ../..

echo ""
echo "SUCCESS! Agent JAR is ready at:"
echo "  bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
echo ""
echo "You can now run tests with ByteBuf tracking:"
echo "  ./gradlew test"
echo ""
