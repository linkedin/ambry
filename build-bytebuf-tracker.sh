#!/bin/bash
#
# Script to build the ByteBuf Flow Tracker agent JAR
# This script should be run when network connectivity is available
#

set -e

echo "Building ByteBuf Flow Tracker..."

cd "$(dirname "$0")"

# Create output directory
mkdir -p bytebuf-tracker/build/libs

# Method 1: Build from the submodule using Maven
echo "Attempting to build from submodule with Maven..."
if [ -d "modules/bytebuddy-bytebuf-tracer" ]; then
    cd modules/bytebuddy-bytebuf-tracer

    # Try Maven build
    if command -v mvn &> /dev/null; then
        echo "Using Maven to build..."
        if mvn clean install -DskipTests 2>&1 | tee /tmp/maven-build.log; then
            # Find the agent JAR
            AGENT_JAR=$(find bytebuf-flow-tracker/target -name "*-agent.jar" -type f | head -1)
            if [ -n "$AGENT_JAR" ] && [ -f "$AGENT_JAR" ]; then
                echo "Found agent JAR: $AGENT_JAR"
                cp "$AGENT_JAR" ../../bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
                cd ../..
                echo "SUCCESS: Agent JAR copied to bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
                exit 0
            else
                echo "Maven build completed but agent JAR not found"
            fi
        else
            echo "Maven build failed. Check /tmp/maven-build.log for details"
        fi
    else
        echo "Maven not found, trying Gradle..."
    fi

    # Try Gradle build from submodule
    if command -v gradle &> /dev/null; then
        echo "Using Gradle to build..."
        cd bytebuf-flow-tracker
        if gradle build -x test 2>&1 | tee /tmp/gradle-build.log; then
            # Find the agent JAR
            AGENT_JAR=$(find build/libs -name "*-agent.jar" -type f | head -1)
            if [ -n "$AGENT_JAR" ] && [ -f "$AGENT_JAR" ]; then
                echo "Found agent JAR: $AGENT_JAR"
                cp "$AGENT_JAR" ../../../bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
                cd ../../..
                echo "SUCCESS: Agent JAR copied to bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
                exit 0
            fi
        fi
    fi

    cd ../..
fi

# Method 2: Try to build with Ambry's Gradle using correct Java version
echo "Attempting to build with Ambry's Gradle wrapper..."
if [ -x "./gradlew" ]; then
    # Stop any running Gradle daemons that might be using wrong Java version
    ./gradlew --stop 2>/dev/null || true

    # Try to build
    if ./gradlew :bytebuf-tracker:agentJar --no-daemon 2>&1 | tee /tmp/ambry-gradle-build.log; then
        if [ -f "bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar" ]; then
            echo "SUCCESS: Agent JAR built at bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
            exit 0
        fi
    fi
fi

# If all methods failed, provide helpful error message
echo ""
echo "======================================================================"
echo "FAILED: Unable to build the ByteBuf Flow Tracker agent JAR"
echo "======================================================================"
echo ""
echo "The build failed due to one of the following reasons:"
echo "1. Gradle/Java version incompatibility"
echo "2. Network connectivity issues preventing dependency downloads"
echo "3. Missing build tools (Maven or Gradle)"
echo ""
echo "WORKAROUND OPTIONS:"
echo ""
echo "Option 1: Build on a different machine with Java 8-11 and Maven:"
echo "  cd modules/bytebuddy-bytebuf-tracer"
echo "  mvn clean install"
echo "  cp bytebuf-flow-tracker/target/*-agent.jar ../../bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
echo ""
echo "Option 2: Use pre-built JAR from the tracker repository releases"
echo "  (if available)"
echo ""
echo "Option 3: Run tests without the tracker:"
echo "  Tests will run normally without ByteBuf flow tracking"
echo ""
echo "Check build logs at:"
echo "  - /tmp/maven-build.log"
echo "  - /tmp/gradle-build.log"
echo "  - /tmp/ambry-gradle-build.log"
echo ""
exit 1
