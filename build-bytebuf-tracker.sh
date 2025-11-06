#!/bin/bash
#
# Script to build the ByteBuf Flow Tracker agent JAR
# This script should be run when network connectivity is available
#

set -e

echo "Building ByteBuf Flow Tracker..."

cd "$(dirname "$0")"

# Check if the tracker module exists
if [ ! -d "bytebuf-tracker" ]; then
    echo "Error: bytebuf-tracker module not found"
    exit 1
fi

# Try to build with Gradle
echo "Attempting to build with Gradle..."
if ./gradlew :bytebuf-tracker:build; then
    echo "Build successful!"
    echo "Agent JAR location: bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
    exit 0
fi

# If Gradle build fails, try manual approach
echo "Gradle build failed, attempting manual JAR creation..."

# Create build directories
mkdir -p bytebuf-tracker/build/classes
mkdir -p bytebuf-tracker/build/libs
mkdir -p bytebuf-tracker/build/tmp

# Compile the source files
echo "Compiling source files..."
CLASSPATH=$(./gradlew :bytebuf-tracker:dependencies --configuration compile | grep -E "\.jar$" | tr '\n' ':')

find bytebuf-tracker/src/main/java -name "*.java" | xargs javac \
    -d bytebuf-tracker/build/classes \
    -classpath "$CLASSPATH" \
    -source 1.8 -target 1.8

if [ $? -ne 0 ]; then
    echo "Compilation failed"
    exit 1
fi

# Extract dependencies
echo "Extracting dependencies..."
cd bytebuf-tracker/build/tmp
for jar in ~/.gradle/caches/modules-2/files-2.1/net/bytebuddy/byte-buddy/*/byte-buddy-*.jar; do
    if [ -f "$jar" ]; then
        unzip -q -o "$jar"
    fi
done

for jar in ~/.gradle/caches/modules-2/files-2.1/net/bytebuddy/byte-buddy-agent/*/byte-buddy-agent-*.jar; do
    if [ -f "$jar" ]; then
        unzip -q -o "$jar"
    fi
done

# Copy compiled classes
cp -r ../classes/* .

# Create manifest
cat > MANIFEST.MF << 'EOF'
Manifest-Version: 1.0
Premain-Class: com.example.bytebuf.tracker.agent.ByteBufFlowAgent
Agent-Class: com.example.bytebuf.tracker.agent.ByteBufFlowAgent
Can-Redefine-Classes: true
Can-Retransform-Classes: true
Boot-Class-Path:

EOF

# Create agent JAR
cd ..
jar cfm libs/bytebuf-tracker-agent.jar tmp/MANIFEST.MF -C tmp .

cd ../..

echo "Agent JAR created successfully at: bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar"
