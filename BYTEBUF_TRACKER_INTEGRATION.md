# ByteBuf Flow Tracker Integration

This document describes the integration of the ByteBuddy ByteBuf Flow Tracker into the Ambry codebase for detecting and analyzing ByteBuf memory leaks during test runs.

## Overview

The ByteBuf Flow Tracker is a lightweight Java agent that tracks Netty ByteBuf object flows through the application using a Trie data structure. It helps identify memory leaks by monitoring ByteBuf reference counts and detecting unreleased buffers.

## Integration Components

### 1. Git Submodule
- **Location**: `modules/bytebuddy-bytebuf-tracer`
- **Repository**: https://github.com/j-tyler/bytebuddy-bytebuf-tracer
- **Purpose**: Contains the source code for the ByteBuf tracker agent

### 2. Bytebuf-Tracker Subproject
- **Location**: `bytebuf-tracker/`
- **Purpose**: Ambry-specific build of the tracker agent
- **Dependencies**: Reuses Ambry's existing ByteBuddy and Netty dependencies
- **Output**: `bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar` (fat JAR with all dependencies)

### 3. Test Listener
- **File**: `ambry-test-utils/src/main/java/com/github/ambry/testutils/ByteBufTrackerListener.java`
- **Purpose**: JUnit RunListener that prints tracker output at the end of test execution
- **Behavior**:
  - Automatically detects if tracker agent is running
  - Prints summary statistics and detailed flow trees
  - Highlights memory leaks with warnings

### 4. Build Configuration
- **File**: `build.gradle` (root)
- **Changes**:
  - Test tasks automatically load the agent JAR if it exists
  - Test tasks register the ByteBufTrackerListener
  - Configuration applies to both `test` and `intTest` tasks

## Building the Tracker Agent

Due to network connectivity requirements for downloading dependencies, the tracker agent must be built when network access is available.

### Option 1: Using the Build Script (Recommended)

```bash
./build-bytebuf-tracker.sh
```

This script attempts multiple build strategies:
1. Gradle build (preferred)
2. Manual JAR creation from cached dependencies (fallback)

### Option 2: Manual Gradle Build

```bash
./gradlew :bytebuf-tracker:build
```

This builds the agent JAR at: `bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar`

### Option 3: Build from Submodule (RECOMMENDED if build script fails)

If the build script fails due to Gradle/Java version issues, build directly from the submodule:

```bash
cd modules/bytebuddy-bytebuf-tracer
mvn clean install
cd ../..
mkdir -p bytebuf-tracker/build/libs
cp modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/target/bytebuf-flow-tracker-*-agent.jar \
   bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
```

This requires Maven and Java 8-11. If you don't have Maven:

```bash
# Install Maven (macOS)
brew install maven

# Install Maven (Ubuntu/Debian)
sudo apt-get install maven

# Install Maven (other)
# Download from https://maven.apache.org/download.cgi
```

## Running Tests with the Tracker

### Automatic Mode (Recommended)

Once the agent JAR is built, tests automatically use it:

```bash
# Run unit tests with tracker
./gradlew test

# Run integration tests with tracker
./gradlew intTest

# Run all tests
./gradlew allTest
```

The tracker will automatically:
- Instrument all methods in `com.github.ambry` packages
- Track ByteBuf flows and reference counts
- Print a comprehensive report at the end of each test module

### Manual Mode (For Debugging)

To manually control the tracker:

```bash
./gradlew test -Djavaagent.path=/path/to/bytebuf-tracker-agent.jar
```

## Understanding the Output

At the end of test execution, you'll see output like:

```
================================================================================
ByteBuf Flow Tracker Report
================================================================================
=== ByteBuf Flow Summary ===
Total Root Methods: 15
Total Traversals: 342
Unique Paths: 28
Leak Paths: 2

--------------------------------------------------------------------------------
Flow Tree:
--------------------------------------------------------------------------------
ROOT: NettyRequest.readInto [count=45]
└── ChunkProcessor.processChunk [ref=1, count=45]
    └── ChunkProcessor.validate [ref=1, count=45]
        └── ChunkProcessor.release [ref=0, count=45]

ROOT: LeakyHandler.handleRequest [count=3]
└── ErrorHandler.logError [ref=1, count=3] ⚠️ LEAK

--------------------------------------------------------------------------------
Flat Paths (Leaks Highlighted):
--------------------------------------------------------------------------------
LeakyHandler.handleRequest -> ErrorHandler.logError [FINAL REF=1] ⚠️ LEAK
...
```

### Interpreting Results

- **⚠️ LEAK**: Indicates a ByteBuf that was not released (refCount > 0 at leaf node)
- **ref=N**: Current reference count at this point in the flow
- **count=N**: Number of times this path was traversed
- **FINAL REF=N**: Reference count at the end of the flow (should be 0)

## Disabling the Tracker

### Temporary Disable

Delete or rename the agent JAR:
```bash
mv bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar \
   bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar.disabled
```

Tests will run normally without the tracker.

### Permanent Disable

Remove or comment out the tracker configuration in `build.gradle`:

```groovy
// Comment out these lines in the test task:
// def trackerJar = file("${rootProject.projectDir}/bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar")
// if (trackerJar.exists()) {
//     jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry"
//     systemProperty 'bytebuf.tracker.enabled', 'true'
// }
```

## Advanced Configuration

### Tracking Additional Packages

Edit `build.gradle` to include more packages:

```groovy
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry,io.netty.buffer"
```

### Excluding Packages

```groovy
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry;exclude=com.github.ambry.test"
```

### JMX Monitoring

Enable JMX to monitor ByteBuf flows in real-time:

```groovy
systemProperty 'com.sun.management.jmxremote', 'true'
systemProperty 'com.sun.management.jmxremote.port', '9999'
systemProperty 'com.sun.management.jmxremote.authenticate', 'false'
systemProperty 'com.sun.management.jmxremote.ssl', 'false'
```

Then connect with JConsole:
```bash
jconsole localhost:9999
# Navigate to: MBeans → com.example → ByteBufFlowTracker
```

## Troubleshooting

### Agent JAR Not Found

**Symptom**: Tests run but no tracker output appears

**Solution**: Build the agent JAR:
```bash
./build-bytebuf-tracker.sh
```

### Class Not Found Errors

**Symptom**: Tests fail with `ClassNotFoundException` for ByteBuddy classes

**Solution**: The agent JAR may not include all dependencies. Rebuild:
```bash
./gradlew :bytebuf-tracker:clean :bytebuf-tracker:build
```

### No Tracking Data Appears

**Symptom**: Agent loads but no ByteBuf flows are tracked

**Solution**:
1. Verify ByteBufs are actually used in the test
2. Check the package include pattern matches your code
3. Enable verbose logging:
   ```bash
   ./gradlew test --debug | grep ByteBuf
   ```

### Performance Impact

**Symptom**: Tests run significantly slower with tracker enabled

**Solution**:
1. Narrow the package scope to only critical areas
2. Use the tracker selectively for suspected leak areas
3. Exclude test utility packages
4. Run tracker on a subset of tests

### Network Issues During Build

**Symptom**: Build fails with network connection errors

**Solution**:
1. Retry the build (transient issues)
2. Build from the submodule using Maven:
   ```bash
   cd modules/bytebuddy-bytebuf-tracer
   mvn clean install
   cd ../..
   mkdir -p bytebuf-tracker/build/libs
   cp modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/target/*-agent.jar \
      bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
   ```
3. Wait for network connectivity and run `./build-bytebuf-tracker.sh`

### Gradle/Java Version Compatibility Issues

**Symptom**: Build fails with `NoClassDefFoundError: Could not initialize class org.codehaus.groovy.vmplugin.v7.Java7`

**Cause**: Java version incompatibility with Gradle version

**Solution**: Build from the submodule using Maven (which is more tolerant of Java versions):
```bash
cd modules/bytebuddy-bytebuf-tracer
mvn clean install -DskipTests
mkdir -p ../../bytebuf-tracker/build/libs
cp bytebuf-flow-tracker/target/*-agent.jar ../../bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
cd ../..
```

Alternatively, use a compatible Java version (Java 8-11):
```bash
# Check your Java version
java -version

# On macOS with SDKMAN
sdk install java 11.0.12-open
sdk use java 11.0.12-open

# Then retry
./build-bytebuf-tracker.sh
```

## Implementation Notes

### Why This Approach?

1. **Zero code changes**: Tracker is added via Java agent (bytecode instrumentation)
2. **Gradle-native**: Integrates cleanly with existing Ambry build
3. **Optional**: Tests work with or without the tracker
4. **Comprehensive**: Tracks all ByteBuf flows automatically
5. **Production-safe**: Only active during tests, never in production

### Architecture

```
Ambry Test Suite
       ↓
   JUnit Tests
       ↓
ByteBufTrackerListener ←------ Prints report at end
       ↑
ByteBufFlowTracker ←----------- Tracks flows during tests
       ↑
ByteBuddy Agent ←-------------- Instruments methods at JVM startup
```

### Key Files

- `modules/bytebuddy-bytebuf-tracer/` - Submodule with tracker source
- `bytebuf-tracker/` - Ambry's build of the tracker
- `bytebuf-tracker/build.gradle` - Build configuration
- `build-bytebuf-tracker.sh` - Build automation script
- `ambry-test-utils/.../ByteBufTrackerListener.java` - Test integration
- `build.gradle` - Test task configuration

## References

- [ByteBuf Flow Tracker GitHub](https://github.com/j-tyler/bytebuddy-bytebuf-tracer)
- [ByteBuddy Documentation](https://bytebuddy.net/)
- [Netty ByteBuf Reference Counting](https://netty.io/wiki/reference-counted-objects.html)
- [Ambry Memory Leak Investigation](https://github.com/linkedin/ambry/issues/XXXX)

## Future Enhancements

Potential improvements:

1. **CI Integration**: Automatically fail builds on detected leaks
2. **Leak Thresholds**: Configure acceptable leak levels
3. **Per-Test Tracking**: Track and report leaks per individual test
4. **Historical Tracking**: Store leak reports for trend analysis
5. **Custom Object Tracking**: Extend beyond ByteBuf to other resources
6. **HTML Reports**: Generate visual flow diagrams

## Support

For issues with the ByteBuf tracker:
1. Check this document's troubleshooting section
2. Review the tracker's README: `modules/bytebuddy-bytebuf-tracer/README.md`
3. Open an issue on the tracker repository
4. Contact the Ambry team for integration-specific issues
