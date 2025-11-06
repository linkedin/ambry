# ByteBuf Flow Tracker - Ambry Integration

This module integrates the [ByteBuddy ByteBuf Flow Tracker](https://github.com/j-tyler/bytebuddy-bytebuf-tracer) into Ambry for detecting and analyzing ByteBuf memory leaks during test execution.

## Overview

The ByteBuf Flow Tracker is a lightweight Java agent that tracks Netty ByteBuf object flows through the application using a Trie data structure. It helps identify memory leaks by monitoring ByteBuf reference counts and detecting unreleased buffers.

**Key Features:**
- Zero allocation overhead - no stack trace collection
- First-touch root approach - initial handler becomes the Trie root
- Memory efficient - shared Trie prefixes minimize storage
- Dual output formats - human-readable trees and LLM-optimized structured text
- JMX integration - runtime monitoring via MBean
- **Gradle-native** - fully integrated into Ambry's build system
- **Java 8 compatible** - matches Ambry's target version

## Quick Start

### Run Tests with ByteBuf Tracking

```bash
# Run all tests with tracking
./gradlew test -PwithByteBufTracking

# Run specific module tests
./gradlew :ambry-commons:test -PwithByteBufTracking

# Run integration tests
./gradlew intTest -PwithByteBufTracking
```

### Run Tests WITHOUT Tracking

```bash
./gradlew test
```

Tests run normally without any tracking overhead when the flag is omitted.

## Recent Updates (from upstream)

This integration has been updated with the latest changes from the upstream repository:

### New Features

1. **Comprehensive Test Suite**
   - Added `ByteBufFlowTrackerTest.java` with multiple test scenarios
   - Validates flow tracking, leak detection, and output formats
   - Located in: `src/test/java/com/example/bytebuf/tracker/test/`

2. **Enhanced Documentation**
   - `CLAUDE_CODE_INTEGRATION.md` - Integration guide for Claude Code projects
   - `MIGRATION_GUIDE.md` - Project restructuring history
   - `UPSTREAM_README.md` - Reference documentation from upstream

3. **Multi-Module Structure Support**
   - Upstream now organized as multi-module Maven project
   - Ambry integration maintains Gradle compatibility
   - Source synced from `bytebuf-flow-tracker` module

4. **Improved JMX Interface**
   - Public interface for better accessibility
   - Enhanced export and monitoring capabilities

### Core Components

#### Source Files (from upstream)

- **`ByteBufFlowTracker.java`** - Main tracking logic using Trie structure
- **`FlowTrie.java`** - Trie data structure for efficient path tracking
- **`TrieRenderer.java`** - Output formatting (tree, flat, CSV, JSON)
- **`ByteBufFlowAgent.java`** - Java agent entry point
- **`ByteBufTrackingAdvice.java`** - ByteBuddy method interception
- **`ByteBufFlowMBean.java`** - JMX monitoring interface

#### Ambry-Specific Extensions

- **`ObjectTrackerHandler.java`** - Interface for tracking any object type
- **`ByteBufObjectHandler.java`** - Implementation for ByteBuf tracking
- **`ObjectTrackerRegistry.java`** - Registry for custom handlers
- **`build.gradle`** - Gradle build configuration

## Architecture

### How It Works

1. **ByteBuddy Instrumentation**: Agent intercepts all methods in specified packages
2. **First Touch = Root**: First method to handle a ByteBuf becomes its Trie root
3. **Path Building**: Each subsequent method call adds a node to the tree
4. **Metric Tracking**: Each node records the ByteBuf's reference count
5. **Leak Detection**: Leaf nodes with refCount > 0 indicate leaks

### Integration Flow

```
./gradlew test -PwithByteBufTracking
       ↓
Gradle detects withByteBufTracking property
       ↓
Builds :bytebuf-tracker:agentJar (if needed)
       ↓
Test JVM starts with -javaagent
       ↓
ByteBuddy instruments com.github.ambry classes
       ↓
Tests run - ByteBuf flows tracked in Trie
       ↓
ByteBufTrackerListener prints report to file
```

## Understanding the Output

At the end of test execution, you'll see a report written to:
`build/reports/bytebuf-tracking/[module-name].txt`

Example output:

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
```

### Interpreting Results

- **⚠️ LEAK** - ByteBuf was not released (refCount > 0 at leaf node)
- **ref=N** - Current reference count at this point in the flow
- **count=N** - Number of times this path was traversed
- **FINAL REF=N** - Reference count at the end of the flow (should be 0)
- **ROOT** - The first method that handled the ByteBuf

## Configuration

### Package Filtering

Edit `build.gradle` in the root project to customize package tracking:

```groovy
// Track additional packages
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry,io.netty.buffer"

// Exclude packages
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry;exclude=com.github.ambry.test"
```

### JMX Monitoring

Enable JMX for real-time monitoring:

```groovy
test {
    systemProperty 'com.sun.management.jmxremote', 'true'
    systemProperty 'com.sun.management.jmxremote.port', '9999'
    systemProperty 'com.sun.management.jmxremote.authenticate', 'false'
    systemProperty 'com.sun.management.jmxremote.ssl', 'false'
}
```

Connect with JConsole:
```bash
jconsole localhost:9999
# Navigate to: MBeans → com.example → ByteBufFlowTracker
```

**JMX Operations**: `getTreeView()`, `getFlatView()`, `getCsvView()`, `getJsonView()`, `getSummary()`, `reset()`

## Extensibility: Custom Object Tracking

The tracker supports tracking any object type, not just ByteBuf:

```java
public class ConnectionTracker implements ObjectTrackerHandler {
    public boolean shouldTrack(Object obj) {
        return obj instanceof Connection;
    }

    public int getMetric(Object obj) {
        return ((Connection) obj).isClosed() ? 0 : 1;
    }

    public String getObjectType() {
        return "DatabaseConnection";
    }
}

// Register in your application
ObjectTrackerRegistry.setHandler(new ConnectionTracker());
```

## Development

### Building the Agent

```bash
# Build with tracking enabled
./gradlew :bytebuf-tracker:agentJar -PwithByteBufTracking

# Output location
./bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
```

### Running Tests

```bash
# Run tracker's own tests
./gradlew :bytebuf-tracker:test

# Run with tracking on another module
./gradlew :ambry-commons:test -PwithByteBufTracking
```

### Updating from Upstream

The tracker source is maintained in a git submodule:

```bash
# Update submodule to latest
git submodule update --remote modules/bytebuddy-bytebuf-tracer

# Copy updated files (if needed)
cp modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/src/main/java/* \
   bytebuf-tracker/src/main/java/

# Rebuild
./gradlew :bytebuf-tracker:agentJar -PwithByteBufTracking
```

## Troubleshooting

### "ByteBuf tracker agent JAR not found"

Make sure to use the `-PwithByteBufTracking` flag:
```bash
./gradlew test -PwithByteBufTracking
```

### No tracking output appears

1. Verify the flag is used
2. Check the report file: `build/reports/bytebuf-tracking/[module-name].txt`
3. Verify ByteBufs are actually used in your tests

### Build failures

```bash
# Refresh dependencies
./gradlew test -PwithByteBufTracking --refresh-dependencies

# Clean rebuild
./gradlew :bytebuf-tracker:clean :bytebuf-tracker:agentJar -PwithByteBufTracking
```

### ClassNotFoundException for tracker classes

The agent JAR may be incomplete. Clean and rebuild:
```bash
./gradlew :bytebuf-tracker:clean :bytebuf-tracker:agentJar -PwithByteBufTracking
```

## Documentation

- **CLAUDE_CODE_INTEGRATION.md** - Integration guide for Claude Code projects
- **MIGRATION_GUIDE.md** - Project restructuring history
- **UPSTREAM_README.md** - Full upstream documentation
- **QUICKSTART.md** - Quick reference guide
- **[Parent Integration Docs](../BYTEBUF_TRACKER_INTEGRATION.md)** - Complete Ambry integration guide

## External Resources

- [Upstream Repository](https://github.com/j-tyler/bytebuddy-bytebuf-tracer)
- [ByteBuddy Documentation](https://bytebuddy.net/)
- [Netty ByteBuf Reference Counting](https://netty.io/wiki/reference-counted-objects.html)
- [Java Agents Tutorial](https://www.baeldung.com/java-instrumentation)

## License

Apache License 2.0

The ByteBuf Flow Tracker integration follows Ambry's Apache License 2.0.
The original tracker project is also Apache License 2.0.

---

**Last Updated**: November 2025 - Synced with upstream master branch
