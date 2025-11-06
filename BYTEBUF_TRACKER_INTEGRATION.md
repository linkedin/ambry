# ByteBuf Flow Tracker Integration

This document describes the integration of the ByteBuddy ByteBuf Flow Tracker into the Ambry codebase for detecting and analyzing ByteBuf memory leaks during test runs.

## Overview

The ByteBuf Flow Tracker is a lightweight Java agent that tracks Netty ByteBuf object flows through the application using a Trie data structure. It helps identify memory leaks by monitoring ByteBuf reference counts and detecting unreleased buffers.

**Key Features:**
- Zero allocation overhead - no stack trace collection
- First-touch root approach - initial handler becomes the Trie root
- Memory efficient - shared Trie prefixes minimize storage
- Dual output formats - human-readable trees and LLM-optimized structured text
- JMX integration - runtime monitoring via MBean
- **Gradle-native** - fully integrated into Ambry's build system
- **Constructor tracking** - tracks ByteBufs passed to constructors and stored in wrapped objects (NEW!)

## Quick Start

### Enable ByteBuf Tracking

Simply add `-PwithByteBufTracking` to any Gradle command:

```bash
# Build with tracking enabled
./gradlew allJar -PwithByteBufTracking

# Run tests with tracking
./gradlew test -PwithByteBufTracking

# Run integration tests with tracking
./gradlew intTest -PwithByteBufTracking
```

**That's it!** Gradle automatically:
1. Builds the tracker agent JAR (if needed)
2. Attaches it to test JVMs
3. Instruments your code
4. Prints a report at the end

### Run Without Tracking (Normal Operation)

Simply omit the flag:

```bash
./gradlew test
```

Tests run normally with no overhead.

## Integration Components

### 1. Git Submodule
- **Location**: `modules/bytebuddy-bytebuf-tracer`
- **Repository**: https://github.com/j-tyler/bytebuddy-bytebuf-tracer
- **Purpose**: Contains the original tracker source code

### 2. Bytebuf-Tracker Subproject
- **Location**: `bytebuf-tracker/`
- **Purpose**: Ambry-specific build of the tracker agent
- **Dependencies**: Reuses Ambry's existing ByteBuddy (1.14.9) and Netty dependencies
- **Output**: `bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar` (fat JAR with all dependencies)
- **Java Compatibility**: Java 8+ (matches Ambry's target)

### 3. Test Listener
- **File**: `ambry-test-utils/src/main/java/com/github/ambry/testutils/ByteBufTrackerListener.java`
- **Purpose**: JUnit RunListener that prints tracker output at the end of test execution
- **Behavior**:
  - Uses reflection to detect if tracker agent is running
  - Prints summary statistics and detailed flow trees
  - Highlights memory leaks with ⚠️ warnings
  - Gracefully handles case when agent is not present

### 4. Build Configuration
- **File**: `build.gradle` (root)
- **Key Changes**:
  - Test tasks check for `withByteBufTracking` property
  - When enabled, automatically builds `:bytebuf-tracker:agentJar`
  - Adds `-javaagent` JVM argument with agent path
  - Registers `ByteBufTrackerListener` for output
  - Configuration applies to both `test` and `intTest` tasks

## How It Works

When you use `-PwithByteBufTracking`:

1. **Gradle detects the property** in `build.gradle`
2. **Builds the agent JAR** by executing `:bytebuf-tracker:agentJar` task
3. **Creates fat JAR** with ByteBuddy, Netty, and all dependencies
4. **Attaches agent** to test JVM via `-javaagent:path/to/agent.jar=include=com.github.ambry`
5. **ByteBuddy instruments** all methods in `com.github.ambry` packages at JVM startup
6. **Tracks ByteBuf flows** using a Trie data structure (zero allocation)
7. **Test listener** retrieves tracking data and prints report at test completion

## Understanding the Output

At the end of test execution, you'll see:

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

- **⚠️ LEAK**: ByteBuf was not released (refCount > 0 at leaf node)
- **ref=N**: Current reference count at this point in the flow
- **count=N**: Number of times this path was traversed
- **FINAL REF=N**: Reference count at the end of the flow (should be 0)
- **ROOT**: The first method that handled the ByteBuf

### Identifying Leaks

Look for:
1. **Leaf nodes with ⚠️ LEAK** - These are the final methods holding unreleased ByteBufs
2. **FINAL REF > 0** - Indicates the ByteBuf wasn't fully released
3. **Paths in "Flat Paths" section** - Shows the complete journey of leaked ByteBufs

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

### Alternative Property Names

All of these work:

```bash
./gradlew test -PwithByteBufTracking
./gradlew test -PenableByteBufTracking
./gradlew test -DenableByteBufTracking=true
```

### JMX Monitoring

Enable JMX to monitor ByteBuf flows in real-time. Add to `build.gradle`:

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

**JMX Operations:**
- `getTreeView()` - Visual tree representation
- `getFlatView()` - Flat path list with leaks
- `getCsvView()` - CSV export
- `getJsonView()` - JSON export
- `getSummary()` - Statistics summary
- `reset()` - Clear all tracking data

## Troubleshooting

### "ByteBuf tracker agent JAR not found"

**Symptom**: Test fails with error about missing agent JAR

**Solution**: Make sure to use the `-PwithByteBufTracking` flag:
```bash
./gradlew test -PwithByteBufTracking
```

The flag tells Gradle to build the agent JAR before running tests.

### No Tracker Output Appears

**Symptom**: Tests run but no ByteBuf flow report is printed

**Solutions**:
1. Verify you used the flag: `-PwithByteBufTracking`
2. Check agent was loaded:
   ```bash
   ./gradlew test -PwithByteBufTracking --info | grep "ByteBuf"
   ```
   You should see: `Using ByteBuf Flow Tracker agent: ...`
3. Verify ByteBufs are actually used in your tests

### Build Failures

**Symptom**: Build fails when trying to compile bytebuf-tracker

**Common Causes & Solutions**:

1. **Network connectivity issues** (dependencies can't download):
   ```bash
   ./gradlew test -PwithByteBufTracking --refresh-dependencies
   ```

2. **Gradle daemon issues**:
   ```bash
   ./gradlew --stop
   ./gradlew test -PwithByteBufTracking --no-daemon
   ```

3. **Java version compatibility**:
   - Ambry targets Java 8
   - Ensure you're using Java 8, 11, or 17
   - Check version: `java -version`

### No Tracking Data for Specific Classes

**Symptom**: Some classes don't appear in tracking output

**Solution**:
1. Verify the package is included in the `include` pattern
2. Check the class isn't in an excluded package
3. Verify methods are `public` or `protected` (private methods aren't instrumented)

### Performance Impact

**Symptom**: Tests run significantly slower with tracker enabled

**Solutions**:
1. **Narrow package scope** - Only track critical packages
2. **Run selectively** - Use tracker only on suspected leak areas
3. **Exclude test utilities** - Don't track test helper classes
4. **Use on subset** - Run tracker on specific test classes:
   ```bash
   ./gradlew :ambry-commons:test -PwithByteBufTracking --tests SuspectedLeakyTest
   ```

### Class Not Found Errors

**Symptom**: Tests fail with `ClassNotFoundException` for tracker classes

**Cause**: Agent JAR may be incomplete

**Solution**: Clean and rebuild:
```bash
./gradlew :bytebuf-tracker:clean :bytebuf-tracker:agentJar -PwithByteBufTracking
```

## Architecture & Implementation

### Why This Approach?

1. **Zero code changes** - Tracker added via Java agent (bytecode instrumentation)
2. **Gradle-native** - Integrates cleanly with existing Ambry build
3. **Opt-in by default** - No overhead unless explicitly enabled
4. **Comprehensive** - Tracks all ByteBuf flows automatically
5. **Production-safe** - Only active during tests, never in production
6. **Java 8 compatible** - Matches Ambry's target version

### Component Flow

```
User runs: ./gradlew test -PwithByteBufTracking
       ↓
Gradle detects withByteBufTracking property
       ↓
Builds bytebuf-tracker:agentJar (if needed)
       ↓
Test JVM starts with -javaagent
       ↓
ByteBuddy instruments com.github.ambry classes
       ↓
Tests run - ByteBuf flows tracked in Trie
       ↓
ByteBufTrackerListener prints report
```

### Key Files

- `build.gradle` - Root build file with tracker configuration
- `bytebuf-tracker/build.gradle` - Tracker module build configuration
- `bytebuf-tracker/src/main/java/` - Tracker source code (from submodule)
- `ambry-test-utils/.../ByteBufTrackerListener.java` - Test integration
- `modules/bytebuddy-bytebuf-tracer/` - Original tracker project (git submodule)

### Source Code Organization

```
bytebuf-tracker/src/main/java/
├── com/example/bytebuf/tracker/
│   ├── ByteBufFlowTracker.java        # Main tracking logic
│   ├── ByteBufObjectHandler.java      # ByteBuf-specific handler
│   ├── ObjectTrackerHandler.java      # Interface for custom objects
│   ├── ObjectTrackerRegistry.java     # Handler registry
│   ├── agent/
│   │   ├── ByteBufFlowAgent.java      # Java agent entry point
│   │   ├── ByteBufFlowMBean.java      # JMX interface
│   │   └── ByteBufTrackingAdvice.java # ByteBuddy advice
│   ├── trie/
│   │   └── FlowTrie.java              # Trie data structure
│   └── view/
│       └── TrieRenderer.java          # Output formatting
```

## Comparison with Netty's Leak Detection

| Feature | Netty Leak Detector | ByteBuf Flow Tracker |
|---------|---------------------|----------------------|
| **Approach** | Sampling-based | Complete tracking |
| **Coverage** | Random sample | All ByteBufs |
| **Call paths** | Stack traces | Flow tree (Trie) |
| **Memory usage** | High (stack traces) | Low (shared Trie) |
| **Output** | Console warnings | Structured report |
| **Root cause** | Allocation site | Flow path |
| **False positives** | Possible | Minimal |
| **Performance impact** | Low-medium | Low |

**When to use each:**
- **Netty**: Production monitoring, broad coverage
- **ByteBuf Tracker**: Deep analysis, debugging specific leaks, understanding flow patterns

## Best Practices

### During Development

1. **Run tests with tracker periodically**:
   ```bash
   ./gradlew test -PwithByteBufTracking
   ```

2. **Fix leaks immediately** - Don't accumulate technical debt

3. **Use narrow package scope** for faster iteration:
   ```groovy
   // In build.gradle, modify to track only your module
   jvmArgs "-javaagent:...=include=com.github.ambry.mymodule"
   ```

### In CI/CD

1. **Run selectively** - Not on every build (performance)
2. **Schedule nightly** - Deep leak analysis overnight
3. **Fail on leaks** - Configure to fail build if leaks detected
4. **Archive reports** - Save output for trend analysis

### Debugging Leaks

1. **Start broad** - Track entire package
2. **Identify leak paths** - Look for ⚠️ LEAK markers
3. **Narrow scope** - Focus on specific classes
4. **Examine flow tree** - Understand ByteBuf journey
5. **Fix root cause** - Not just symptoms

## Migration from Shell Scripts

**Old approach (deprecated):**
```bash
./build-bytebuf-tracker.sh  # Build agent
./gradlew test              # Run tests (auto-detected)
```

**New approach (recommended):**
```bash
./gradlew test -PwithByteBufTracking  # One command!
```

The shell scripts (`build-bytebuf-tracker.sh`, `build-bytebuf-tracker-maven.sh`) are kept for backward compatibility but are no longer needed.

## Related Documentation

- **CONSTRUCTOR_TRACKING.md** - Comprehensive guide to constructor tracking for wrapped objects (⭐ NEW!)
- **README-BYTEBUF-TRACKING.md** - Simple usage guide
- **bytebuf-tracker/QUICKSTART.md** - Quick reference
- **GRADLE-INTEGRATION-SUMMARY.md** - Migration guide from shell scripts
- **modules/bytebuddy-bytebuf-tracer/README.md** - Original tracker documentation

## External Resources

- [ByteBuddy Documentation](https://bytebuddy.net/)
- [Netty ByteBuf Reference Counting](https://netty.io/wiki/reference-counted-objects.html)
- [Java Agents Tutorial](https://www.baeldung.com/java-instrumentation)
- [Original ByteBuf Flow Tracker](https://github.com/j-tyler/bytebuddy-bytebuf-tracer)

## Support & Contributing

### Getting Help

1. Check troubleshooting section above
2. Verify agent JAR exists: `ls -lh bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar`
3. Run with verbose logging: `./gradlew test -PwithByteBufTracking --info`
4. Review recent commits for similar issues

### Reporting Issues

When reporting issues, include:
- Command used
- Error message
- Java version: `java -version`
- Gradle version: `./gradlew --version`
- Whether agent JAR was built successfully

### Future Enhancements

Potential improvements:
- Automatic leak threshold enforcement in CI
- HTML report generation with visual flow diagrams
- Per-test leak isolation and reporting
- Historical trend tracking
- Custom object tracking beyond ByteBuf
- Integration with other memory analysis tools

## License

The ByteBuf Flow Tracker integration follows Ambry's Apache License 2.0.
The original tracker project is also Apache License 2.0.

---

**Quick Links:**
- [Quick Start](#quick-start)
- [Troubleshooting](#troubleshooting)
- [Advanced Configuration](#advanced-configuration)
- [Related Documentation](#related-documentation)
