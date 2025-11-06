# ByteBuf Flow Tracking in Ambry

Ambry includes integrated ByteBuf flow tracking to help identify and diagnose memory leaks during test execution.

## Quick Start

### Build Everything (Including ByteBuf Tracker)

```bash
./gradlew allJar -PwithByteBufTracking
```

This builds the entire Ambry project **and** the ByteBuf tracker agent.

### Run Tests With ByteBuf Tracking

```bash
# Run all tests with tracking
./gradlew test -PwithByteBufTracking

# Run specific module tests
./gradlew :ambry-commons:test -PwithByteBufTracking

# Run integration tests
./gradlew intTest -PwithByteBufTracking

# Run all tests
./gradlew allTest -PwithByteBufTracking
```

### What You'll See

At the end of test execution, you'll see a comprehensive ByteBuf flow report:

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

- **⚠️ LEAK** = ByteBuf was not properly released (refCount > 0 at end)
- **ref=N** = Reference count at this point in the flow
- **count=N** = Number of times this path was traversed

## Run Tests WITHOUT Tracking

Simply omit the flag:

```bash
./gradlew test
```

Tests run normally without any tracking overhead.

## How It Works

When you use `-PwithByteBufTracking`:

1. **Gradle builds the tracker agent** from the `bytebuf-tracker` module
2. **Creates a fat JAR** with ByteBuddy and all dependencies
3. **Automatically attaches the agent** to test JVMs via `-javaagent`
4. **Tracks all ByteBuf flows** through `com.github.ambry` packages
5. **Prints a report** at the end of each test module

## Alternative Property Names

All of these work:

```bash
./gradlew test -PwithByteBufTracking
./gradlew test -PenableByteBufTracking
./gradlew test -DenableByteBufTracking=true
```

## Configuration

### Track Additional Packages

Edit `build.gradle` in the root, find the tracker configuration, and change:

```groovy
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry"
```

To:

```groovy
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry,io.netty.buffer"
```

### Exclude Packages

```groovy
jvmArgs "-javaagent:${trackerJar.absolutePath}=include=com.github.ambry;exclude=com.github.ambry.test"
```

## Troubleshooting

### "ByteBuf tracker agent JAR not found"

The agent JAR wasn't built. Make sure to use `-PwithByteBufTracking`:

```bash
./gradlew :bytebuf-tracker:agentJar -PwithByteBufTracking
```

### No Tracking Output

Verify the tracker is enabled:

```bash
./gradlew test -PwithByteBufTracking --info | grep "ByteBuf"
```

You should see: "Using ByteBuf Flow Tracker agent: ..."

### Build Failures

If you get dependency download errors:

```bash
# Retry with --refresh-dependencies
./gradlew test -PwithByteBufTracking --refresh-dependencies
```

## Under the Hood

The tracker consists of:

- **bytebuf-tracker/** - Gradle submodule with tracker source code
- **ByteBufTrackerListener.java** - JUnit listener that prints reports
- **build.gradle** - Conditional agent configuration

The tracker uses ByteBuddy to instrument methods at JVM startup, tracking ByteBuf objects through a Trie data structure with zero allocation overhead.

## More Information

- **BYTEBUF_TRACKER_INTEGRATION.md** - Complete integration documentation
- **bytebuf-tracker/QUICKSTART.md** - Quick start guide
- **modules/bytebuddy-bytebuf-tracer/** - Original tracker project (git submodule)

## Need Help?

1. Check if the agent JAR was built:
   ```bash
   ls -lh bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
   ```

2. Build it explicitly:
   ```bash
   ./gradlew :bytebuf-tracker:agentJar -PwithByteBufTracking
   ```

3. Run with verbose logging:
   ```bash
   ./gradlew test -PwithByteBufTracking --info
   ```
