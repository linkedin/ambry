# ByteBuf Tracker Quick Start

## TL;DR - Just Use Gradle

```bash
# Build with tracking enabled
./gradlew allJar -PwithByteBufTracking

# Run tests with tracking
./gradlew test -PwithByteBufTracking

# That's it! 🎉
```

## What This Does

When you use `-PwithByteBufTracking`:

1. **Gradle automatically builds the tracker agent** (no separate build needed!)
2. **Tests run with ByteBuf flow tracking enabled**
3. **A detailed report prints at the end** showing all ByteBuf flows and leaks

## Example Output

```
================================================================================
ByteBuf Flow Tracker Report
================================================================================
=== ByteBuf Flow Summary ===
Total Root Methods: 15
Total Traversals: 342
Unique Paths: 28
Leak Paths: 2

ROOT: LeakyHandler.handleRequest [count=3]
└── ErrorHandler.logError [ref=1, count=3] ⚠️ LEAK
```

The ⚠️ shows where ByteBufs weren't properly released!

## Run Without Tracking

Just omit the flag:

```bash
./gradlew test
```

Tests run normally without any overhead.

## Available Commands

```bash
# Build everything with tracking
./gradlew allJar -PwithByteBufTracking

# Run unit tests with tracking
./gradlew test -PwithByteBufTracking

# Run integration tests with tracking
./gradlew intTest -PwithByteBufTracking

# Run all tests with tracking
./gradlew allTest -PwithByteBufTracking

# Build just the agent (if needed)
./gradlew :bytebuf-tracker:agentJar -PwithByteBufTracking
```

## Troubleshooting

### "ByteBuf tracker agent JAR not found"

Make sure to include the flag:

```bash
./gradlew test -PwithByteBufTracking
```

### No output appears

Check that the tracker is enabled:

```bash
./gradlew test -PwithByteBufTracking --info | grep "ByteBuf"
```

You should see: "Using ByteBuf Flow Tracker agent: ..."

## Old Shell Script Approach (Deprecated)

The shell scripts (`build-bytebuf-tracker.sh`, `build-bytebuf-tracker-maven.sh`) are no longer needed. Gradle handles everything automatically when you use `-PwithByteBufTracking`.

## More Info

- **README-BYTEBUF-TRACKING.md** - Full usage guide
- **BYTEBUF_TRACKER_INTEGRATION.md** - Complete technical documentation
