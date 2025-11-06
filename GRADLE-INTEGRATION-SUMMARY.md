# ByteBuf Tracker - Gradle Integration Summary

## What Changed

The ByteBuf Flow Tracker is now **fully integrated into Ambry's Gradle build system**. No more shell scripts or separate build steps!

## New Usage

### Build with Tracking

```bash
./gradlew allJar -PwithByteBufTracking
```

### Run Tests with Tracking

```bash
# Unit tests
./gradlew test -PwithByteBufTracking

# Integration tests
./gradlew intTest -PwithByteBufTracking

# All tests
./gradlew allTest -PwithByteBufTracking

# Specific module
./gradlew :ambry-commons:test -PwithByteBufTracking
```

### Run Tests WITHOUT Tracking (Normal)

```bash
./gradlew test
```

## How It Works

1. **Add the flag**: `-PwithByteBufTracking`
2. **Gradle builds the agent**: Automatically creates the fat JAR with ByteBuddy
3. **Tests run with tracking**: Agent attaches via `-javaagent`
4. **Report prints at end**: See all ByteBuf flows and leaks

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

## Key Changes

### 1. `bytebuf-tracker/build.gradle`
- Checks for `withByteBufTracking` property
- Only builds agent JAR when tracking is enabled
- Creates fat JAR with all dependencies

### 2. `build.gradle` (root)
- Test tasks check for `withByteBufTracking` property
- Automatically adds agent to JVM args when enabled
- Ensures agent JAR is built before tests run

### 3. New Documentation
- `README-BYTEBUF-TRACKING.md` - Simple usage guide
- `bytebuf-tracker/QUICKSTART.md` - Updated for Gradle approach
- `BYTEBUF_TRACKER_INTEGRATION.md` - Still has full technical docs

## Benefits

✅ **No separate build step** - Gradle handles everything
✅ **Standard workflow** - Works like any Gradle command
✅ **Opt-in by default** - No overhead unless explicitly enabled
✅ **Clear and simple** - Just add one flag
✅ **Fully integrated** - Part of Ambry's build system

## Shell Scripts

The old shell scripts (`build-bytebuf-tracker.sh`, `build-bytebuf-tracker-maven.sh`) are deprecated but kept for backward compatibility. You don't need them anymore!

## Troubleshooting

### "ByteBuf tracker agent JAR not found"

Make sure to use the flag:
```bash
./gradlew test -PwithByteBufTracking
```

### No tracking output appears

Verify it's enabled:
```bash
./gradlew test -PwithByteBufTracking --info | grep "ByteBuf"
```

You should see: "Using ByteBuf Flow Tracker agent: ..."

### Build failures

If dependencies fail to download:
```bash
./gradlew test -PwithByteBufTracking --refresh-dependencies
```

## What's Different from Shell Scripts?

| Aspect | Old (Shell Scripts) | New (Gradle Native) |
|--------|---------------------|---------------------|
| Build agent | `./build-bytebuf-tracker.sh` | Automatic with flag |
| Run tests | `./gradlew test` | `./gradlew test -PwithByteBufTracking` |
| Dependencies | Manual Maven/Gradle | Gradle handles it |
| Integration | Separate step | Fully integrated |
| User experience | Multi-step | Single command |

## Migration Guide

If you previously used shell scripts:

**Before:**
```bash
./build-bytebuf-tracker-maven.sh  # Build the agent
./gradlew test                     # Run tests (auto-detected agent)
```

**After:**
```bash
./gradlew test -PwithByteBufTracking  # One command does both!
```

## Documentation

- **README-BYTEBUF-TRACKING.md** - Start here for simple usage
- **bytebuf-tracker/QUICKSTART.md** - Quick reference
- **BYTEBUF_TRACKER_INTEGRATION.md** - Complete technical documentation

## Commits

All changes pushed to: `claude/fix-ambry-memory-leak-011CUsJwgBKP7xSUAjL7wScZ`

Key commits:
1. Initial integration with ByteBuf tracker
2. Fixed build script for Java/Gradle compatibility
3. **Native Gradle integration** (this change)
4. Documentation updates

## Next Steps

Try it out:

```bash
git pull origin claude/fix-ambry-memory-leak-011CUsJwgBKP7xSUAjL7wScZ
./gradlew test -PwithByteBufTracking
```

The first run will build the agent JAR (one-time setup), then tests will run with full ByteBuf flow tracking!
