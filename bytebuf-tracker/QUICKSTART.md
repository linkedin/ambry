# ByteBuf Tracker Quick Start

## The Problem
The main build script (`build-bytebuf-tracker.sh`) may fail due to Java/Gradle version incompatibility.

## Quick Solution

Use the Maven-based build script instead:

```bash
./build-bytebuf-tracker-maven.sh
```

This script:
1. Builds the tracker from the submodule using Maven (more compatible)
2. Copies the agent JAR to the correct location
3. Requires Maven to be installed

## Install Maven (if needed)

**macOS:**
```bash
brew install maven
```

**Ubuntu/Debian:**
```bash
sudo apt-get install maven
```

**Other systems:**
Download from https://maven.apache.org/download.cgi

## Manual Build (if scripts don't work)

```bash
cd modules/bytebuddy-bytebuf-tracer
mvn clean install -DskipTests
cd ../..
mkdir -p bytebuf-tracker/build/libs
cp modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/target/bytebuf-flow-tracker-*-agent.jar \
   bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
```

## Verify Installation

After building, verify the JAR exists:

```bash
ls -lh bytebuf-tracker/build/libs/bytebuf-tracker-agent.jar
```

You should see a file around 5-10 MB.

## Run Tests with Tracking

Once the JAR is built, run tests normally:

```bash
./gradlew test
```

The ByteBuf flow report will be printed at the end of test execution.

## Run Tests WITHOUT Tracking

If you can't build the agent JAR, tests still work normally:

```bash
./gradlew test
```

The tracker is completely optional. Tests run the same with or without it.

## More Info

See `BYTEBUF_TRACKER_INTEGRATION.md` for complete documentation.
