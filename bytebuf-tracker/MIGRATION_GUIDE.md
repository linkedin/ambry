# Migration Guide: Two-Module Architecture

This document explains the restructuring of the ByteBuf Flow Tracker project from a single-directory structure to a multi-module Maven project.

## What Changed

### Before: Single Directory
```
bytebuddy-bytebuf-tracer/
├── ByteBufFlowAgent.java
├── ByteBufFlowMBean.java
├── ByteBufFlowTracker.java
├── ByteBufFlowTrackerTest.java
├── ByteBufTrackingAdvice.java
├── FlowTrie.java
├── TrieRenderer.java
└── README.md
```

All files were in the root directory with no build system or packaging structure.

### After: Multi-Module Maven Project
```
bytebuddy-bytebuf-tracer/
├── pom.xml                          # Parent POM
├── README.md                        # Project overview
├── MIGRATION_GUIDE.md              # This file
├── bytebuf-flow-tracker/            # Module 1: Reusable Library
│   ├── pom.xml
│   ├── README.md
│   └── src/
│       ├── main/java/com/example/bytebuf/tracker/
│       │   ├── ByteBufFlowTracker.java
│       │   ├── agent/
│       │   │   ├── ByteBufFlowAgent.java
│       │   │   ├── ByteBufFlowMBean.java
│       │   │   └── ByteBufTrackingAdvice.java
│       │   ├── trie/
│       │   │   └── FlowTrie.java
│       │   └── view/
│       │       └── TrieRenderer.java
│       └── test/java/com/example/bytebuf/tracker/test/
│           └── ByteBufFlowTrackerTest.java
└── bytebuf-flow-example/            # Module 2: Example Application
    ├── pom.xml
    ├── README.md
    └── src/main/java/com/example/demo/
        ├── DemoApplication.java
        ├── MessageProcessor.java
        ├── ErrorHandler.java
        └── LeakyService.java
```

## Architectural Benefits

### 1. **Reusable Library Module** (`bytebuf-flow-tracker`)

**Purpose**: A standalone library that can be:
- Published to Maven repositories
- Used as a dependency in any project
- Versioned and released independently

**Features**:
- Proper Maven structure with dependencies managed
- Generates two JARs:
  - Regular JAR for Maven dependencies
  - Fat JAR (`-agent.jar`) with all dependencies for Java agent usage
- Includes comprehensive tests
- Clean package structure following Java conventions

**How to Use**:
```xml
<dependency>
    <groupId>com.example.bytebuf</groupId>
    <artifactId>bytebuf-flow-tracker</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. **Example Module** (`bytebuf-flow-example`)

**Purpose**: Demonstrates how external projects integrate the tracker

**Shows**:
- How to add the tracker as a Maven dependency
- How to configure the Java agent in pom.xml
- Example code patterns (normal usage, leaks, error handling)
- How to access tracking data programmatically
- How to run with JMX monitoring

**Benefits**:
- Clear template for integration
- Runnable demo showing the tracker in action
- Best practices for configuration

## Key Improvements

### 1. Separation of Concerns
- **Library**: Pure tracking functionality, reusable
- **Example**: Integration patterns, not part of the library

### 2. Proper Dependency Management
- Parent POM manages all dependency versions
- Each module declares only what it needs
- ByteBuddy and Netty versions centrally managed

### 3. Build System Integration
- Maven Shade Plugin creates fat JAR for easy deployment
- Proper MANIFEST.MF entries for Java agent
- Example shows how to integrate in other projects

### 4. Documentation Structure
- Main README: Project overview
- Library README: API documentation and architecture
- Example README: Integration guide
- This file: Migration explanation

## How External Projects Use This

### Step 1: Add Dependency

In your `pom.xml`:
```xml
<dependency>
    <groupId>com.example.bytebuf</groupId>
    <artifactId>bytebuf-flow-tracker</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Step 2: Configure Agent

Option A - Development (Exec Plugin):
```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <configuration>
        <mainClass>com.yourcompany.Main</mainClass>
        <commandlineArgs>
            -javaagent:path/to/bytebuf-flow-tracker-agent.jar=include=com.yourcompany
        </commandlineArgs>
    </configuration>
</plugin>
```

Option B - Testing (Surefire Plugin):
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <argLine>
            -javaagent:path/to/bytebuf-flow-tracker-agent.jar=include=com.yourcompany
        </argLine>
    </configuration>
</plugin>
```

Option C - Production (Command Line):
```bash
java -javaagent:bytebuf-flow-tracker-agent.jar=include=com.yourcompany \
     -jar your-application.jar
```

### Step 3: Access Results

Programmatically:
```java
ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();
TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
System.out.println(renderer.renderSummary());
```

Or via JMX:
```bash
jconsole localhost:9999
# Navigate to: com.example:type=ByteBufFlowTracker
```

## Building the Project

### Build Everything
```bash
mvn clean install
```

This builds:
1. Parent project (coordinates dependencies)
2. `bytebuf-flow-tracker` library
3. `bytebuf-flow-example` demo application

### Build Individual Modules
```bash
# Library only
cd bytebuf-flow-tracker
mvn clean package

# Example only (requires library to be built first)
cd bytebuf-flow-example
mvn clean package
```

### Run the Example
```bash
cd bytebuf-flow-example
mvn exec:java
```

## Customization for Your Objects

The library is designed for ByteBuf but can track any object:

1. **Fork/modify the library module**:
   - Edit `ByteBufTrackingAdvice.java`
   - Change object detection logic
   - Adjust metric tracking (refCount → your metric)

2. **The Trie structure stays the same**:
   - `FlowTrie.java` - No changes needed
   - `TrieRenderer.java` - Works with any flow data

3. **Build your custom version**:
   ```bash
   cd bytebuf-flow-tracker
   mvn clean install
   ```

4. **Use in your project** with your custom version number

## Migration Checklist

If you're migrating existing code using the old structure:

- [x] Project split into two modules
- [x] Source files moved to proper Maven structure
- [x] Package structure aligned (`com.example.bytebuf.tracker.*`)
- [x] Parent POM created with dependency management
- [x] Library POM configured with Shade plugin
- [x] Example POM shows integration pattern
- [x] Tests moved to library module
- [x] Documentation updated for all modules
- [x] Build generates both regular and agent JARs
- [x] Example demonstrates real-world usage

## Backward Compatibility

### Source Code
All source code is unchanged except:
- Package declarations (now match directory structure)
- Removed unused `ByteBufInterceptor` class from advice

### Functionality
All functionality remains the same:
- Same tracking algorithm
- Same Trie structure
- Same output formats
- Same JMX interface

### Usage
External projects can now:
- Use as a proper Maven dependency
- Get automatic dependency resolution
- Have cleaner integration
- Follow the example module patterns

## Publishing

To publish the library to Maven Central or a private repository:

```bash
cd bytebuf-flow-tracker
mvn clean deploy
```

Configure your `~/.m2/settings.xml` with repository credentials.

## Questions?

- Library API/architecture → See `bytebuf-flow-tracker/README.md`
- Integration examples → See `bytebuf-flow-example/README.md`
- Project overview → See main `README.md`
- This migration → You're reading it!
