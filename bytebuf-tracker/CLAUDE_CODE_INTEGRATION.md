# Integration Guide for Claude Code Projects

This guide explains how to integrate the ByteBuf Flow Tracker into another project when using Claude Code, where the tracker is not available on Maven Central.

## Recommended Strategy: Multi-Module Build from Source

Since this project isn't published to Maven, and Java agents require a fat JAR with all dependencies, the best approach is to **include this project as a module in your multi-module Maven build**.

### Why This Strategy Works Best

1. **Claude Code builds Maven projects from source** - No need for pre-built JARs
2. **Maven Shade plugin creates the fat JAR** - Automatically includes all dependencies
3. **Single `mvn install` builds everything** - One command builds your project + the tracker
4. **No binary JARs in git** - Everything builds from source (clean repository)
5. **Easy updates** - Pull latest tracker source and rebuild

### How Java Agents Work (Important Context)

The `-javaagent` argument requires:
- A **single JAR file** (not a Maven dependency)
- **All dependencies included** (ByteBuddy, etc.)
- **Available at JVM startup** (before application classpath loads)

This is why we need the `-agent.jar` (fat JAR), not the regular JAR.

---

## Step-by-Step Integration for Claude Code

### Step 1: Add ByteBuf Flow Tracker as a Module

From your project root:

```bash
# Option A: Git submodule (recommended - allows updates)
git submodule add https://github.com/j-tyler/bytebuddy-bytebuf-tracer.git modules/bytebuddy-bytebuf-tracer

# Option B: Copy the directory directly
cp -r /path/to/bytebuddy-bytebuf-tracer modules/
```

Your project structure:

```
your-project/
├── pom.xml                          # Your parent POM
├── your-app/                        # Your application module
│   ├── pom.xml
│   └── src/
├── modules/
│   └── bytebuddy-bytebuf-tracer/    # Tracker project
│       ├── pom.xml                  # Tracker parent POM
│       ├── bytebuf-flow-tracker/    # The library module we need
│       └── bytebuf-flow-example/    # Can ignore this
```

### Step 2: Update Your Parent POM

Edit `your-project/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.yourcompany</groupId>
    <artifactId>your-project-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <modules>
        <!-- Your existing modules -->
        <module>your-app</module>

        <!-- Add the ByteBuf tracker module -->
        <module>modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker</module>
    </modules>

    <!-- Optional: Add dependency management for the tracker -->
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>com.example.bytebuf</groupId>
                <artifactId>bytebuf-flow-tracker</artifactId>
                <version>1.0.0-SNAPSHOT</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

**Note**: We only include `bytebuf-flow-tracker` module, not the entire parent project, to avoid conflicts.

### Step 3: Add Dependency to Your Application Module

Edit `your-project/your-app/pom.xml`:

```xml
<dependencies>
    <!-- Your existing dependencies -->

    <!-- ByteBuf Flow Tracker -->
    <dependency>
        <groupId>com.example.bytebuf</groupId>
        <artifactId>bytebuf-flow-tracker</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Step 4: Configure the Java Agent in Your Application Module

Edit `your-project/your-app/pom.xml`:

```xml
<build>
    <plugins>
        <!-- For running during development -->
        <plugin>
            <groupId>org.codehaus.mojo</groupId>
            <artifactId>exec-maven-plugin</artifactId>
            <version>3.1.0</version>
            <configuration>
                <mainClass>com.yourcompany.yourapp.Main</mainClass>
                <arguments>
                    <!-- Reference the fat JAR in the tracker module's target directory -->
                    <argument>-javaagent:${project.parent.basedir}/modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/target/bytebuf-flow-tracker-1.0.0-SNAPSHOT-agent.jar=include=com.yourcompany</argument>
                </arguments>
                <systemProperties>
                    <systemProperty>
                        <key>com.sun.management.jmxremote</key>
                        <value>true</value>
                    </systemProperty>
                    <systemProperty>
                        <key>com.sun.management.jmxremote.port</key>
                        <value>9999</value>
                    </systemProperty>
                    <systemProperty>
                        <key>com.sun.management.jmxremote.authenticate</key>
                        <value>false</value>
                    </systemProperty>
                    <systemProperty>
                        <key>com.sun.management.jmxremote.ssl</key>
                        <value>false</value>
                    </systemProperty>
                </systemProperties>
            </configuration>
        </plugin>

        <!-- For running during tests -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>3.0.0</version>
            <configuration>
                <argLine>
                    -javaagent:${project.parent.basedir}/modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/target/bytebuf-flow-tracker-1.0.0-SNAPSHOT-agent.jar=include=com.yourcompany
                </argLine>
            </configuration>
        </plugin>
    </plugins>
</build>
```

### Step 5: Build Everything

From your project root:

```bash
# This builds your project AND the tracker module in one command
mvn clean install
```

This will:
1. Build the `bytebuf-flow-tracker` module first
2. Create both JARs:
   - `bytebuf-flow-tracker-1.0.0-SNAPSHOT.jar` (regular)
   - `bytebuf-flow-tracker-1.0.0-SNAPSHOT-agent.jar` (fat JAR with dependencies)
3. Build your application module
4. Install everything to your local Maven repository

### Step 6: Run Your Application

```bash
cd your-app
mvn exec:java
```

Or run tests with the agent:

```bash
mvn test
```

---

## Claude Code Workflow

When Claude Code integrates this project, it should:

### 1. Initial Setup (One Time)

```bash
# Add as submodule
cd /path/to/target-project
git submodule add https://github.com/j-tyler/bytebuddy-bytebuf-tracer.git modules/bytebuddy-bytebuf-tracer
git submodule update --init --recursive

# Update parent POM to include the tracker module
# (Edit pom.xml to add module reference)

# Build everything
mvn clean install
```

### 2. Development Workflow

```bash
# Normal development - just build as usual
mvn clean install

# Run application with tracker
cd your-app
mvn exec:java

# Run tests with tracker
mvn test
```

### 3. Updating the Tracker

```bash
# Pull latest tracker code
cd modules/bytebuddy-bytebuf-tracer
git pull origin main

# Rebuild everything
cd ../..
mvn clean install
```

---

## Alternative: Copy Just the bytebuf-flow-tracker Module

If you want to avoid submodules, you can copy just the library module:

```bash
# Copy only the tracker library (not the example or parent)
cp -r /path/to/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker your-project/modules/

# Then follow steps 2-6 above
```

This is simpler but means manual updates (copy files again to update).

---

## Why NOT Check in the Fat JAR?

You might be tempted to just build the fat JAR once and check it into git. **Don't do this:**

### Problems with Checking in Binaries:
1. **Large files in git** - Fat JARs are 5-10 MB, bloats repository
2. **Merge conflicts** - Binary files cause conflicts
3. **Stale JARs** - Easy to forget to rebuild when source changes
4. **No visibility** - Can't see what changed in the library
5. **Version mismatches** - JAR might not match source code

### Benefits of Building from Source:
1. **Clean git history** - Only source code tracked
2. **Always current** - Rebuilding ensures latest version
3. **Transparent** - Can see exactly what's in the tracker
4. **Easy debugging** - Can modify tracker code and rebuild
5. **Maven best practices** - Multi-module builds are standard

---

## Troubleshooting

### Problem: "Could not find artifact com.example.bytebuf:bytebuf-flow-tracker"

**Cause**: Module not included in parent POM

**Solution**:
```xml
<!-- In parent pom.xml -->
<modules>
    <module>modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker</module>
</modules>
```

### Problem: "Could not find or load Java agent"

**Cause**: Agent JAR path is wrong or wasn't built

**Solution**:
```bash
# Build from project root first
mvn clean install

# Verify the agent JAR exists
ls modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker/target/*-agent.jar
```

### Problem: "Module hierarchy is inconsistent"

**Cause**: The tracker's parent POM conflicts with your parent POM

**Solution**: Only include the `bytebuf-flow-tracker` module, not the tracker's parent:
```xml
<!-- Correct: -->
<module>modules/bytebuddy-bytebuf-tracer/bytebuf-flow-tracker</module>

<!-- Wrong (causes conflicts): -->
<module>modules/bytebuddy-bytebuf-tracer</module>
```

### Problem: Network issues during Maven build

**Cause**: Maven can't download ByteBuddy/Netty dependencies

**Solution**:
```bash
# Retry with forced updates
mvn clean install -U

# Or use offline mode if dependencies are cached
mvn clean install -o
```

---

## Summary for Claude Code

When integrating this tracker into another Claude Code project:

1. ✅ **Use multi-module Maven build** (include tracker as a module)
2. ✅ **Build from source** (`mvn clean install` builds everything)
3. ✅ **Reference the fat JAR** in target directory for `-javaagent`
4. ❌ **Don't check in the fat JAR** to git
5. ❌ **Don't try to use Maven Central** (project isn't published there)

The fat JAR is **required** for the Java agent, but it's **generated automatically** by Maven Shade plugin during the build. Claude Code just needs to run `mvn clean install` and reference the generated JAR path.

---

## Complete Example Directory Structure

After integration, your project looks like:

```
your-project/
├── pom.xml                                              # Parent POM (includes tracker module)
├── your-app/
│   ├── pom.xml                                          # Depends on bytebuf-flow-tracker
│   └── src/main/java/com/yourcompany/yourapp/
│       └── Main.java
├── modules/
│   └── bytebuddy-bytebuf-tracer/
│       └── bytebuf-flow-tracker/
│           ├── pom.xml                                  # Builds regular + fat JAR
│           ├── src/main/java/...                        # Tracker source code
│           └── target/
│               ├── bytebuf-flow-tracker-1.0.0-SNAPSHOT.jar          # Regular JAR
│               └── bytebuf-flow-tracker-1.0.0-SNAPSHOT-agent.jar    # Fat JAR (for -javaagent)
```

Run `mvn clean install` from root → Everything builds → Fat JAR is in target directory → Reference it with `-javaagent` ✅
