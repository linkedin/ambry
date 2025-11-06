# ByteBuf Flow Tracker Library

A lightweight, efficient ByteBuddy-based Java agent for tracking ByteBuf flows through your application using a Trie data structure.

## Features

- **Zero allocation overhead**: No stack trace collection or allocation site tracking
- **First-touch root**: The first method that handles a ByteBuf becomes the Trie root
- **Memory efficient**: Trie structure shares common prefixes, minimizing memory usage
- **Real-time monitoring**: JMX MBean for runtime analysis
- **Multiple output formats**: Tree, flat paths, CSV, JSON
- **Leak detection**: Identifies ByteBufs not properly released

## Building the Library

```bash
mvn clean package
```

This produces two JARs:
- `bytebuf-flow-tracker-1.0.0-SNAPSHOT.jar` - Library JAR (for Maven dependencies)
- `bytebuf-flow-tracker-1.0.0-SNAPSHOT-agent.jar` - Fat JAR with all dependencies (for Java agent usage)

## Using as a Dependency

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>com.example.bytebuf</groupId>
    <artifactId>bytebuf-flow-tracker</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Running as a Java Agent

### Option 1: Command Line

```bash
java -javaagent:bytebuf-flow-tracker-1.0.0-SNAPSHOT-agent.jar=include=com.example \
     -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9999 \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -Dcom.sun.management.jmxremote.ssl=false \
     -jar your-application.jar
```

### Option 2: Maven Exec Plugin

Configure in your `pom.xml`:

```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <version>3.1.0</version>
    <configuration>
        <mainClass>com.example.YourMainClass</mainClass>
        <arguments>
            <argument>-javaagent:${project.basedir}/path/to/bytebuf-flow-tracker-agent.jar=include=com.example</argument>
        </arguments>
    </configuration>
</plugin>
```

## Agent Configuration

Agent arguments format: `include=package1,package2;exclude=package3,package4`

Examples:
- Track everything in com.example: `include=com.example`
- Track multiple packages: `include=com.example,com.myapp,org.custom`
- Exclude legacy code: `include=com.example;exclude=com.example.legacy`

## JMX Monitoring

Connect to the JMX MBean at `com.example:type=ByteBufFlowTracker`

Available operations:
- `getTreeView()` - Hierarchical tree view
- `getFlatView()` - Flat root-to-leaf paths
- `getCsvView()` - CSV format for analysis
- `getJsonView()` - JSON for programmatic processing
- `getSummary()` - Statistics and summary
- `exportToFile(filepath, format)` - Export to file
- `reset()` - Clear all tracking data

## Programmatic Usage

```java
import com.example.bytebuf.tracker.ByteBufFlowTracker;
import com.example.bytebuf.tracker.view.TrieRenderer;

// Get tracker instance
ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();

// Manually record ByteBuf method calls
tracker.recordMethodCall(byteBuf, "MyClass", "myMethod", byteBuf.refCnt());

// Analyze results
TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
String treeView = renderer.renderIndentedTree();
System.out.println(treeView);
```

## Architecture

### Core Components

- **FlowTrie**: Pure data structure for storing method call paths
- **ByteBufFlowTracker**: Main tracking logic using first-touch-as-root approach
- **ByteBufFlowAgent**: Java agent entry point for ByteBuddy instrumentation
- **ByteBufTrackingAdvice**: ByteBuddy advice that intercepts methods
- **TrieRenderer**: Formats Trie data into various output views
- **ByteBufFlowMBean**: JMX interface for runtime monitoring

### How It Works

1. **ByteBuddy Instrumentation**: Intercepts all public/protected methods in specified packages
2. **First Touch = Root**: First method to handle a ByteBuf becomes the Trie root for that object
3. **Path Building**: Each subsequent method call adds a node to the tree
4. **RefCount Tracking**: Each node records the ByteBuf's reference count
5. **Leak Detection**: When refCount reaches 0, the flow is complete; non-zero leaf nodes indicate leaks

## Testing

Run the included tests:

```bash
mvn test
```

The tests demonstrate:
- Simple flow tracking
- Leak detection
- RefCount anomaly detection
- High-volume tracking
- CSV export

## Extending for Custom Objects

While designed for ByteBuf, you can track any object by:

1. Modify `ByteBufTrackingAdvice` to detect your objects
2. Extract appropriate "refCount" equivalent (or use a different metric)
3. The Trie structure and rendering remain the same

See the example module for a complete integration example.

## License

Apache License 2.0
