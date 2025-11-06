# Constructor Tracking for Wrapped ByteBuf Objects

## Overview

**Constructor tracking** is a critical feature for detecting ByteBuf memory leaks in **wrapped objects** - classes that receive a ByteBuf in their constructor and store it as an instance field.

## The Problem: Flow Breaks Without Constructor Tracking

### Without Constructor Tracking ❌

```
allocate() -> prepareData() -> new Request(byteBuf) -> ???
                                     ↑
                            FLOW BREAKS HERE
                            Constructor not tracked!
```

When a ByteBuf is passed to a constructor, the default instrumentation **loses track** of it because:
1. Only public/protected **methods** are instrumented by default
2. **Constructors** are explicitly excluded (`not(isConstructor())`)
3. The ByteBuf "disappears" into the object's private field

**Result:** Memory leaks in wrapped objects go undetected!

### With Constructor Tracking ✅

```
allocate() -> prepareData() -> Request.<init>() -> process() -> release()
                                    ↑
                         NOW TRACKED! Continuous flow maintained
```

The flow continues unbroken through the constructor, allowing complete leak detection.

---

## How Constructor Tracking Works

### Configuration

Constructor tracking is **opt-in and selective**. You specify which classes should have their constructors tracked:

```groovy
jvmArgs "-javaagent:${trackerJar}=include=com.github.ambry;trackConstructors=Class1,Class2,Package.*"
```

### Ambry's Current Configuration

Located in `/home/user/ambry/build.gradle` (lines 185 and 247):

```groovy
trackConstructors=com.github.ambry.protocol.*,
                  com.github.ambry.network.Request,
                  com.github.ambry.network.Response,
                  com.github.ambry.network.Send,
                  com.github.ambry.router.GetBlobOperation,
                  com.github.ambry.rest.NettyRequest,
                  com.github.ambry.rest.NettyResponse
```

### What Gets Tracked

For each specified class:
- **All public/protected constructors** are instrumented
- ByteBuf parameters are tracked **on entry**
- Flow continues into the object's methods
- Leak detection works across the entire lifecycle

---

## Common Ambry Wrapper Classes

### 1. Protocol/Message Layer

**Pattern:** Request/Response objects that wrap ByteBuf payloads

```java
// Typical pattern in com.github.ambry.protocol.*
public class GetRequest {
    private final ByteBuf buffer;  // ← Stored as field!

    public GetRequest(ByteBuf buffer) {
        this.buffer = buffer;  // ← Without constructor tracking, flow breaks here
    }

    public void process() {
        // Use buffer...
    }
}
```

**Why track:** Requests/responses often pass through many layers before release.

### 2. Network Layer

**Pattern:** Network wrappers that hold buffers during transmission

```java
public class NetworkRequest {
    private final ByteBuf payload;

    public NetworkRequest(ByteBuf payload) {
        this.payload = payload;
    }
}
```

**Why track:** Network objects have complex lifecycles with async callbacks.

### 3. Router Layer

**Pattern:** Operation objects that accumulate ByteBuf chunks

```java
public class GetBlobOperation {
    private final List<ByteBuf> chunks;

    public GetBlobOperation(ByteBuf firstChunk) {
        this.chunks = new ArrayList<>();
        this.chunks.add(firstChunk);  // ← Easy to leak!
    }
}
```

**Why track:** Operations may accumulate buffers over time and fail to release on errors.

### 4. REST/Frontend Layer

**Pattern:** HTTP-like wrappers for Netty ByteBufs

```java
public class NettyRequest {
    private final ByteBuf body;

    public NettyRequest(HttpRequest httpRequest) {
        this.body = httpRequest.content();  // ← Wraps Netty's buffer
    }
}
```

**Why track:** Frontend objects bridge HTTP world with internal Ambry storage.

---

## Syntax and Wildcards

### Exact Class Names

Track specific classes:
```
trackConstructors=com.github.ambry.network.Request,com.github.ambry.protocol.GetRequest
```

### Wildcard Patterns

Track all classes in a package:
```
trackConstructors=com.github.ambry.protocol.*
```

This matches:
- `com.github.ambry.protocol.GetRequest`
- `com.github.ambry.protocol.PutRequest`
- `com.github.ambry.protocol.DeleteRequest`
- ... and all other classes in that package

### Multiple Patterns

Combine exact names and wildcards:
```
trackConstructors=com.github.ambry.protocol.*,com.github.ambry.network.Request,com.github.ambry.router.*
```

---

## Example: Leak Detection With Constructor Tracking

### Scenario: Request Object Leak

**Code:**
```java
// Allocate ByteBuf
ByteBuf buffer = Unpooled.buffer(1024);
buffer.writeBytes(data);

// Wrap in Request (constructor call)
GetRequest request = new GetRequest(buffer);

// Process request
requestHandler.handle(request);

// OOPS! Forgot to release buffer
// request.release();  ← Missing!
```

### Output WITHOUT Constructor Tracking

```
=== ByteBuf Flow Summary ===
Total Root Methods: 1
Leak Paths: 0

ROOT: allocate [count=1]
└── writeBytes [ref=1, count=1]
```

**Problem:** Flow stops at `writeBytes`. Constructor not tracked, so leak is **invisible**!

### Output WITH Constructor Tracking

```
=== ByteBuf Flow Summary ===
Total Root Methods: 1
Leak Paths: 1

ROOT: allocate [count=1]
└── writeBytes [ref=1, count=1]
    └── GetRequest.<init> [ref=1, count=1]
        └── RequestHandler.handle [ref=1, count=1]
            └── processData [ref=1, count=1] ⚠️ LEAK

Leak detected! ByteBuf still has refCount=1
```

**Success:** Complete flow tracked, leak **detected**!

---

## Performance Considerations

### Overhead

- **Methods**: ~5-10% overhead (already active)
- **Constructors**: Additional ~2-3% overhead per tracked class
- **Recommendation**: Track only classes that commonly wrap ByteBufs

### Selective Tracking Benefits

By being selective (not tracking ALL constructors), you get:
1. **Lower overhead** - Only track critical classes
2. **Cleaner output** - Less noise from unrelated constructors
3. **Focused analysis** - See only relevant ByteBuf flows

### What NOT to Track

❌ Don't track constructors for:
- Classes that **never** handle ByteBufs
- Utility classes that pass ByteBufs through immediately
- Internal JDK/Netty classes (already excluded)

---

## Customizing Constructor Tracking

### Add New Classes

Edit `/home/user/ambry/build.gradle` at lines 185 and 247:

```groovy
trackConstructors=com.github.ambry.protocol.*,
                  com.github.ambry.network.*,          // ← Added wildcard
                  com.github.ambry.router.GetBlobOperation,
                  com.github.ambry.yourmodule.YourClass  // ← Added your class
```

### Remove Classes

Simply remove from the comma-separated list if a class doesn't need tracking.

### Test-Specific Tracking

Different tracking for unit vs integration tests:

```groovy
test {
    if (byteBufTrackingEnabled) {
        // Narrow tracking for fast unit tests
        jvmArgs "-javaagent:...;trackConstructors=com.github.ambry.protocol.*"
    }
}

intTest {
    if (byteBufTrackingEnabled) {
        // Comprehensive tracking for integration tests
        jvmArgs "-javaagent:...;trackConstructors=com.github.ambry.protocol.*,com.github.ambry.network.*,com.github.ambry.router.*"
    }
}
```

---

## Verifying Constructor Tracking

### Check Configuration

When tests run with `-PwithByteBufTracking`, you'll see:

```
[ByteBufFlowAgent] Starting with config: AgentConfig{
    include=[com.github.ambry],
    exclude=[],
    trackConstructors=[com.github.ambry.protocol.*, com.github.ambry.network.Request, ...]
}
[ByteBufFlowAgent] Constructor tracking enabled for: [com.github.ambry.protocol.*, ...]
```

### Check Output

Look for `<init>` in the flow tree:

```
ROOT: allocator.allocate [count=5]
└── GetRequest.<init> [ref=1, count=5]    ← Constructor tracked!
    └── handler.process [ref=1, count=5]
```

If you don't see `<init>`, constructor tracking isn't working.

---

## Troubleshooting

### Constructor not appearing in output

**Problem:** Class not in `trackConstructors` list

**Solution:** Add it:
```groovy
trackConstructors=...,com.github.ambry.yourpackage.YourClass
```

### Too much output / Performance issues

**Problem:** Tracking too many constructors

**Solution:** Be more selective:
```groovy
// Instead of this (tracks EVERYTHING in ambry):
trackConstructors=com.github.ambry.*

// Do this (track only specific modules):
trackConstructors=com.github.ambry.protocol.*,com.github.ambry.network.Request
```

### Flow still breaks after constructor

**Problem:** ByteBuf passed to another object's constructor

**Solution:** Track that class too! Follow the chain:
```
allocate() -> RequestWrapper.<init>() -> Request.<init>() -> ...
              ↑                           ↑
              Track this                  Track this too!
```

---

## Best Practices

### 1. Start with Protocol Layer

Begin with `com.github.ambry.protocol.*`:
- Most likely to wrap ByteBufs
- Good signal-to-noise ratio

### 2. Expand Based on Leaks

If you see leaks in output, trace back to find constructor calls and add those classes.

### 3. Use Wildcards for Packages

Instead of listing every class:
```groovy
// Good
trackConstructors=com.github.ambry.protocol.*

// Bad (tedious and error-prone)
trackConstructors=com.github.ambry.protocol.GetRequest,com.github.ambry.protocol.PutRequest,...
```

### 4. Document Why Each Class is Tracked

Add comments explaining the rationale:
```groovy
trackConstructors=
    com.github.ambry.protocol.*,              // All protocol messages wrap ByteBufs
    com.github.ambry.network.Request,         // Network layer wraps protocol messages
    com.github.ambry.router.GetBlobOperation  // Operations accumulate ByteBuf chunks
```

---

## Related Documentation

- **BYTEBUF_TRACKER_INTEGRATION.md** - Complete integration guide
- **bytebuf-tracker/README.md** - Tracker module documentation
- **bytebuf-tracker/CLAUDE_CODE_INTEGRATION.md** - Integration patterns
- **bytebuf-tracker/UPSTREAM_README.md** - Upstream project documentation

---

## Summary

- **Constructor tracking is essential** for wrapped ByteBuf leak detection
- **Opt-in and selective** - Configure exactly which classes to track
- **Already configured** for key Ambry wrapper classes
- **Wildcard support** - Track entire packages with `Package.*`
- **Minimal overhead** when used selectively
- **Complete flow visibility** from allocation through wrappers to release

**When in doubt, add the class to `trackConstructors`!** It's better to track too much than miss a leak.
