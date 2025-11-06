package com.example.bytebuf.tracker;

/**
 * Handler interface for tracking custom objects through your application.
 *
 * Implement this interface to track objects other than ByteBuf.
 * The tracker will use this to:
 * 1. Identify objects that should be tracked
 * 2. Extract a metric from those objects (like refCount for ByteBuf)
 * 3. Name the object type for reporting
 *
 * Example implementation for tracking database connections:
 *
 * <pre>
 * public class ConnectionTrackerHandler implements ObjectTrackerHandler {
 *     public boolean shouldTrack(Object obj) {
 *         return obj instanceof Connection;
 *     }
 *
 *     public int getMetric(Object obj) {
 *         Connection conn = (Connection) obj;
 *         return conn.isClosed() ? 0 : 1;
 *     }
 *
 *     public String getObjectType() {
 *         return "Connection";
 *     }
 * }
 * </pre>
 */
public interface ObjectTrackerHandler {

    /**
     * Determine if this object should be tracked.
     *
     * Called for every parameter and return value in instrumented methods.
     * Should be fast - this is called very frequently.
     *
     * @param obj The object to check (may be null)
     * @return true if this object should be tracked
     */
    boolean shouldTrack(Object obj);

    /**
     * Extract a metric from the tracked object.
     *
     * For ByteBuf, this is the reference count.
     * For database connections, this might be 0 if closed, 1 if open.
     * For file handles, this might be 0 if closed, 1 if open.
     * For custom objects, use any integer metric that makes sense.
     *
     * The tracker builds a tree showing how this metric changes as the
     * object flows through your code. Leaf nodes with non-zero metrics
     * are flagged as potential leaks.
     *
     * @param obj The object to extract metric from (never null, already passed shouldTrack)
     * @return Integer metric value (commonly 0 means released/closed, >0 means active)
     */
    int getMetric(Object obj);

    /**
     * Get the type name for this object (used in reports).
     *
     * This appears in the output to identify what's being tracked.
     * Examples: "ByteBuf", "Connection", "FileHandle", "SocketChannel"
     *
     * @return A descriptive name for the object type
     */
    String getObjectType();
}
