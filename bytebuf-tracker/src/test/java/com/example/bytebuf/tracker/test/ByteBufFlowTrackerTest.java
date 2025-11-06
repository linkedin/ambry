package com.example.bytebuf.tracker.test;

import com.example.bytebuf.tracker.ByteBufFlowTracker;
import com.example.bytebuf.tracker.view.TrieRenderer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test and example usage of ByteBuf Flow Tracker
 */
public class ByteBufFlowTrackerTest {
    
    private ByteBufFlowTracker tracker;
    
    @Before
    public void setup() {
        tracker = ByteBufFlowTracker.getInstance();
        tracker.reset();
    }
    
    @Test
    public void testSimpleFlowTracking() {
        // Create a ByteBuf
        ByteBuf buffer = Unpooled.buffer(256);
        
        // Simulate method calls
        tracker.recordMethodCall(buffer, "FrameDecoder", "decode", buffer.refCnt());
        tracker.recordMethodCall(buffer, "MessageHandler", "handle", buffer.refCnt());
        tracker.recordMethodCall(buffer, "BusinessService", "process", buffer.refCnt());
        
        // Release the buffer
        buffer.release();
        tracker.recordMethodCall(buffer, "BusinessService", "process", buffer.refCnt());
        
        // Verify the tree structure
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        String tree = renderer.renderIndentedTree();
        
        assertTrue(tree.contains("FrameDecoder.decode"));
        assertTrue(tree.contains("MessageHandler.handle"));
        assertTrue(tree.contains("BusinessService.process"));
        
        System.out.println("Simple Flow Tree:");
        System.out.println(tree);
    }
    
    @Test
    public void testLeakDetection() {
        // Create a ByteBuf that will leak
        ByteBuf leakyBuffer = Unpooled.buffer(256);
        
        // Simulate method calls
        tracker.recordMethodCall(leakyBuffer, "HttpHandler", "handleRequest", leakyBuffer.refCnt());
        tracker.recordMethodCall(leakyBuffer, "RequestProcessor", "process", leakyBuffer.refCnt());
        tracker.recordMethodCall(leakyBuffer, "ErrorLogger", "log", leakyBuffer.refCnt());
        // Note: We don't release the buffer - this is a leak!
        
        // Create another ByteBuf that is properly released
        ByteBuf goodBuffer = Unpooled.buffer(256);
        tracker.recordMethodCall(goodBuffer, "HttpHandler", "handleRequest", goodBuffer.refCnt());
        tracker.recordMethodCall(goodBuffer, "RequestProcessor", "process", goodBuffer.refCnt());
        tracker.recordMethodCall(goodBuffer, "ResponseWriter", "write", goodBuffer.refCnt());
        goodBuffer.release();
        tracker.recordMethodCall(goodBuffer, "ResponseWriter", "write", goodBuffer.refCnt());
        
        // Get the flat view to see leaks
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        String flatView = renderer.renderFlatPaths();
        
        System.out.println("\nLeak Detection - Flat Paths:");
        System.out.println(flatView);
        
        // The leaky path should show ref=1 at the end
        assertTrue(flatView.contains("ErrorLogger.log[1]"));
        // The good path should show ref=0 at the end
        assertTrue(flatView.contains("ResponseWriter.write[0]"));
    }
    
    @Test
    public void testRefCountAnomalies() {
        // Simulate the same path with different refCounts
        ByteBuf buffer1 = Unpooled.buffer(256);
        buffer1.retain(); // refCount = 2
        
        tracker.recordMethodCall(buffer1, "MessageDecoder", "decode", buffer1.refCnt());
        tracker.recordMethodCall(buffer1, "MessageValidator", "validate", buffer1.refCnt());
        buffer1.release();
        tracker.recordMethodCall(buffer1, "MessageProcessor", "process", buffer1.refCnt());
        buffer1.release();
        tracker.recordMethodCall(buffer1, "MessageProcessor", "process", buffer1.refCnt());
        
        // Same path but different refCount pattern
        ByteBuf buffer2 = Unpooled.buffer(256);
        // refCount = 1 (no extra retain)
        
        tracker.recordMethodCall(buffer2, "MessageDecoder", "decode", buffer2.refCnt());
        tracker.recordMethodCall(buffer2, "MessageValidator", "validate", buffer2.refCnt());
        tracker.recordMethodCall(buffer2, "MessageProcessor", "process", buffer2.refCnt());
        buffer2.release();
        tracker.recordMethodCall(buffer2, "MessageProcessor", "process", buffer2.refCnt());
        
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        String tree = renderer.renderIndentedTree();
        
        System.out.println("\nRefCount Anomaly Detection:");
        System.out.println(tree);
        
        // Should show MessageValidator appearing twice with different refCounts
        assertTrue(tree.contains("MessageValidator.validate [ref=2"));
        assertTrue(tree.contains("MessageValidator.validate [ref=1"));
    }
    
    @Test
    public void testHighVolumeTracking() {
        // Simulate high volume traffic
        for (int i = 0; i < 1000; i++) {
            ByteBuf buffer = Unpooled.buffer(256);
            
            tracker.recordMethodCall(buffer, "HighVolumeHandler", "handle", buffer.refCnt());
            tracker.recordMethodCall(buffer, "FastProcessor", "process", buffer.refCnt());
            
            if (i % 10 == 0) {
                // 10% take a different path
                tracker.recordMethodCall(buffer, "SlowProcessor", "process", buffer.refCnt());
            }
            
            buffer.release();
            tracker.recordMethodCall(buffer, "FastProcessor", "process", buffer.refCnt());
        }
        
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        String summary = renderer.renderSummary();
        
        System.out.println("\nHigh Volume Summary:");
        System.out.println(summary);
        
        assertTrue(summary.contains("Total Traversals: 1000"));
    }
    
    @Test
    public void testCsvExport() {
        // Create some test data
        ByteBuf buffer = Unpooled.buffer(256);
        
        tracker.recordMethodCall(buffer, "CsvTest", "method1", buffer.refCnt());
        tracker.recordMethodCall(buffer, "CsvTest", "method2", buffer.refCnt());
        buffer.release();
        tracker.recordMethodCall(buffer, "CsvTest", "method3", buffer.refCnt());
        
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        String csv = renderer.renderCsv();
        
        System.out.println("\nCSV Export:");
        System.out.println(csv);
        
        assertTrue(csv.startsWith("root,path,final_ref_count,traversal_count,is_leak"));
        assertTrue(csv.contains("CsvTest.method1"));
        assertTrue(csv.contains("false")); // not a leak
    }
    
    /**
     * Example of how to use the tracker in production code
     */
    public static class ProductionExample {
        
        private final ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();
        
        public void handleMessage(ByteBuf message) {
            // Track entry into this method
            tracker.recordMethodCall(message, getClass().getSimpleName(), "handleMessage", message.refCnt());
            
            try {
                // Process the message
                processInternal(message);
            } finally {
                // Always release and track
                message.release();
                tracker.recordMethodCall(message, getClass().getSimpleName(), "handleMessage_exit", message.refCnt());
            }
        }
        
        private void processInternal(ByteBuf message) {
            tracker.recordMethodCall(message, getClass().getSimpleName(), "processInternal", message.refCnt());
            // ... actual processing ...
        }
    }
}
