package com.example.bytebuf.tracker;

import com.example.bytebuf.tracker.trie.FlowTrie;
import com.example.bytebuf.tracker.trie.FlowTrie.TrieNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks ByteBuf flows through the system using a Trie structure.
 * First method to touch a ByteBuf becomes its root in the Trie.
 * Simplified design with no allocation tracking or stack traces.
 */
public class ByteBufFlowTracker {
    private static final ByteBufFlowTracker INSTANCE = new ByteBufFlowTracker();
    
    private final FlowTrie trie = new FlowTrie();
    private final Map<Integer, FlowContext> activeFlows = new ConcurrentHashMap<>();
    
    /**
     * Context for tracking a single ByteBuf through its lifecycle
     */
    private static class FlowContext {
        private final int objectId;
        private TrieNode currentNode;
        private boolean isRootSet = false;
        
        public FlowContext(int objectId) {
            this.objectId = objectId;
        }
        
        public void setRoot(TrieNode root) {
            this.currentNode = root;
            this.isRootSet = true;
        }
        
        public void moveToNode(TrieNode node) {
            this.currentNode = node;
        }
        
        public TrieNode getCurrentNode() {
            return currentNode;
        }
        
        public boolean hasRoot() {
            return isRootSet;
        }
    }
    
    /**
     * Record a method call involving a ByteBuf
     * 
     * @param byteBuf The ByteBuf object
     * @param className The class containing the method
     * @param methodName The method name
     * @param refCount Current reference count of the ByteBuf
     */
    public void recordMethodCall(Object byteBuf, String className, String methodName, int refCount) {
        if (byteBuf == null) return;
        
        int objectId = System.identityHashCode(byteBuf);
        
        // Get or create context for this ByteBuf
        FlowContext context = activeFlows.computeIfAbsent(objectId, FlowContext::new);
        
        if (!context.hasRoot()) {
            // First time seeing this ByteBuf - create root
            TrieNode root = trie.getOrCreateRoot(className, methodName);
            root.recordTraversal();
            context.setRoot(root);
        } else {
            // Continue traversing the Trie
            TrieNode currentNode = context.getCurrentNode();
            if (currentNode != null) {
                TrieNode nextNode = currentNode.traverse(className, methodName, refCount);
                context.moveToNode(nextNode);
            }
        }
        
        // If refCount is 0, ByteBuf is released - remove from tracking
        if (refCount == 0) {
            activeFlows.remove(objectId);
        }
    }
    
    /**
     * Record that a ByteBuf was garbage collected without being properly released
     * This indicates a leak
     * 
     * @param byteBuf The ByteBuf that was GC'd
     * @param finalRefCount The reference count when GC'd
     */
    public void recordGarbageCollection(Object byteBuf, int finalRefCount) {
        if (byteBuf == null) return;
        
        int objectId = System.identityHashCode(byteBuf);
        FlowContext context = activeFlows.remove(objectId);
        
        if (context != null && context.getCurrentNode() != null) {
            // Mark this as a leak by recording it with the final refCount
            // The fact that it ends with non-zero refCount indicates a leak
            context.getCurrentNode().recordTraversal();
        }
    }
    
    /**
     * Get the underlying Trie for analysis/viewing
     */
    public FlowTrie getTrie() {
        return trie;
    }
    
    /**
     * Get number of ByteBufs currently being tracked
     */
    public int getActiveFlowCount() {
        return activeFlows.size();
    }
    
    /**
     * Clear all tracking data
     */
    public void reset() {
        activeFlows.clear();
        trie.clear();
    }
    
    /**
     * Get singleton instance
     */
    public static ByteBufFlowTracker getInstance() {
        return INSTANCE;
    }
}
