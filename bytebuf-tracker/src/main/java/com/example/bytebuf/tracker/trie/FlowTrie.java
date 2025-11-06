package com.example.bytebuf.tracker.trie;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Pure Trie data structure for ByteBuf flow tracking.
 * First method that touches a ByteBuf becomes the root.
 * No allocation tracking - simplified and efficient.
 */
public class FlowTrie {
    // Map from "ClassName.methodName" to root nodes
    private final Map<String, TrieNode> roots = new ConcurrentHashMap<>();
    
    /**
     * Get or create a root node for a method
     */
    public TrieNode getOrCreateRoot(String className, String methodName) {
        String key = className + "." + methodName;
        return roots.computeIfAbsent(key, k -> new TrieNode(className, methodName, 1));
    }
    
    /**
     * Get all roots for analysis/viewing
     */
    public Map<String, TrieNode> getRoots() {
        return Collections.unmodifiableMap(roots);
    }
    
    /**
     * Clear all data (useful for testing or resetting)
     */
    public void clear() {
        roots.clear();
    }
    
    /**
     * Get total number of root nodes
     */
    public int getRootCount() {
        return roots.size();
    }
    
    /**
     * Single node in the Trie representing a method invocation
     */
    public static class TrieNode {
        private final String className;
        private final String methodName;
        private final int refCount;
        private final Map<NodeKey, TrieNode> children = new ConcurrentHashMap<>();
        private final LongAdder traversalCount = new LongAdder();
        
        public TrieNode(String className, String methodName, int refCount) {
            this.className = className;
            this.methodName = methodName;
            this.refCount = refCount;
        }
        
        /**
         * Record traversal through this node and get/create child
         */
        public TrieNode traverse(String className, String methodName, int refCount) {
            traversalCount.increment();
            
            NodeKey key = new NodeKey(className, methodName, refCount);
            return children.computeIfAbsent(key, k -> 
                new TrieNode(className, methodName, refCount)
            );
        }
        
        /**
         * Just record that we passed through this node (for leaf nodes)
         */
        public void recordTraversal() {
            traversalCount.increment();
        }
        
        // Getters for read-only access
        public String getClassName() { return className; }
        public String getMethodName() { return methodName; }
        public int getRefCount() { return refCount; }
        public long getTraversalCount() { return traversalCount.sum(); }
        public Map<NodeKey, TrieNode> getChildren() { 
            return Collections.unmodifiableMap(children); 
        }
        
        public boolean isLeaf() {
            return children.isEmpty();
        }
    }
    
    /**
     * Key for child nodes - includes refCount to track variations
     * Using refCount in the key means same method with different refCounts 
     * creates different branches (making anomalies visible)
     */
    public static class NodeKey {
        public final String className;
        public final String methodName;
        public final int refCount;
        private final int hashCode;
        
        public NodeKey(String className, String methodName, int refCount) {
            this.className = className;
            this.methodName = methodName;
            this.refCount = refCount;
            this.hashCode = Objects.hash(className, methodName, refCount);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NodeKey)) return false;
            NodeKey that = (NodeKey) o;
            return refCount == that.refCount &&
                   className.equals(that.className) &&
                   methodName.equals(that.methodName);
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        @Override
        public String toString() {
            return String.format("%s.%s[%d]", className, methodName, refCount);
        }
    }
}
