package com.example.bytebuf.tracker.view;

import com.example.bytebuf.tracker.trie.FlowTrie;
import com.example.bytebuf.tracker.trie.FlowTrie.TrieNode;
import com.example.bytebuf.tracker.trie.FlowTrie.NodeKey;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Renders the Trie data structure in various formats for analysis.
 * Pure presentation logic - no business logic or analysis.
 */
public class TrieRenderer {
    
    private final FlowTrie trie;
    
    public TrieRenderer(FlowTrie trie) {
        this.trie = trie;
    }
    
    /**
     * Render as indented tree format (human-readable)
     */
    public String renderIndentedTree() {
        StringBuilder sb = new StringBuilder();
        
        // Sort roots by traversal count for better visibility
        List<Map.Entry<String, TrieNode>> sortedRoots = trie.getRoots().entrySet().stream()
            .sorted((a, b) -> Long.compare(b.getValue().getTraversalCount(), 
                                          a.getValue().getTraversalCount()))
            .collect(Collectors.toList());
        
        for (Map.Entry<String, TrieNode> entry : sortedRoots) {
            sb.append("ROOT: ").append(entry.getKey());
            sb.append(" [count=").append(entry.getValue().getTraversalCount()).append("]\n");
            renderNode(sb, entry.getValue(), "", true, true);
        }
        
        return sb.toString();
    }
    
    private void renderNode(StringBuilder sb, TrieNode node, String prefix, boolean isLast, boolean isRoot) {
        if (!isRoot) {
            sb.append(prefix);
            sb.append(isLast ? "└── " : "├── ");
            sb.append(formatNode(node));
            sb.append("\n");
        }
        
        String childPrefix = isRoot ? "" : prefix + (isLast ? "    " : "│   ");
        
        List<Map.Entry<NodeKey, TrieNode>> children = new ArrayList<>(node.getChildren().entrySet());
        // Sort children by traversal count
        children.sort((a, b) -> Long.compare(b.getValue().getTraversalCount(), 
                                            a.getValue().getTraversalCount()));
        
        for (int i = 0; i < children.size(); i++) {
            Map.Entry<NodeKey, TrieNode> child = children.get(i);
            boolean isLastChild = (i == children.size() - 1);
            renderNode(sb, child.getValue(), childPrefix, isLastChild, false);
        }
    }
    
    private String formatNode(TrieNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getClassName()).append(".").append(node.getMethodName());
        sb.append(" [ref=").append(node.getRefCount());
        sb.append(", count=").append(node.getTraversalCount()).append("]");
        
        // Add indicators for potential issues
        if (node.isLeaf() && node.getRefCount() != 0) {
            sb.append(" ⚠️ LEAK");
        }
        
        return sb.toString();
    }
    
    /**
     * Render as flat paths (each root-to-leaf path on one line)
     */
    public String renderFlatPaths() {
        StringBuilder sb = new StringBuilder();
        
        for (Map.Entry<String, TrieNode> entry : trie.getRoots().entrySet()) {
            List<String> currentPath = new ArrayList<>();
            currentPath.add(entry.getKey());
            collectPaths(sb, entry.getValue(), currentPath);
        }
        
        return sb.toString();
    }
    
    private void collectPaths(StringBuilder sb, TrieNode node, List<String> currentPath) {
        if (node.isLeaf()) {
            // Leaf node - output the complete path
            sb.append("[count=").append(node.getTraversalCount()).append("]");
            if (node.getRefCount() != 0) {
                sb.append(" [LEAK:ref=").append(node.getRefCount()).append("]");
            }
            sb.append(" ");
            sb.append(String.join(" -> ", currentPath));
            sb.append("\n");
        } else {
            // Continue traversing
            for (Map.Entry<NodeKey, TrieNode> child : node.getChildren().entrySet()) {
                List<String> newPath = new ArrayList<>(currentPath);
                newPath.add(formatNodeCompact(child.getValue()));
                collectPaths(sb, child.getValue(), newPath);
            }
        }
    }
    
    private String formatNodeCompact(TrieNode node) {
        return String.format("%s.%s[%d]", 
            node.getClassName(), 
            node.getMethodName(), 
            node.getRefCount());
    }
    
    /**
     * Render as CSV for analysis in spreadsheets
     */
    public String renderCsv() {
        StringBuilder sb = new StringBuilder();
        sb.append("root,path,final_ref_count,traversal_count,is_leak\n");
        
        for (Map.Entry<String, TrieNode> entry : trie.getRoots().entrySet()) {
            List<String> currentPath = new ArrayList<>();
            exportToCsv(sb, entry.getKey(), entry.getValue(), currentPath);
        }
        
        return sb.toString();
    }
    
    private void exportToCsv(StringBuilder sb, String root, TrieNode node, List<String> currentPath) {
        currentPath.add(formatNodeCompact(node));
        
        if (node.isLeaf()) {
            boolean isLeak = node.getRefCount() != 0;
            sb.append('"').append(root).append('"').append(',');
            sb.append('"').append(String.join(" -> ", currentPath)).append('"').append(',');
            sb.append(node.getRefCount()).append(',');
            sb.append(node.getTraversalCount()).append(',');
            sb.append(isLeak ? "true" : "false").append('\n');
        } else {
            for (TrieNode child : node.getChildren().values()) {
                exportToCsv(sb, root, child, new ArrayList<>(currentPath));
            }
        }
    }
    
    /**
     * Render as JSON for programmatic analysis
     */
    public String renderJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"rootCount\": ").append(trie.getRootCount()).append(",\n");
        sb.append("  \"roots\": [\n");
        
        boolean firstRoot = true;
        for (Map.Entry<String, TrieNode> entry : trie.getRoots().entrySet()) {
            if (!firstRoot) sb.append(",\n");
            firstRoot = false;
            
            sb.append("    {\n");
            sb.append("      \"name\": \"").append(entry.getKey()).append("\",\n");
            sb.append("      \"traversalCount\": ").append(entry.getValue().getTraversalCount()).append(",\n");
            sb.append("      \"tree\": ");
            renderNodeJson(sb, entry.getValue(), "      ");
            sb.append("\n    }");
        }
        
        sb.append("\n  ]\n");
        sb.append("}\n");
        
        return sb.toString();
    }
    
    private void renderNodeJson(StringBuilder sb, TrieNode node, String indent) {
        sb.append("{\n");
        sb.append(indent).append("  \"class\": \"").append(node.getClassName()).append("\",\n");
        sb.append(indent).append("  \"method\": \"").append(node.getMethodName()).append("\",\n");
        sb.append(indent).append("  \"refCount\": ").append(node.getRefCount()).append(",\n");
        sb.append(indent).append("  \"traversalCount\": ").append(node.getTraversalCount()).append(",\n");
        sb.append(indent).append("  \"isLeaf\": ").append(node.isLeaf()).append(",\n");
        
        if (node.isLeaf() && node.getRefCount() != 0) {
            sb.append(indent).append("  \"isLeak\": true,\n");
        }
        
        sb.append(indent).append("  \"children\": [");
        
        if (!node.getChildren().isEmpty()) {
            sb.append("\n");
            boolean firstChild = true;
            for (TrieNode child : node.getChildren().values()) {
                if (!firstChild) sb.append(",\n");
                firstChild = false;
                sb.append(indent).append("    ");
                renderNodeJson(sb, child, indent + "    ");
            }
            sb.append("\n").append(indent).append("  ]");
        } else {
            sb.append("]");
        }
        
        sb.append("\n").append(indent).append("}");
    }
    
    /**
     * Render summary statistics
     */
    public String renderSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== ByteBuf Flow Summary ===\n");
        sb.append("Total Root Methods: ").append(trie.getRootCount()).append("\n");
        
        long totalTraversals = 0;
        int totalPaths = 0;
        int leakPaths = 0;
        
        for (TrieNode root : trie.getRoots().values()) {
            PathStats stats = calculatePathStats(root);
            totalTraversals += root.getTraversalCount();
            totalPaths += stats.pathCount;
            leakPaths += stats.leakCount;
        }
        
        sb.append("Total Traversals: ").append(totalTraversals).append("\n");
        sb.append("Unique Paths: ").append(totalPaths).append("\n");
        sb.append("Leak Paths: ").append(leakPaths).append("\n");
        
        if (totalPaths > 0) {
            double leakPercentage = (leakPaths * 100.0) / totalPaths;
            sb.append(String.format("Leak Percentage: %.2f%%\n", leakPercentage));
        }
        
        return sb.toString();
    }
    
    private static class PathStats {
        int pathCount = 0;
        int leakCount = 0;
    }
    
    private PathStats calculatePathStats(TrieNode node) {
        PathStats stats = new PathStats();
        
        if (node.isLeaf()) {
            stats.pathCount = 1;
            if (node.getRefCount() != 0) {
                stats.leakCount = 1;
            }
        } else {
            for (TrieNode child : node.getChildren().values()) {
                PathStats childStats = calculatePathStats(child);
                stats.pathCount += childStats.pathCount;
                stats.leakCount += childStats.leakCount;
            }
        }
        
        return stats;
    }
}
