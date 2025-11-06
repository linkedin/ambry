package com.example.bytebuf.tracker.agent;

import com.example.bytebuf.tracker.ByteBufFlowTracker;
import com.example.bytebuf.tracker.view.TrieRenderer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Date;

/**
 * JMX MBean interface for ByteBuf flow monitoring
 */
public interface ByteBufFlowMBeanInterface {
    // View operations
    String getTreeView();
    String getFlatView();
    String getCsvView();
    String getJsonView();
    String getSummary();
    
    // Statistics
    int getRootCount();
    int getActiveFlowCount();
    
    // Export operations
    void exportToFile(String filepath, String format);
    
    // Control operations
    void reset();
}

/**
 * JMX MBean implementation
 */
public class ByteBufFlowMBean implements ByteBufFlowMBeanInterface {
    
    private final ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();
    
    @Override
    public String getTreeView() {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        return renderer.renderIndentedTree();
    }
    
    @Override
    public String getFlatView() {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        return renderer.renderFlatPaths();
    }
    
    @Override
    public String getCsvView() {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        return renderer.renderCsv();
    }
    
    @Override
    public String getJsonView() {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        return renderer.renderJson();
    }
    
    @Override
    public String getSummary() {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        StringBuilder sb = new StringBuilder();
        
        sb.append("=== ByteBuf Flow Tracking Status ===\n");
        sb.append("Time: ").append(new Date()).append("\n");
        sb.append("Active Flows: ").append(tracker.getActiveFlowCount()).append("\n");
        sb.append("Root Methods: ").append(tracker.getTrie().getRootCount()).append("\n");
        sb.append("\n");
        sb.append(renderer.renderSummary());
        
        return sb.toString();
    }
    
    @Override
    public int getRootCount() {
        return tracker.getTrie().getRootCount();
    }
    
    @Override
    public int getActiveFlowCount() {
        return tracker.getActiveFlowCount();
    }
    
    @Override
    public void exportToFile(String filepath, String format) {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        String content;
        
        switch (format.toLowerCase()) {
            case "tree":
                content = renderer.renderIndentedTree();
                break;
            case "flat":
                content = renderer.renderFlatPaths();
                break;
            case "csv":
                content = renderer.renderCsv();
                break;
            case "json":
                content = renderer.renderJson();
                break;
            default:
                content = "Unknown format: " + format + "\n" +
                         "Supported formats: tree, flat, csv, json";
        }
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(filepath))) {
            writer.write(content);
            System.out.println("[ByteBufFlowMBean] Exported to " + filepath);
        } catch (IOException e) {
            System.err.println("[ByteBufFlowMBean] Failed to export: " + e);
        }
    }
    
    @Override
    public void reset() {
        tracker.reset();
        System.out.println("[ByteBufFlowMBean] Tracker reset");
    }
}

/**
 * Reporter for generating comprehensive reports
 */
class ByteBufFlowReporter {
    
    private final ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();
    
    /**
     * Generate a comprehensive report
     */
    public String generateReport() {
        TrieRenderer renderer = new TrieRenderer(tracker.getTrie());
        StringBuilder report = new StringBuilder();
        
        // Header
        report.append("ByteBuf Flow Analysis Report\n");
        report.append("Generated: ").append(new Date()).append("\n");
        report.append("=" .repeat(80)).append("\n\n");
        
        // Summary
        report.append(renderer.renderSummary()).append("\n");
        
        // Tree view
        report.append("=== Flow Tree ===\n");
        report.append(renderer.renderIndentedTree()).append("\n");
        
        // Potential leaks (leaf nodes with refCount != 0)
        report.append("=== Potential Leaks ===\n");
        String flatPaths = renderer.renderFlatPaths();
        String[] lines = flatPaths.split("\n");
        int leakCount = 0;
        for (String line : lines) {
            if (line.contains("[LEAK:")) {
                report.append(line).append("\n");
                leakCount++;
            }
        }
        if (leakCount == 0) {
            report.append("No leaks detected\n");
        }
        report.append("\n");
        
        return report.toString();
    }
}
