// Copyright (C) 2025. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied.

package com.github.ambry.testutils;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * JUnit test listener that writes ByteBuf flow tracker information to a file at the end of test execution.
 * This integrates with the ByteBuddy ByteBuf tracer to detect memory leaks in tests.
 *
 * Output is written to: build/reports/bytebuf-tracking/[module-name].txt
 */
public class ByteBufTrackerListener extends RunListener {

  private static final String TRACKER_CLASS = "com.example.bytebuf.tracker.ByteBufFlowTracker";
  private static final String RENDERER_CLASS = "com.example.bytebuf.tracker.view.TrieRenderer";
  private static final String SEPARATOR_LINE = "================================================================================";
  private static final String SUBSECTION_LINE = "--------------------------------------------------------------------------------";

  /**
   * Helper method to repeat a string (Java 8 compatible).
   * @param str the string to repeat
   * @param count the number of times to repeat
   * @return the repeated string
   */
  private static String repeat(String str, int count) {
    StringBuilder sb = new StringBuilder(str.length() * count);
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    try {
      // Try to load the tracker class (only if the agent is running)
      Class<?> trackerClass = Class.forName(TRACKER_CLASS);
      Object tracker = trackerClass.getMethod("getInstance").invoke(null);
      Object trie = trackerClass.getMethod("getTrie").invoke(tracker);

      // Create a renderer
      Class<?> rendererClass = Class.forName(RENDERER_CLASS);
      Object renderer = rendererClass.getConstructor(trie.getClass()).newInstance(trie);

      // Build the report
      StringBuilder report = new StringBuilder();
      report.append("\n").append(SEPARATOR_LINE).append("\n");
      report.append("ByteBuf Flow Tracker Report\n");
      report.append(SEPARATOR_LINE).append("\n");

      String summary = (String) rendererClass.getMethod("renderSummary").invoke(renderer);
      report.append(summary).append("\n");

      report.append("\n").append(SUBSECTION_LINE).append("\n");
      report.append("Flow Tree:\n");
      report.append(SUBSECTION_LINE).append("\n");

      String tree = (String) rendererClass.getMethod("renderIndentedTree").invoke(renderer);
      report.append(tree).append("\n");

      report.append("\n").append(SUBSECTION_LINE).append("\n");
      report.append("Flat Paths (Leaks Highlighted):\n");
      report.append(SUBSECTION_LINE).append("\n");

      String flatPaths = (String) rendererClass.getMethod("renderFlatPaths").invoke(renderer);
      report.append(flatPaths).append("\n");

      report.append("\n").append(SEPARATOR_LINE).append("\n");
      report.append("End of ByteBuf Flow Tracker Report\n");
      report.append(SEPARATOR_LINE).append("\n");

      // Write to file instead of stdout (Gradle captures stdout)
      writeReportToFile(report.toString());

      // Also print a notification to stdout
      System.out.println("\n[ByteBufTracker] Report written to: " + getReportFile().getAbsolutePath());

    } catch (ClassNotFoundException e) {
      // Tracker not available - agent not running or not in classpath
      // This is normal when tests run without -PwithByteBufTracking
    } catch (Exception e) {
      System.err.println("Error generating ByteBuf flow report: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Get the report file location
   */
  private File getReportFile() {
    // Try to determine the module name from the working directory
    String workingDir = System.getProperty("user.dir", "");
    String moduleName = "unknown";

    // Extract module name from path (e.g., /path/to/ambry/ambry-store -> ambry-store)
    if (workingDir.contains(File.separator)) {
      String[] parts = workingDir.split(File.separator);
      if (parts.length > 0) {
        moduleName = parts[parts.length - 1];
      }
    }

    // Create reports directory
    File reportsDir = new File("build/reports/bytebuf-tracking");
    reportsDir.mkdirs();

    return new File(reportsDir, moduleName + "-" + System.currentTimeMillis() + ".txt");
  }

  /**
   * Write report to file
   */
  private void writeReportToFile(String report) {
    File reportFile = getReportFile();
    try (PrintWriter writer = new PrintWriter(new FileWriter(reportFile))) {
      writer.write(report);
      writer.flush();
    } catch (IOException e) {
      System.err.println("Failed to write ByteBuf tracking report to file: " + e.getMessage());
      // Fall back to stdout
      System.out.println(report);
    }
  }

  @Override
  public void testStarted(Description description) throws Exception {
    // Could add per-test tracking here if needed
  }

  @Override
  public void testFinished(Description description) throws Exception {
    // Could add per-test reporting here if needed
  }
}
