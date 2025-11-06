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

/**
 * JUnit test listener that prints ByteBuf flow tracker information at the end of test execution.
 * This integrates with the ByteBuddy ByteBuf tracer to detect memory leaks in tests.
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

      // Print summary and full report
      System.out.println("\n" + SEPARATOR_LINE);
      System.out.println("ByteBuf Flow Tracker Report");
      System.out.println(SEPARATOR_LINE);

      String summary = (String) rendererClass.getMethod("renderSummary").invoke(renderer);
      System.out.println(summary);

      System.out.println("\n" + SUBSECTION_LINE);
      System.out.println("Flow Tree:");
      System.out.println(SUBSECTION_LINE);

      String tree = (String) rendererClass.getMethod("renderIndentedTree").invoke(renderer);
      System.out.println(tree);

      System.out.println("\n" + SUBSECTION_LINE);
      System.out.println("Flat Paths (Leaks Highlighted):");
      System.out.println(SUBSECTION_LINE);

      String flatPaths = (String) rendererClass.getMethod("renderFlatPaths").invoke(renderer);
      System.out.println(flatPaths);

      System.out.println("\n" + SEPARATOR_LINE);
      System.out.println("End of ByteBuf Flow Tracker Report");
      System.out.println(SEPARATOR_LINE + "\n");

    } catch (ClassNotFoundException e) {
      // Tracker not available - agent not running or not in classpath
      System.out.println("\nByteBuf Flow Tracker not available (agent not running)");
    } catch (Exception e) {
      System.err.println("Error generating ByteBuf flow report: " + e.getMessage());
      e.printStackTrace();
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
