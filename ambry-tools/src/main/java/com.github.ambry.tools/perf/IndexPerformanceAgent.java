package com.github.ambry.tools.perf;

import java.lang.instrument.Instrumentation;

/**
 * Used for instrumentation
 */
public class IndexPerformanceAgent {
  private static Instrumentation instrumentation;

  public static void premain(String args, Instrumentation inst) {
    instrumentation = inst;
  }

  public static long getObjectSize(Object o) {
    return instrumentation.getObjectSize(o);
  }
}
