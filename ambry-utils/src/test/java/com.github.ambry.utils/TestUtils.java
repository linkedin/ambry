package com.github.ambry.utils;

/**
 * A class consisting of common util methods useful for tests.
 */
public class TestUtils {
  /**
   * Return the number of threads currently running with the given name.
   * @param name the name to compare
   * @return the number of threads currently running with the given name.
   */
  public static int numThreadsByThisName(String name) {
    int count = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().equals(name)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Return the thread with the given name. If there are multiple such threads, return the first thread by this name.
   * @param name the name to compare
   * @return the first thread with the given name.
   */
  public static Thread getThreadByThisName(String name) {
    Thread thread = null;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().equals(name)) {
        thread = t;
        break;
      }
    }
    return thread;
  }
}
