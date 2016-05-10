/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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
