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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.junit.Assert;


/**
 * A class consisting of common util methods useful for tests.
 */
public class TestUtils {
  public static final Random RANDOM = new Random();

  /**
   * Return the number of threads currently running with a name containing the given pattern.
   * @param pattern the pattern to compare
   * @return the number of threads currently running with a name containing the given pattern.
   */
  public static int numThreadsByThisName(String pattern) {
    int count = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains(pattern)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Return the thread with a name that contains the given name. If there are multiple such threads,
   * return the first such thread.
   * @param pattern the pattern to compare
   * @return the first thread with a name that contains the given pattern.
   */
  public static Thread getThreadByThisName(String pattern) {
    Thread thread = null;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains(pattern)) {
        thread = t;
        break;
      }
    }
    return thread;
  }

  /**
   * Gets a byte array of length {@code size} with random bytes.
   * @param size the required length of the random byte array.
   * @return a byte array of length {@code size} with random bytes.
   */
  public static byte[] getRandomBytes(int size) {
    byte[] bytes = new byte[size];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /**
   * Awaits on the passed-in {@link CountDownLatch}. If times out throws an exception.
   * @param latch The latch to await on.
   * @param timeoutMs Timeout in millisecond.
   * @throws TimeoutException If awaits for more than the specified time, throw a {@link TimeoutException}.
   * @throws InterruptedException If wait is interrupted.
   */
  public static void awaitLatchOrTimeout(CountDownLatch latch, long timeoutMs)
      throws TimeoutException, InterruptedException {
    if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Too long time to complete operation.");
    }
  }

  /**
   * Succeed if the {@code body} throws an exception of type {@code exceptionClass}, otherwise fail.
   * @param exceptionClass the type of exception that should occur.
   * @param body the body to execute. This should throw an exception of type {@code exceptionClass}
   * @param errorAction if non-null and the exception class matches, execute this action.
   */
  public static <E extends Exception> void assertException(Class<E> exceptionClass, ThrowingRunnable body,
      ThrowingConsumer<E> errorAction) throws Exception {
    try {
      body.run();
      Assert.fail("Should have thrown exception");
    } catch (Exception e) {
      if (exceptionClass.isInstance(e)) {
        if (errorAction != null) {
          errorAction.accept(exceptionClass.cast(e));
        }
      } else {
        throw e;
      }
    }
  }


  /**
   * Similar to {@link Runnable}, but able to throw checked exceptions.
   */
  public interface ThrowingRunnable {

    /**
     * Run the action.
     * @throws Exception
     */
    void run() throws Exception;
  }

  /**
   * Similar to {@link Consumer}, but able to throw checked exceptions.
   * @param <T> the type of the input to the operation
   */
  public interface ThrowingConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     */
    void accept(T t) throws Exception;
  }
}
