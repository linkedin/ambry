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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
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
   * Gets a random element from the given array of elements.
   * @param elements the array of elements.
   * @param <T> the type of the elements.
   */
  public static <T> T getRandomElement(T[] elements) {
    return elements[RANDOM.nextInt(elements.length)];
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
   * Waits until the HardDeleter thread is in the {@code expectedState} for the specified {@code timeoutMs} time.
   * @param thread the thread whose state needs to be checked.
   * @param expectedState Expected HardDeleter thread state
   * @param timeoutMs time in ms after which the check is considered failed if {@code expectedState} is not reached.
   */
  public static boolean waitUntilExpectedState(Thread thread, Thread.State expectedState, long timeoutMs)
      throws InterruptedException {
    long timeSoFar = 0;
    while (expectedState != thread.getState()) {
      Thread.sleep(10);
      timeSoFar += 10;
      if (timeSoFar >= timeoutMs) {
        return false;
      }
    }
    return true;
  }

  /**
   * Succeed if the {@code body} throws an exception of type {@code exceptionClass}, otherwise fail.
   * @param exceptionClass the type of exception that should occur.
   * @param body the body to execute. This should throw an exception of type {@code exceptionClass}
   * @param errorAction if non-null and the exception class matches, execute this action.
   * @throws Exception when an unexpected exception occurs.
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
   * Gets a temporary directory with the given prefix. The directory will be deleted when the virtual machine terminates.
   * @param prefix The prefix for the name of the temporary directory.
   * @return The absolute path of the generated temporary directory.
   * @throws IOException
   */
  public static String getTempDir(String prefix) throws IOException {
    File tempDir = Files.createTempDirectory(prefix + RANDOM.nextInt(1000)).toFile();
    tempDir.deleteOnExit();
    return tempDir.getAbsolutePath();
  }

  /**
   * Generates and returns a random Hex String of the specified size
   * @param size expected key hex string size
   * @return the hex string thus generated
   */
  public static String getRandomKey(int size) {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < size) {
      sb.append(Integer.toHexString(TestUtils.RANDOM.nextInt()));
    }
    sb.setLength(size);
    return sb.toString();
  }

  /**
   * A class to initialize and hold information about each Zk Server.
   */
  public static class ZkInfo {
    private String dcName;
    private byte id;
    private int port;
    private String dataDir;
    private String logDir;
    private ZkServer zkServer;

    /**
     * Instantiate by starting a Zk server.
     * @param tempDirPath the temporary directory string to use.
     * @param dcName the name of the datacenter.
     * @param id the id of the datacenter.
     * @param port the port at which this Zk server should run on localhost.
     */
    public ZkInfo(String tempDirPath, String dcName, byte id, int port, boolean start) throws IOException {
      this.dcName = dcName;
      this.id = id;
      this.port = port;
      this.dataDir = tempDirPath + "/dataDir";
      this.logDir = tempDirPath + "/logDir";
      if (start) {
        startZkServer(port, dataDir, logDir);
      }
    }

    private void startZkServer(int port, String dataDir, String logDir) {
      IDefaultNameSpace defaultNameSpace = zkClient -> {
      };
      // start zookeeper
      zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
      zkServer.start();
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getDcName() {
      return dcName;
    }

    public byte getId() {
      return id;
    }

    public void shutdown() {
      if (zkServer != null) {
        zkServer.shutdown();
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