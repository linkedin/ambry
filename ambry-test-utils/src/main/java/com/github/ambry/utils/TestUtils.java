/*
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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;


/**
 * A class consisting of common util methods useful for tests.
 */
public class TestUtils {
  public static final long TTL_SECS = TimeUnit.DAYS.toSeconds(7);
  public static final Random RANDOM = new Random();
  public static final List<Boolean> BOOLEAN_VALUES = Collections.unmodifiableList(Arrays.asList(true, false));
  private static final int CHECK_INTERVAL_IN_MS = 100;
  private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

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
   * Return all the threads with a name that contains the given name.
   * @param pattern the pattern to compare
   * @return all the threads with a name that contains the given pattern.
   */
  public static List<Thread> getAllThreadsByThisName(String pattern) {
    List<Thread> threads = new ArrayList<>();
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains(pattern)) {
        threads.add(t);
      }
    }
    return threads;
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
   * Asserts that {@code actual} and {@code expect} are equal. Checks that {@code actual}
   * contains no extra data if {@code checkActualComplete} is {@code true}.
   */
  public static void assertInputStreamEqual(InputStream expect, InputStream actual, int size,
      boolean checkActualComplete) throws IOException {
    byte[] actualBuf = Utils.readBytesFromStream(actual, size);
    if (checkActualComplete) {
      int finalRead = actual.read();
      // some InputStream impls in Ambry return 0 instead of -1 when they end
      assertTrue("Actual stream had more bytes than expected", finalRead == 0 || finalRead == -1);
    }
    byte[] expectBuf = Utils.readBytesFromStream(expect, size);
    assertArrayEquals("Data from actual stream does not match expected", expectBuf, actualBuf);
  }

  /**
   * Verify that the {@code inputStream} satisfies basic properties of the contract.
   * @param inputStream
   * @throws Exception
   */
  public static void validateInputStreamContract(InputStream inputStream) throws Exception {
    int numBytes = 8;
    byte[] bytes = new byte[numBytes];
    assertException(NullPointerException.class, () -> inputStream.read(null, 0, 5), null);
    assertException(IndexOutOfBoundsException.class, () -> inputStream.read(bytes, -1, 5), null);
    assertException(IndexOutOfBoundsException.class, () -> inputStream.read(bytes, 0, -1), null);
    assertException(IndexOutOfBoundsException.class, () -> inputStream.read(bytes, numBytes, 1), null);
    assertException(IndexOutOfBoundsException.class, () -> inputStream.read(bytes, 1, numBytes), null);
    Assert.assertEquals(0, inputStream.read(bytes, 0, 0));
  }

  /**
   * Read through the {@code inputStream} using the no-arg read method until {@code -1} is returned,
   * and verify that the expected number of bytes {@code expectedLength} is read.
   * @param inputStream
   * @param expectedLength
   * @throws IOException
   */
  public static void readInputStreamAndValidateSize(InputStream inputStream, long expectedLength) throws IOException {
    int readVal = 0;
    long numRead = 0;
    do {
      readVal = inputStream.read();
      numRead++;
    } while (readVal != -1);
    numRead--;
    Assert.assertEquals("Unexpected inputstream read length", expectedLength, numRead);
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

  public static String getRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  /**
   * A wrapper class to start and shutdown {@link ZooKeeperServer}. The code is from {@link org.I0Itec.zkclient.ZkServer}.
   * We maintain this class to speed up tests because function calls to NetworkUtil.getLocalHostNames() in
   * {@link org.I0Itec.zkclient.ZkServer} takes time in Mac OS.
   * {@link org.I0Itec.zkclient.ZkServer} calls NetworkUtil.getLocalHostNames() to log and make sure "localhost" is in
   * the list of NetworkUtil.getLocalHostNames(), which are not necessary in tests.
   */
  static class ZkServerWrapper {
    private static final Logger logger = LoggerFactory.getLogger(ZkServerWrapper.class);
    private ZooKeeperServer zk;
    private NIOServerCnxnFactory nioFactory;
    private int port;
    private File dataDir;
    private File dataLogDir;

    public ZkServerWrapper(String dataDir, String logDir, int port) {
      this.dataDir = new File(dataDir);
      this.dataLogDir = new File(logDir);
      this.dataDir.mkdirs();
      this.dataLogDir.mkdirs();
      this.port = port;
    }

    public void start() {
      try {
        zk = new ZooKeeperServer(dataDir, dataLogDir, ZkServer.DEFAULT_TICK_TIME);
        zk.setMinSessionTimeout(ZkServer.DEFAULT_MIN_SESSION_TIMEOUT);
        nioFactory = new NIOServerCnxnFactory();
        int maxClientConnections = 0; // 0 means unlimited
        nioFactory.configure(new InetSocketAddress(port), maxClientConnections);
        nioFactory.startup(zk);
      } catch (IOException e) {
        throw new ZkException("Unable to start single ZooKeeper server.", e);
      } catch (InterruptedException e) {
        throw new ZkInterruptedException(e);
      }
      logger.info("ZooKeeperServer started successfully.");
    }

    public void shutdown() {
      logger.info("Shutting down ZkServer...");
      if (nioFactory != null) {
        nioFactory.shutdown();
        try {
          nioFactory.join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        nioFactory = null;
      }
      if (zk != null) {
        zk.shutdown();
        zk = null;
      }
      logger.info("Shutting down ZooKeeperServer...done");
    }
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
    private ZkServerWrapper zkServer;

    /**
     * Instantiate by starting a Zk server.
     * @param tempDirPath the temporary directory string to use.
     * @param dcName the name of the datacenter.
     * @param id the id of the datacenter.
     * @param port the port at which this Zk server should run on localhost.
     */
    public ZkInfo(String tempDirPath, String dcName, byte id, int port, boolean start) {
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
      // start zookeeper
      zkServer = new ZkServerWrapper(dataDir, logDir, port);
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
   * Periodically check expectedValue and actualValue until timeout.
   * @param expectedValue the expected value.
   * @param expressionToCheck the expression to check.
   * @param timeoutInMs the time out in millisecond.
   * @return true if value match.
   */
  public static <T> boolean checkAndSleep(T expectedValue, Supplier expressionToCheck, int timeoutInMs) {
    long startTime = System.currentTimeMillis();
    try {
      while (!Objects.equals(expectedValue, expressionToCheck.get())) {
        if (System.currentTimeMillis() - startTime >= timeoutInMs) {
          return false;
        }
        Thread.sleep(CHECK_INTERVAL_IN_MS);
      }
    } catch (InterruptedException e) {
      return false;
    }
    return true;
  }
}

