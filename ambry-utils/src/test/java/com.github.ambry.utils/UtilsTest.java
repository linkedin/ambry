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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests Utils methods
 */
public class UtilsTest {

  @Test(expected = IllegalArgumentException.class)
  public void testGetRandomLongException() {
    Utils.getRandomLong(new Random(), 0);
  }

  public void whpGetRandomLongRangeTest(int range, int draws) {
    // This test is probabilistic in nature if range is greater than one.
    // Make sure draws >> range for test to pass with high probability.
    int count[] = new int[range];
    Random random = new Random();
    for (int i = 0; i < draws; i++) {
      long r = Utils.getRandomLong(random, range);
      assertTrue(r >= 0);
      assertTrue(r < range);
      count[(int) r] = count[(int) r] + 1;
    }
    for (int i = 0; i < range; i++) {
      assertTrue(count[i] > 0);
    }
  }

  @Test
  public void testGetRandom() {
    whpGetRandomLongRangeTest(1, 1);
    whpGetRandomLongRangeTest(2, 1000);
    whpGetRandomLongRangeTest(3, 1000);
    whpGetRandomLongRangeTest(31, 100 * 1000);
    whpGetRandomLongRangeTest(99, 100 * 1000);
    whpGetRandomLongRangeTest(100, 100 * 1000);
  }

  @Test
  public void testReadStrings() throws IOException {
    // good case
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.putShort((short) 8);
    String s = getRandomString(8);
    buffer.put(s.getBytes());
    buffer.flip();
    String outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertEquals(s, outputString);
    // 0-length
    buffer.rewind();
    buffer.putShort(0, (short) 0);
    outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertTrue(outputString.isEmpty());

    // failed case
    buffer.rewind();
    buffer.putShort((short) 10);
    buffer.put(s.getBytes());
    buffer.flip();
    try {
      outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
    buffer.rewind();
    buffer.putShort(0, (short) -1);
    try {
      outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
      Assert.fail("Should have encountered exception with negative length.");
    } catch (IllegalArgumentException e) {
    }

    // good case
    buffer = ByteBuffer.allocate(40004);
    buffer.putInt(40000);
    s = getRandomString(40000);
    buffer.put(s.getBytes());
    buffer.flip();
    outputString = Utils.readIntString(new DataInputStream(new ByteBufferInputStream(buffer)), StandardCharsets.UTF_8);
    Assert.assertEquals(s, outputString);
    // 0-length
    buffer.rewind();
    buffer.putInt(0, 0);
    outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertTrue(outputString.isEmpty());

    // failed case
    buffer.rewind();
    buffer.putInt(50000);
    buffer.put(s.getBytes());
    buffer.flip();
    try {
      outputString =
          Utils.readIntString(new DataInputStream(new ByteBufferInputStream(buffer)), StandardCharsets.UTF_8);
      fail("Should have failed");
    } catch (IllegalArgumentException e) {
      // expected.
    }

    buffer.rewind();
    buffer.putInt(0, -1);
    try {
      Utils.readIntString(new DataInputStream(new ByteBufferInputStream(buffer)), StandardCharsets.UTF_8);
      Assert.fail("Should have encountered exception with negative length.");
    } catch (IllegalArgumentException e) {
      // expected.
    }
  }

  @Test
  public void testReadBuffers() throws IOException {
    byte[] buf = new byte[40004];
    new Random().nextBytes(buf);
    ByteBuffer inputBuf = ByteBuffer.wrap(buf);
    inputBuf.putInt(0, 40000);
    ByteBuffer outputBuf = Utils.readIntBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
    for (int i = 0; i < 40000; i++) {
      Assert.assertEquals(buf[i + 4], outputBuf.array()[i]);
    }
    // 0 size
    inputBuf.rewind();
    inputBuf.putInt(0, 0);
    outputBuf = Utils.readIntBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
    Assert.assertEquals("Output should be of length 0", 0, outputBuf.array().length);
    // negative size
    inputBuf.rewind();
    inputBuf.putInt(0, -1);
    try {
      Utils.readIntBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
      Assert.fail("Should have encountered exception with negative length.");
    } catch (IllegalArgumentException e) {
    }

    buf = new byte[10];
    new Random().nextBytes(buf);
    inputBuf = ByteBuffer.wrap(buf);
    inputBuf.putShort(0, (short) 8);
    outputBuf = Utils.readShortBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
    for (int i = 0; i < 8; i++) {
      Assert.assertEquals(buf[i + 2], outputBuf.array()[i]);
    }
    // 0 size
    inputBuf.rewind();
    inputBuf.putShort(0, (short) 0);
    outputBuf = Utils.readShortBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
    Assert.assertEquals("Output should be of length 0", 0, outputBuf.array().length);
    // negative size
    inputBuf.rewind();
    inputBuf.putShort(0, (short) -1);
    try {
      Utils.readShortBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
      Assert.fail("Should have encountered exception with negative length.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testHashCode() {
    String s = "test";
    Integer i = new Integer(245);
    Double d = new Double(2.4);
    Object[] objs = new Object[3];
    objs[0] = s;
    objs[1] = i;
    objs[2] = d;
    int code1 = Utils.hashcode(objs);
    int code2 = Utils.hashcode(objs);
    Assert.assertEquals(code1, code2);
  }

  @Test
  public void testReadWriteStringToFile() throws IOException {
    File file = File.createTempFile("test", "1");
    file.deleteOnExit();
    Utils.writeStringToFile("Test", file.getPath());
    String outputString = Utils.readStringFromFile(file.getPath());
    Assert.assertEquals("Test", outputString);
  }

  @Test
  public void testReadFileToByteBuffer() throws IOException {
    File file = File.createTempFile("test", "1");
    file.deleteOnExit();
    FileChannel fileChannel = Utils.openChannel(file, false);
    byte[] referenceBytes = new byte[20];
    new Random().nextBytes(referenceBytes);
    FileUtils.writeByteArrayToFile(file, referenceBytes);

    // fill up fresh byteBuffer
    ByteBuffer buffer = ByteBuffer.allocate(20);
    Utils.readFileToByteBuffer(fileChannel, 0, buffer);
    assertArrayEquals("Data mismatch", referenceBytes, buffer.array());

    // write to byteBuffer based on buffer remaining
    buffer.limit(10);
    buffer.position(0);
    assertEquals("buffer remaining should be 10", 10, buffer.remaining());
    Utils.readFileToByteBuffer(fileChannel, 10, buffer);
    assertEquals("buffer remaining should be 0", 0, buffer.remaining());
    for (int i = 0; i < 10; i++) {
      assertEquals("First 10 bytes in buffer should match last 10 bytes in file", buffer.array()[i],
          referenceBytes[i + 10]);
    }

    // byteBuffer.remaining() + starting offset > file size, exception is expected.
    buffer.clear();
    assertEquals("buffer remaining should be 20", 20, buffer.remaining());
    try {
      Utils.readFileToByteBuffer(fileChannel, 1, buffer);
      fail("Should fail");
    } catch (IOException e) {
    }

    // starting offset exceeds file size, exception is expected.
    buffer.clear();
    assertEquals("buffer remaining should be 20", 20, buffer.remaining());
    try {
      Utils.readFileToByteBuffer(fileChannel, 21, buffer);
      fail("Should fail");
    } catch (IOException e) {
    }
  }

  @Test
  public void testGetIntStringLength() {
    for (int i = 0; i < 10; i++) {
      String value = getRandomString(1000 + TestUtils.RANDOM.nextInt(10000));
      assertEquals("Size mismatch ", Integer.BYTES + value.length(), Utils.getIntStringLength(value));
    }
    assertEquals("Size mismatch for empty string ", Integer.BYTES, Utils.getIntStringLength(""));
    assertEquals("Size mismatch for null string ", Integer.BYTES, Utils.getIntStringLength(null));
  }

  @Test
  public void testSerializeNullableString() {
    String randomString = getRandomString(10);
    ByteBuffer outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    Utils.serializeNullableString(outputBuffer, randomString);
    outputBuffer.flip();
    int length = outputBuffer.getInt();
    assertEquals("Input and output string lengths don't match ", randomString.getBytes().length, length);
    byte[] output = new byte[length];
    outputBuffer.get(output);
    assertFalse("Output buffer shouldn't have any remaining, but has " + outputBuffer.remaining() + " bytes",
        outputBuffer.hasRemaining());
    String outputString = new String(output);
    assertEquals("Input and output strings don't match", randomString, outputString);

    randomString = null;
    outputBuffer = ByteBuffer.allocate(4);
    Utils.serializeNullableString(outputBuffer, randomString);
    outputBuffer.flip();
    length = outputBuffer.getInt();
    assertEquals("Input and output string lengths don't match", 0, length);
    output = new byte[length];
    outputBuffer.get(output);
    assertFalse("Output buffer shouldn't have any remaining, but has " + outputBuffer.remaining() + " bytes",
        outputBuffer.hasRemaining());
    outputString = new String(output);
    assertEquals("Output string \"" + outputString + "\" expected to be empty", outputString, "");
  }

  @Test
  public void testSerializeString() {
    String randomString = getRandomString(10);
    ByteBuffer outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    int length = outputBuffer.getInt();
    assertEquals("Input and output string lengths don't match", randomString.getBytes().length, length);
    byte[] output = new byte[length];
    outputBuffer.get(output);
    assertFalse("Output buffer shouldn't have any remaining, but has " + outputBuffer.remaining() + " bytes",
        outputBuffer.hasRemaining());
    String outputString = new String(output);
    assertEquals("Input and output strings don't match", randomString, outputString);

    randomString = getRandomString(10) + "Ò";
    outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    length = outputBuffer.getInt();
    assertEquals("Input and output string lengths don't match ", (randomString.getBytes().length - 1), length);
    output = new byte[length];
    outputBuffer.get(output);
    assertFalse("Output buffer shouldn't have any remaining, but has " + outputBuffer.remaining() + " bytes",
        outputBuffer.hasRemaining());
    outputString = new String(output);
    randomString = randomString.substring(0, randomString.length() - 1) + "?";
    assertEquals("Input and output strings don't match", randomString, outputString);

    randomString = "";
    outputBuffer = ByteBuffer.allocate(4);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    length = outputBuffer.getInt();
    assertEquals("Input and output string lengths don't match", 0, length);
    output = new byte[length];
    outputBuffer.get(output);
    assertFalse("Output buffer shouldn't have any remaining, but has " + outputBuffer.remaining() + " bytes",
        outputBuffer.hasRemaining());
    outputString = new String(output);
    assertEquals("Output string \"" + outputString + "\" expected to be empty", outputString, "");

    randomString = getRandomString(10);
    outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length - 1);
    try {
      Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
      Assert.fail("Serialization should have failed due to insufficient space");
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testDeserializeString() {
    String randomString = getRandomString(10);
    ByteBuffer outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    String outputString = Utils.deserializeString(outputBuffer, StandardCharsets.US_ASCII);
    assertEquals("Input and output strings don't match", randomString, outputString);

    randomString = getRandomString(10) + "Ò";
    outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    outputString = Utils.deserializeString(outputBuffer, StandardCharsets.US_ASCII);
    randomString = randomString.substring(0, randomString.length() - 1) + "?";
    assertEquals("Input and output strings don't match", randomString, outputString);

    randomString = "";
    outputBuffer = ByteBuffer.allocate(4);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    outputString = Utils.deserializeString(outputBuffer, StandardCharsets.US_ASCII);
    assertEquals("Output string \"" + outputString + "\" expected to be empty", outputString, "");

    randomString = getRandomString(10);
    outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    outputBuffer.putInt(12);
    outputBuffer.put(randomString.getBytes());
    outputBuffer.flip();
    try {
      outputString = Utils.deserializeString(outputBuffer, StandardCharsets.US_ASCII);
      Assert.fail("Deserialization should have failed " + randomString);
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testGetObj() {
    try {
      MockClassForTesting mockObj = Utils.getObj("com.github.ambry.utils.MockClassForTesting");
      Assert.assertNotNull(mockObj);
      Assert.assertTrue(mockObj.noArgConstructorInvoked);
      mockObj = Utils.getObj("com.github.ambry.utils.MockClassForTesting", new Object());
      Assert.assertNotNull(mockObj);
      Assert.assertTrue(mockObj.oneArgConstructorInvoked);
      mockObj = Utils.getObj("com.github.ambry.utils.MockClassForTesting", new Object(), new Object());
      Assert.assertNotNull(mockObj);
      Assert.assertTrue(mockObj.twoArgConstructorInvoked);
      mockObj = Utils.getObj("com.github.ambry.utils.MockClassForTesting", new Object(), new Object(), new Object());
      Assert.assertNotNull(mockObj);
      Assert.assertTrue(mockObj.threeArgConstructorInvoked);
      mockObj = Utils.getObj("com.github.ambry.utils.MockClassForTesting", new Object(), new Object(), new Object(),
          new Object());
      Assert.assertNotNull(mockObj);
      Assert.assertTrue(mockObj.fourArgConstructorInvoked);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  /**
   * Tests {@link Utils#getRootCause(Throwable)}.
   */
  @Test
  public void getRootCauseTest() {
    int nestingLevel = 5;
    String innerExceptionMsg = "InnerException";
    String outerExceptionMsgBase = "OuterException";
    Exception innerException = new Exception(innerExceptionMsg);
    Exception outerException = null;
    for (int i = 0; i < nestingLevel; i++) {
      outerException = new Exception(outerExceptionMsgBase + "-" + i, innerException);
    }
    assertEquals("Message should that of the innermost exception", innerExceptionMsg,
        Utils.getRootCause(outerException).getMessage());
  }

  /**
   * Test {@link Utils#newScheduler(int, String, boolean)}
   */
  @Test
  public void newSchedulerTest() throws Exception {
    ScheduledExecutorService scheduler = Utils.newScheduler(2, false);
    Future<String> future = scheduler.schedule(new Callable<String>() {
      @Override
      public String call() {
        return Thread.currentThread().getName();
      }
    }, 50, TimeUnit.MILLISECONDS);
    String threadName = future.get(10, TimeUnit.SECONDS);
    assertTrue("Unexpected thread name returned: " + threadName, threadName.startsWith("ambry-scheduler-"));
    scheduler.shutdown();
  }

  /**
   * Test {@link Utils#getTimeInMsToTheNearestSec(long)}
   */
  @Test
  public void getTimeInMsToTheNearestSecTest() {
    long msValue = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
    long expectedMsValue = (msValue / Time.MsPerSec) * Time.MsPerSec;
    assertEquals("Time in Ms to the nearest Sec mismatch ", expectedMsValue, Utils.getTimeInMsToTheNearestSec(msValue));
    msValue = Utils.Infinite_Time;
    assertEquals("Time in Ms to the nearest Sec mismatch ", msValue, Utils.getTimeInMsToTheNearestSec(msValue));
  }

  /**
   * Tests {@link Utils#addSecondsToEpochTime(long, long)}
   */
  @Test
  public void addSecondsToEpochTimeTest() {
    for (int i = 0; i < 5; i++) {
      long epochTimeInMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt(10000);
      long deltaInSecs = TestUtils.RANDOM.nextInt(10000);
      assertEquals("epoch epochTimeInMs mismatch ", epochTimeInMs + (deltaInSecs * Time.MsPerSec),
          Utils.addSecondsToEpochTime(epochTimeInMs, deltaInSecs));
      assertEquals("epoch epochTimeInMs mismatch ", Utils.Infinite_Time,
          Utils.addSecondsToEpochTime(Utils.Infinite_Time, deltaInSecs));
      assertEquals("epoch epochTimeInMs mismatch ", Utils.Infinite_Time,
          Utils.addSecondsToEpochTime(epochTimeInMs, Utils.Infinite_Time));
    }
  }

  /**
   * Tests for {@link Utils#isPossibleClientTermination(Throwable)} and
   * {@link Utils#convertToClientTerminationException(Throwable)}.
   */
  @Test
  public void clientTerminationWrapAndRecognizeTest() {
    Exception exception = new IOException("Connection reset by peer");
    assertTrue("Should be declared as a client termination", Utils.isPossibleClientTermination(exception));

    exception = new IOException("Broken pipe");
    assertTrue("Should be declared as a client termination", Utils.isPossibleClientTermination(exception));

    exception = new IOException("Connection not reset by peer");
    assertFalse("Should not be declared as a client termination", Utils.isPossibleClientTermination(exception));
    exception = Utils.convertToClientTerminationException(exception);
    assertTrue("Should be declared as a client termination", Utils.isPossibleClientTermination(exception));

    exception = new Exception("Connection reset by peer");
    // debatable but this is the current implementation.
    assertFalse("Should not be declared as a client termination", Utils.isPossibleClientTermination(exception));
    exception = Utils.convertToClientTerminationException(exception);
    assertTrue("Should be declared as a client termination", Utils.isPossibleClientTermination(exception));
  }

  /**
   * Tests for {@link Utils#getTtlInSecsFromExpiryMs(long, long)}.
   */
  @Test
  public void getTtlInSecsFromExpiryMsTest() {
    long creationTimeMs = SystemTime.getInstance().milliseconds();
    for (long ttlInSecs : new long[]{1, 20, 1000, Integer.MAX_VALUE, Utils.Infinite_Time, -100, -(
        TimeUnit.MILLISECONDS.toSeconds(creationTimeMs) + 1)}) {
      long expectedTtlSecs = ttlInSecs;
      if (ttlInSecs == Utils.Infinite_Time) {
        expectedTtlSecs = Utils.Infinite_Time;
      } else if (ttlInSecs < 0) {
        expectedTtlSecs = 0;
      }
      long expiresAtMs = Utils.addSecondsToEpochTime(creationTimeMs, ttlInSecs);
      long returnedTtlSecs = Utils.getTtlInSecsFromExpiryMs(expiresAtMs, creationTimeMs);
      assertEquals("TTL not as expected", expectedTtlSecs, returnedTtlSecs);
    }
  }

  /**
   * Tests for {@link Utils#isNullOrEmpty(String)}.
   */
  @Test
  public void isNullOrEmptyTest() {
    assertTrue("String should be declared null", Utils.isNullOrEmpty(null));
    assertTrue("String should be declared empty", Utils.isNullOrEmpty(""));
    assertFalse("String should not be declared empty", Utils.isNullOrEmpty(" "));
    assertFalse("String should not be declared empty", Utils.isNullOrEmpty("a"));
    assertFalse("String should not be declared empty", Utils.isNullOrEmpty(getRandomString(10)));
  }

  private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random random = new Random();

  public static String getRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }
}

class MockClassForTesting {

  public boolean noArgConstructorInvoked = false;
  public boolean oneArgConstructorInvoked = false;
  public boolean twoArgConstructorInvoked = false;
  public boolean threeArgConstructorInvoked = false;
  public boolean fourArgConstructorInvoked = false;

  public MockClassForTesting() {
    noArgConstructorInvoked = true;
  }

  public MockClassForTesting(Object obj1) {
    oneArgConstructorInvoked = true;
  }

  public MockClassForTesting(Object obj1, Object obj2) {
    twoArgConstructorInvoked = true;
  }

  public MockClassForTesting(Object obj1, Object obj2, Object obj3) {
    threeArgConstructorInvoked = true;
  }

  public MockClassForTesting(Object obj1, Object obj2, Object obj3, Object obj4) {
    fourArgConstructorInvoked = true;
  }
}
