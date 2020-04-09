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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLException;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests Utils methods
 */
public class UtilsTest {
  static final String STATIC_FIELD_TEST_STRING = "field1";

  @Test(expected = IllegalArgumentException.class)
  public void testGetRandomLongException() {
    Utils.getRandomLong(new Random(), 0);
  }

  public void whpGetRandomLongRangeTest(int range, int draws) {
    // This test is probabilistic in nature if range is greater than one.
    // Make sure draws >> range for test to pass with high probability.
    int[] count = new int[range];
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
    String s = TestUtils.getRandomString(8);
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
    s = TestUtils.getRandomString(40000);
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
  public void testGetByteBufferInputStreamFromCrcStreamShareMemoryWithNettyByteBuf() throws Exception {
    int blobSize = 1000;
    // The first 8 bytes are the size of blob, the next 1000 bytes are the blob content, the next 8 bytes are the crc
    // value, and we do this twice.
    int bufferSize = (Long.SIZE / Byte.SIZE + blobSize + Long.SIZE / Byte.SIZE) * 2;
    byte[] firstRandomBytes = new byte[blobSize];
    byte[] secondRandomBytes = new byte[blobSize];
    new Random().nextBytes(firstRandomBytes);
    new Random().nextBytes(secondRandomBytes);

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    ByteBufferOutputStream bbos = new ByteBufferOutputStream(buffer);

    // Fill the buffer
    byte[] arrayToFill = firstRandomBytes;
    while (arrayToFill != null) {
      CrcOutputStream crcStream = new CrcOutputStream(bbos);
      DataOutputStream dos = new DataOutputStream(crcStream);
      dos.writeLong((long) blobSize);
      dos.write(arrayToFill);
      buffer.putLong(crcStream.getValue());
      arrayToFill = (arrayToFill == firstRandomBytes) ? secondRandomBytes : null;
    }
    buffer.flip();
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.heapBuffer(buffer.remaining());
    try {
      byteBuf.writeBytes(buffer);

      byte[] expectedArray = firstRandomBytes;
      while (expectedArray != null) {
        CrcInputStream cis = new CrcInputStream(new NettyByteBufDataInputStream(byteBuf));
        DataInputStream dis = new DataInputStream(cis);
        long dataSize = dis.readLong();
        assertEquals(dataSize, blobSize);
        ByteBufferInputStream obtained = Utils.getByteBufferInputStreamFromCrcInputStream(cis, (int) dataSize);
        assertEquals(byteBuf.array(), getByteArrayFromByteBuffer(obtained.getByteBuffer()));
        byte[] obtainedArray = new byte[blobSize];
        obtained.read(obtainedArray);
        assertArrayEquals(obtainedArray, expectedArray);
        long crcRead = byteBuf.readLong();
        assertEquals(crcRead, cis.getValue());
        expectedArray = (expectedArray == firstRandomBytes) ? secondRandomBytes : null;
      }
    } finally {
      byteBuf.release();
    }
  }

  /**
   * Return the internal byte array of the given {@link ByteBuffer}. It only works when the {@link ByteBuffer} is not
   * a direct {@link ByteBuffer}.
   * @param buffer The {@link ByteBuffer}.
   * @return The internal byte array.
   * @throws Exception Any unexpected error.
   */
  private byte[] getByteArrayFromByteBuffer(ByteBuffer buffer) throws Exception {
    assertFalse(buffer.isDirect());
    if (buffer.hasArray()) {
      return buffer.array();
    }
    Field arrayField = ByteBuffer.class.getDeclaredField("hb");
    arrayField.setAccessible(true);
    return (byte[]) arrayField.get(buffer);
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
      String value = TestUtils.getRandomString(1000 + TestUtils.RANDOM.nextInt(10000));
      assertEquals("Size mismatch ", Integer.BYTES + value.length(), Utils.getIntStringLength(value));
    }
    assertEquals("Size mismatch for empty string ", Integer.BYTES, Utils.getIntStringLength(""));
    assertEquals("Size mismatch for null string ", Integer.BYTES, Utils.getIntStringLength(null));
  }

  @Test
  public void testSerializeNullableString() {
    String randomString = TestUtils.getRandomString(10);
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
    String randomString = TestUtils.getRandomString(10);
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

    randomString = TestUtils.getRandomString(10) + "Ò";
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

    randomString = TestUtils.getRandomString(10);
    outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length - 1);
    try {
      Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
      Assert.fail("Serialization should have failed due to insufficient space");
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testDeserializeString() {
    String randomString = TestUtils.getRandomString(10);
    ByteBuffer outputBuffer = ByteBuffer.allocate(4 + randomString.getBytes().length);
    Utils.serializeString(outputBuffer, randomString, StandardCharsets.US_ASCII);
    outputBuffer.flip();
    String outputString = Utils.deserializeString(outputBuffer, StandardCharsets.US_ASCII);
    assertEquals("Input and output strings don't match", randomString, outputString);

    randomString = TestUtils.getRandomString(10) + "Ò";
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

    randomString = TestUtils.getRandomString(10);
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
   * Tests {@link Utils#getStaticFieldValuesAsStrings(Class)}.
   */
  @Test
  public void testGetStaticFieldValuesAsStrings() {
    Stream<String> fieldValuesAsStrings = Utils.getStaticFieldValuesAsStrings(MockClassForTesting.class);
    Set<String> staticFields = fieldValuesAsStrings.collect(Collectors.toSet());
    assertTrue("Static field is not found in the result.", staticFields.contains(STATIC_FIELD_TEST_STRING));
    assertEquals("The number of static field strings in result", 1, staticFields.size());
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

    exception = new SSLException("Handshake failure");
    assertFalse("Should not be declared as a client termination", Utils.isPossibleClientTermination(exception));
    exception = new SSLException("SSLEngine closed already");
    assertTrue("Should be declared as a client termination", Utils.isPossibleClientTermination(exception));
  }

  /**
   * Tests for {@link Utils#getTtlInSecsFromExpiryMs(long, long)}.
   */
  @Test
  public void getTtlInSecsFromExpiryMsTest() {
    long creationTimeMs = SystemTime.getInstance().milliseconds();
    for (long ttlInSecs : new long[]{1, 20, 1000, Integer.MAX_VALUE, Utils.Infinite_Time, -100,
        -(TimeUnit.MILLISECONDS.toSeconds(creationTimeMs) + 1)}) {
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
    assertFalse("String should not be declared empty", Utils.isNullOrEmpty(TestUtils.getRandomString(10)));
  }

  /**
   * Tests for {@link Utils#compareTimes(long, long)}.
   */
  @Test
  public void compareTimesTest() {
    assertEquals("Infinite_Time should be equal to Infinite_Time", 0,
        Utils.compareTimes(Utils.Infinite_Time, Utils.Infinite_Time));
    assertEquals("Infinite_Time should be greater than any finite time.", 1,
        Utils.compareTimes(Utils.Infinite_Time, Long.MAX_VALUE));
    assertEquals("Any finite time should be less than any Infinite_Time.", -1,
        Utils.compareTimes(Long.MAX_VALUE, Utils.Infinite_Time));
    assertEquals("Wrong comparison result for finite times", 0, Utils.compareTimes(25, 25));
    assertEquals("Wrong comparison result for finite times", -1, Utils.compareTimes(24, 25));
    assertEquals("Wrong comparison result for finite times", 1, Utils.compareTimes(25, 24));
  }

  /**
   * Tests for {@link Utils#splitString(String, String)}.
   */
  @Test
  public void splitStringTest() {
    assertEquals("Unexpected result", new ArrayList<>(Arrays.asList("a", "b", "c")), Utils.splitString("a,b,c", ","));
    assertEquals("Empty string should return empty list", new ArrayList<>(), Utils.splitString("", ","));
    assertEquals("Empty segments should be ignored", new ArrayList<>(Arrays.asList("a", "b-extra", "c")),
        Utils.splitString(",a,,b-extra,c,,", ","));
  }

  /**
   * Tests for {@link Utils#splitString(String, String, Supplier)}.
   */
  @Test
  public void splitStringForCollectionTest() {
    assertEquals("Unexpected result", new ArrayList<>(Arrays.asList("a", "b", "c")),
        Utils.splitString("a,b,c", ",", ArrayList::new));
    assertEquals("Unexpected result", new HashSet<>(Arrays.asList("a", "b", "c")),
        Utils.splitString("a,b,c", ",", HashSet::new));
    assertEquals("Empty string should return empty list", new ArrayList<>(),
        Utils.splitString("", ",", ArrayList::new));
    assertEquals("Empty segments should be ignored", new ArrayList<>(Arrays.asList("a", "b-extra", "c")),
        Utils.splitString(",a,,b-extra,c,,", ",", ArrayList::new));
  }
}

class MockClassForTesting {

  public boolean noArgConstructorInvoked = false;
  public boolean oneArgConstructorInvoked = false;
  public boolean twoArgConstructorInvoked = false;
  public boolean threeArgConstructorInvoked = false;
  public boolean fourArgConstructorInvoked = false;
  public static String STATIC_FIELD = UtilsTest.STATIC_FIELD_TEST_STRING;

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
