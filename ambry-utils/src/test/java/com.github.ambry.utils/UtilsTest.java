package com.github.ambry.utils;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import java.util.Random;

import static org.junit.Assert.assertTrue;


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
  public void testReadStrings()
      throws IOException {
    // good case
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.putShort((short) 8);
    String s = getRandomString(8);
    buffer.put(s.getBytes());
    buffer.flip();
    String outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertEquals(s, outputString);

    // failed case
    buffer.flip();
    buffer.putShort((short) 10);
    buffer.put(s.getBytes());
    buffer.flip();
    try {
      outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
    buffer.flip();
    buffer.putShort((short) -1);
    buffer.flip();
    outputString = Utils.readShortString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertNull(outputString);

    // good case
    buffer = ByteBuffer.allocate(40004);
    buffer.putInt(40000);
    s = getRandomString(40000);
    buffer.put(s.getBytes());
    buffer.flip();
    outputString = Utils.readIntString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertEquals(s, outputString);

    // failed case
    buffer.flip();
    buffer.putInt(50000);
    buffer.put(s.getBytes());
    buffer.flip();
    try {
      outputString = Utils.readIntString(new DataInputStream(new ByteBufferInputStream(buffer)));
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
    buffer.flip();
    buffer.putInt(-1);
    buffer.flip();
    outputString = Utils.readIntString(new DataInputStream(new ByteBufferInputStream(buffer)));
    Assert.assertNull(outputString);
  }

  @Test
  public void testReadBuffers()
      throws IOException {
    byte[] buf = new byte[40004];
    new Random().nextBytes(buf);
    ByteBuffer inputBuf = ByteBuffer.wrap(buf);
    inputBuf.putInt(0, 40000);
    ByteBuffer outputBuf = Utils.readIntBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
    for (int i = 0; i < 40000; i++) {
      Assert.assertEquals(buf[i + 4], outputBuf.array()[i]);
    }

    buf = new byte[10];
    new Random().nextBytes(buf);
    inputBuf = ByteBuffer.wrap(buf);
    inputBuf.putShort(0, (short) 8);
    outputBuf = Utils.readShortBuffer(new DataInputStream(new ByteBufferInputStream(inputBuf)));
    for (int i = 0; i < 8; i++) {
      Assert.assertEquals(buf[i + 2], outputBuf.array()[i]);
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
  public void testReadWriteStringToFile()
      throws IOException {
    File file = File.createTempFile("test", "1");
    file.deleteOnExit();
    Utils.writeStringToFile("Test", file.getPath());
    String outputString = Utils.readStringFromFile(file.getPath());
    Assert.assertEquals("Test", outputString);
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
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random random = new Random();

  private String getRandomString(int length) {
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
}
