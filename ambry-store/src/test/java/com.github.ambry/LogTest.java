package com.github.ambry;

import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;
import java.util.RandomAccess;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/24/13
 * Time: 1:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogTest {

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  @Test
  public void logBasicTest() throws IOException {
    File tempFile = tempFile();
    RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
    // preallocate file
    randomFile.setLength(5000);
    Log logTest = new Log(tempFile.getParent());
    byte[] testbuf = new byte[1000];
    new Random().nextBytes(testbuf);
    // append to log from byte buffer
    int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
    Assert.assertEquals(written, 1000);
    Assert.assertEquals(logTest.sizeInBytes(), 1000);
    // append to log from channel
    long writtenFromChannel = logTest.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(testbuf))), 1000);
    Assert.assertEquals(writtenFromChannel, 1000);
    Assert.assertEquals(logTest.sizeInBytes(), 2000);
    written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
    Assert.assertEquals(written, 1000);
    Assert.assertEquals(logTest.sizeInBytes(), 3000);
    ByteBuffer result = ByteBuffer.allocate(3000);
    logTest.readInto(result, 0);
    byte[] expectedAns = new byte[3000];
    System.arraycopy(testbuf, 0, expectedAns, 0, 1000);
    System.arraycopy(testbuf, 0, expectedAns, 1000, 1000);
    System.arraycopy(testbuf, 0, expectedAns, 2000, 1000);
    Assert.assertArrayEquals(result.array(), expectedAns);

    // read arbitrary offsets from the log and ensure they are consistent
    result.clear();
    result.limit(1000);
    logTest.readInto(result, 0);
    Assert.assertEquals(result.limit(), 1000);
    result.limit(2000);
    logTest.readInto(result, 1000);
    Assert.assertEquals(result.limit(), 2000);
    result.limit(3000);
    logTest.readInto(result, 2000);
    Assert.assertEquals(result.limit(), 3000);
    Assert.assertArrayEquals(result.array(), expectedAns);

    // flush the file and ensure the write offset is different from file size
    logTest.flush();
    Assert.assertEquals(randomFile.length(), 5000);
    Assert.assertEquals(logTest.sizeInBytes(), 3000);
    tempFile.delete();
  }
}
