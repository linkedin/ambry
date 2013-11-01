package com.github.ambry.store;

import org.junit.Assert;
import org.junit.Test;
import com.github.ambry.utils.ByteBufferOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;

public class BlobMessageReadSetTest {

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  @Test
  public void testMessageRead() throws IOException {
    BlobReadOptions readOptions1 = new BlobReadOptions(500, 30);
    BlobReadOptions readOptions2 = new BlobReadOptions(100, 15);
    BlobReadOptions readOptions3 = new BlobReadOptions(200, 100);
    BlobReadOptions[] options = new BlobReadOptions[3];
    options[0] = readOptions1;
    options[1] = readOptions2;
    options[2] = readOptions3;
    File tempFile = tempFile();
    RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
    // preallocate file
    randomFile.setLength(5000);
    Log logTest = new Log(tempFile.getParent());
    byte[] testbuf = new byte[3000];
    new Random().nextBytes(testbuf);
    // append to log from byte buffer
    int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
    Assert.assertEquals(written, 3000);
    MessageReadSet readSet = new BlobMessageReadSet(tempFile, randomFile.getChannel(), options, logTest.sizeInBytes());
    Assert.assertEquals(readSet.count(), 3);
    Assert.assertEquals(readSet.sizeInBytes(0), 15);
    Assert.assertEquals(readSet.sizeInBytes(1), 100);
    Assert.assertEquals(readSet.sizeInBytes(2), 30);
    ByteBuffer buf = ByteBuffer.allocate(3000);
    ByteBufferOutputStream stream = new ByteBufferOutputStream(buf);
    readSet.writeTo(0, Channels.newChannel(stream), 0, 15);
    Assert.assertEquals(buf.position(), 15);
    buf.flip();
    for (int i = 100; i < 115; i++) {
      Assert.assertEquals(buf.get(), testbuf[i]);
    }

    buf.flip();
    readSet.writeTo(0, Channels.newChannel(stream), 5, 1000);
    Assert.assertEquals(buf.position(), 10);
    buf.flip();
    for (int i = 105; i < 115; i++) {
      Assert.assertEquals(buf.get(), testbuf[i]);
    }

    // do similarly for index 2
    buf.clear();
    readSet.writeTo(1, Channels.newChannel(stream), 0, 100);
    Assert.assertEquals(buf.position(), 100);
    buf.flip();
    for (int i = 200; i < 300; i++) {
      Assert.assertEquals(buf.get(), testbuf[i]);
    }
  }
}
