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
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class StoreMessageReadSetTest {

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
    File tempFile = tempFile();
    try {
      BlobReadOptions readOptions1 = new BlobReadOptions(500, 30, 1, null);
      BlobReadOptions readOptions2 = new BlobReadOptions(100, 15, 1, null);
      BlobReadOptions readOptions3 = new BlobReadOptions(200, 100, 1, null);
      List<BlobReadOptions> options = new ArrayList<BlobReadOptions>(3);
      options.add(0, readOptions1);
      options.add(1, readOptions2);
      options.add(2, readOptions3);
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      Log logTest =
          new Log(tempFile.getParent(), 5000, 5000, new StoreMetrics(tempFile.getParent(), new MetricRegistry()));
      byte[] testbuf = new byte[3000];
      new Random().nextBytes(testbuf);
      // append to log from byte buffer
      int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
      Assert.assertEquals(written, 3000);
      MessageReadSet readSet =
          new StoreMessageReadSet(tempFile, randomFile.getChannel(), options, logTest.getEndOffset().getOffset());
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

      // verify args
      readOptions1 = new BlobReadOptions(500, 30, 1, null);
      readOptions2 = new BlobReadOptions(100, 15, 1, null);
      readOptions3 = new BlobReadOptions(200, 100, 1, null);
      options = new ArrayList<BlobReadOptions>(3);
      options.add(0, readOptions1);
      options.add(1, readOptions2);
      options.add(2, readOptions3);
      try {
        readSet = new StoreMessageReadSet(tempFile, randomFile.getChannel(), options, 10);
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
      readSet = new StoreMessageReadSet(tempFile, randomFile.getChannel(), options, 1000);
      try {
        readSet.sizeInBytes(4);
        Assert.assertTrue(false);
      } catch (IndexOutOfBoundsException e) {
        Assert.assertTrue(true);
      }
      try {
        readSet.writeTo(4, randomFile.getChannel(), 100, 100);
        Assert.assertTrue(false);
      } catch (IndexOutOfBoundsException e) {
        Assert.assertTrue(true);
      }
    } finally {
      tempFile.delete();
      File logFile = new File(tempFile.getParent(), "log_current");
      logFile.delete();
    }
  }
}
