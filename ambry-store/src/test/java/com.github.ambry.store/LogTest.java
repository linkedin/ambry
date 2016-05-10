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
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.util.Random;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;


public class LogTest {

  /**
   * Create a temporary file
   */
  File tempFile()
      throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  @Test
  public void logBasicTest() {
    try {
      File tempFile = tempFile();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      File logFile = new File(tempFile.getParent(), "log_current");
      logFile.deleteOnExit();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log logTest = new Log(tempFile.getParent(), 5000, new StoreMetrics(tempFile.getParent(), new MetricRegistry()));
      byte[] testbuf = new byte[1000];
      new Random().nextBytes(testbuf);
      // append to log from byte buffer
      int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
      Assert.assertEquals(written, 1000);
      Assert.assertEquals(logTest.getLogEndOffset(), 1000);
      // append to log from channel
      logTest.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(testbuf))), 1000);
      Assert.assertEquals(logTest.getLogEndOffset(), 2000);
      written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
      Assert.assertEquals(written, 1000);
      Assert.assertEquals(logTest.getLogEndOffset(), 3000);
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
      Assert.assertEquals(logTest.getLogEndOffset(), 3000);
      tempFile.delete();
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void logAppendTest() {
    try {
      File tempFile = tempFile();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      File logFile = new File(tempFile.getParent(), "log_current");
      logFile.deleteOnExit();
      // preallocate file
      randomFile.setLength(5000);
      MetricRegistry registry = new MetricRegistry();
      Log logTest = new Log(tempFile.getParent(), 5000, new StoreMetrics(tempFile.getParent(), registry));
      byte[] testbuf = new byte[2000];
      new Random().nextBytes(testbuf);
      // append to log from byte buffer
      int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
      Assert.assertEquals(written, 2000);
      Assert.assertEquals(logTest.getLogEndOffset(), 2000);
      // append to log from channel
      logTest.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(testbuf))), 2000);
      Assert.assertEquals(logTest.getLogEndOffset(), 4000);

      // write more and verify we fail to write
      try {
        logTest.appendFrom(ByteBuffer.wrap(testbuf));
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(registry.getCounters().
            get("com.github.ambry.store.Log." + tempFile.getParent() + "-overflowWriteError").getCount(), 1);
      }

      // append to log from buffer and check overflow
      // write more and verify we fail to write
      try {
        logTest.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(testbuf))), 2000);
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(registry.getCounters().
            get("com.github.ambry.store.Log." + tempFile.getParent() + "-overflowWriteError").getCount(), 2);
      }

      logTest.close();
      // ensure we fail to append
      try {
        logTest.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(testbuf))), 1000);
        Assert.assertTrue(false);
      } catch (ClosedChannelException e) {
        Assert.assertTrue(true);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  @Test
  public void logReadTest() {
    try {
      File tempFile = tempFile();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      File logFile = new File(tempFile.getParent(), "log_current");
      logFile.deleteOnExit();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log logTest = new Log(tempFile.getParent(), 5000, new StoreMetrics(tempFile.getParent(), new MetricRegistry()));
      byte[] testbuf = new byte[2000];
      new Random().nextBytes(testbuf);
      // append to log from byte buffer
      int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
      Assert.assertEquals(written, 2000);
      Assert.assertEquals(logTest.getLogEndOffset(), 2000);

      ByteBuffer buffer = ByteBuffer.allocate(1000);
      logTest.readInto(buffer, 0);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(buffer.array()[i], testbuf[i]);
      }
      buffer.clear();
      logTest.readInto(buffer, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(buffer.array()[i], testbuf[i + 1000]);
      }
      try {
        buffer.clear();
        logTest.readInto(buffer, 5000);
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
      buffer.clear();
      try {
        logTest.readInto(buffer, 5000);
        Assert.assertFalse(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
    } catch (Exception e) {
      Assert.assertEquals(true, false);
    }
  }

  @Test
  public void setOffsetTest() {
    try {
      File tempFile = tempFile();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      File logFile = new File(tempFile.getParent(), "log_current");
      logFile.deleteOnExit();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log logTest = new Log(tempFile.getParent(), 5000, new StoreMetrics(tempFile.getParent(), new MetricRegistry()));
      byte[] testbuf = new byte[2000];
      new Random().nextBytes(testbuf);
      // append to log from byte buffer
      int written = logTest.appendFrom(ByteBuffer.wrap(testbuf));
      Assert.assertEquals(written, 2000);
      Assert.assertEquals(logTest.getLogEndOffset(), 2000);
      logTest.setLogEndOffset(4000);
      Assert.assertEquals(logTest.getLogEndOffset(), 4000);
      try {
        logTest.setLogEndOffset(6000);
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
    } catch (Exception e) {
      Assert.assertEquals(true, false);
    }
  }
}
