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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link StoreMessageReadSet} and {@link BlobReadOptions}.
 */
public class StoreMessageReadSetTest {
  private static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final File tempDir;
  private final StoreMetrics metrics;

  /**
   * Creates a temporary directory.
   * @throws IOException
   */
  public StoreMessageReadSetTest() throws IOException {
    tempDir = StoreTestUtils.createTempDirectory("storeMessageReadSetDir-" + UtilsTest.getRandomString(10));
    metrics = new StoreMetrics(tempDir.getAbsolutePath(), new MetricRegistry());
  }

  /**
   * Deletes the temporary directory.
   * @throws IOException
   */
  @After
  public void cleanup() throws IOException {
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Primarily tests {@link StoreMessageReadSet} and its APIs but also checks the the {@link Comparable} APIs of
   * {@link BlobReadOptions}.
   * @throws IOException
   */
  @Test
  public void storeMessageReadSetTest() throws IOException {
    int logCapacity = 2000;
    int segCapacity = 1000;
    Log log = new Log(tempDir.getAbsolutePath(), logCapacity, segCapacity, metrics);
    try {
      LogSegment firstSegment = log.getFirstSegment();
      int availableSegCapacity = (int) (segCapacity - firstSegment.getStartOffset());
      byte[] srcOfTruth = TestUtils.getRandomBytes(2 * availableSegCapacity);
      ReadableByteChannel dataChannel = Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(srcOfTruth)));
      log.appendFrom(dataChannel, availableSegCapacity);
      log.appendFrom(dataChannel, availableSegCapacity);
      LogSegment secondSegment = log.getNextSegment(firstSegment);

      Offset firstSegOffset1 = new Offset(firstSegment.getName(), firstSegment.getStartOffset());
      Offset firstSegOffset2 =
          new Offset(firstSegment.getName(), firstSegment.getStartOffset() + availableSegCapacity / 2);
      Offset secondSegOffset1 = new Offset(secondSegment.getName(), secondSegment.getStartOffset());
      Offset secondSegOffset2 =
          new Offset(secondSegment.getName(), secondSegment.getStartOffset() + availableSegCapacity / 2);
      BlobReadOptions ro1 = new BlobReadOptions(log, firstSegOffset2, availableSegCapacity / 3, 1, new MockId("id1"));
      BlobReadOptions ro2 = new BlobReadOptions(log, secondSegOffset1, availableSegCapacity / 4, 1, new MockId("id2"));
      BlobReadOptions ro3 = new BlobReadOptions(log, secondSegOffset2, availableSegCapacity / 2, 1, new MockId("id3"));
      BlobReadOptions ro4 = new BlobReadOptions(log, firstSegOffset1, availableSegCapacity / 5, 1, new MockId("id4"));
      // to test equality in the compareTo() of BlobReadOptions
      BlobReadOptions ro5 = new BlobReadOptions(log, firstSegOffset2, availableSegCapacity / 6, 1, new MockId("id5"));
      List<BlobReadOptions> options = new ArrayList<>(Arrays.asList(ro1, ro2, ro3, ro4, ro5));
      MessageReadSet readSet = new StoreMessageReadSet(options);

      assertEquals(readSet.count(), options.size());
      // options should get sorted by offsets in the constructor
      assertEquals(readSet.getKeyAt(0), new MockId("id4"));
      assertEquals(readSet.sizeInBytes(0), availableSegCapacity / 5);
      assertEquals(readSet.getKeyAt(1), new MockId("id1"));
      assertEquals(readSet.sizeInBytes(1), availableSegCapacity / 3);
      assertEquals(readSet.getKeyAt(2), new MockId("id5"));
      assertEquals(readSet.sizeInBytes(2), availableSegCapacity / 6);
      assertEquals(readSet.getKeyAt(3), new MockId("id2"));
      assertEquals(readSet.sizeInBytes(3), availableSegCapacity / 4);
      assertEquals(readSet.getKeyAt(4), new MockId("id3"));
      assertEquals(readSet.sizeInBytes(4), availableSegCapacity / 2);

      ByteBuffer readBuf = ByteBuffer.allocate(availableSegCapacity / 5);
      ByteBufferOutputStream stream = new ByteBufferOutputStream(readBuf);

      // read the first one all at once
      long written = readSet.writeTo(0, Channels.newChannel(stream), 0, Long.MAX_VALUE);
      assertEquals("Return value from writeTo() is incorrect", availableSegCapacity / 5, written);
      assertArrayEquals(readBuf.array(), Arrays.copyOfRange(srcOfTruth, 0, availableSegCapacity / 5));

      // read the second one byte by byte
      readBuf = ByteBuffer.allocate(availableSegCapacity / 3);
      stream = new ByteBufferOutputStream(readBuf);
      WritableByteChannel channel = Channels.newChannel(stream);
      long currentReadOffset = 0;
      while (currentReadOffset < availableSegCapacity / 3) {
        written = readSet.writeTo(1, channel, currentReadOffset, 1);
        assertEquals("Return value from writeTo() is incorrect", 1, written);
        currentReadOffset++;
      }
      long startOffset = availableSegCapacity / 2;
      long endOffset = availableSegCapacity / 2 + availableSegCapacity / 3;
      assertArrayEquals(readBuf.array(), Arrays.copyOfRange(srcOfTruth, (int) startOffset, (int) endOffset));

      // read the last one in multiple stages
      readBuf = ByteBuffer.allocate(availableSegCapacity / 2);
      stream = new ByteBufferOutputStream(readBuf);
      channel = Channels.newChannel(stream);
      currentReadOffset = 0;
      while (currentReadOffset < availableSegCapacity / 2) {
        written = readSet.writeTo(4, channel, currentReadOffset, availableSegCapacity / 6);
        long expectedWritten = Math.min(availableSegCapacity / 2 - currentReadOffset, availableSegCapacity / 6);
        assertEquals("Return value from writeTo() is incorrect", expectedWritten, written);
        currentReadOffset += availableSegCapacity / 6;
      }
      startOffset = availableSegCapacity + availableSegCapacity / 2;
      endOffset = startOffset + availableSegCapacity / 2;
      assertArrayEquals(readBuf.array(), Arrays.copyOfRange(srcOfTruth, (int) startOffset, (int) endOffset));

      // should not write anything if relative offset is at the size
      readBuf = ByteBuffer.allocate(1);
      stream = new ByteBufferOutputStream(readBuf);
      channel = Channels.newChannel(stream);
      written = readSet.writeTo(0, channel, readSet.sizeInBytes(0), 1);
      assertEquals("No data should have been written", 0, written);

      try {
        readSet.sizeInBytes(options.size());
        fail("Reading should have failed because index is out of bounds");
      } catch (IndexOutOfBoundsException e) {
        // expected. Nothing to do.
      }

      try {
        readSet.writeTo(options.size(), channel, 100, 100);
        fail("Reading should have failed because index is out of bounds");
      } catch (IndexOutOfBoundsException e) {
        // expected. Nothing to do.
      }

      try {
        readSet.getKeyAt(options.size());
        fail("Getting key should have failed because index is out of bounds");
      } catch (IndexOutOfBoundsException e) {
        // expected. Nothing to do.
      }
    } finally {
      log.close();
    }
  }

  /**
   * Tests {@link BlobReadOptions} for getter correctness, serialization/deserialization and bad input.
   * @throws IOException
   */
  @Test
  public void blobReadOptionsTest() throws IOException {
    int logCapacity = 2000;
    int[] segCapacities = {2000, 1000};
    for (int segCapacity : segCapacities) {
      Log log = new Log(tempDir.getAbsolutePath(), logCapacity, segCapacity, metrics);
      try {
        LogSegment firstSegment = log.getFirstSegment();
        int availableSegCapacity = (int) (segCapacity - firstSegment.getStartOffset());
        int count = logCapacity / segCapacity;
        for (int i = 0; i < count; i++) {
          ByteBuffer buffer = ByteBuffer.allocate(availableSegCapacity);
          log.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), availableSegCapacity);
        }
        long offset = Utils.getRandomLong(TestUtils.RANDOM, availableSegCapacity) + firstSegment.getStartOffset();
        long size = Utils.getRandomLong(TestUtils.RANDOM, firstSegment.getEndOffset() - offset);
        long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        MockId id = new MockId("id1");

        // basic test
        BlobReadOptions options =
            new BlobReadOptions(log, new Offset(firstSegment.getName(), offset), size, expiresAtMs, id);
        assertEquals("Ref count of log segment should have increased", 1, firstSegment.refCount());
        verifyGetters(options, firstSegment, offset, size, expiresAtMs, id);
        options.close();
        assertEquals("Ref count of log segment should have decreased", 0, firstSegment.refCount());

        // toBytes() and back test
        doSerDeTest(options, firstSegment, log);

        if (count > 1) {
          // toBytes() and back test for the second segment
          LogSegment secondSegment = log.getNextSegment(firstSegment);
          options = new BlobReadOptions(log, new Offset(secondSegment.getName(), offset), size, expiresAtMs, id);
          assertEquals("Ref count of log segment should have increased", 1, secondSegment.refCount());
          options.close();
          assertEquals("Ref count of log segment should have decreased", 0, secondSegment.refCount());
          doSerDeTest(options, secondSegment, log);
        }

        try {
          new BlobReadOptions(log, new Offset(firstSegment.getName(), firstSegment.getEndOffset()), 1, 1, null);
          fail("Construction should have failed because offset + size > endOffset");
        } catch (IllegalArgumentException e) {
          // expected. Nothing to do.
        }
      } finally {
        log.close();
        StoreTestUtils.cleanDirectory(tempDir, false);
      }
    }
  }

  // helpers
  // blobReadOptionsTest() helpers

  /**
   * Verifies getters of {@link BlobReadOptions} for correctness.
   * @param options the {@link BlobReadOptions} to check.
   * @param logSegment the expected {@link LogSegment} which {@code options} should refer to.
   * @param offset the expected offset inside {@code logSegment} which {@code options} should refer to.
   * @param size the expected size in {@code options}.
   * @param expiresAtMs the expected expiration time in {@code options}.
   * @param id the expected {@link MockId} in {@code options}.
   */
  private void verifyGetters(BlobReadOptions options, LogSegment logSegment, long offset, long size, long expiresAtMs,
      MockId id) {
    assertEquals("LogSegment name not as expected", logSegment.getName(), options.getLogSegmentName());
    assertEquals("Offset not as expected", offset, options.getOffset());
    assertEquals("Size not as expected", size, options.getSize());
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, options.getExpiresAtMs());
    assertEquals("StoreKey not as expected", id, options.getStoreKey());
    MessageInfo messageInfo = options.getMessageInfo();
    assertEquals("Size not as expected", size, messageInfo.getSize());
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, messageInfo.getExpirationTimeInMs());
    assertEquals("StoreKey not as expected", id, messageInfo.getStoreKey());
    Pair<File, FileChannel> fileAndFileChannel = logSegment.getView();
    assertEquals("File instance not as expected", fileAndFileChannel.getFirst(), options.getFile());
    assertEquals("FileChannel instance not as expected", fileAndFileChannel.getSecond(), options.getChannel());
    logSegment.closeView();
  }

  /**
   * Serializes {@code readOptions} in all formats and ensures that the {@link BlobReadOptions} obtained from the
   * deserialization matches the original.
   * @param readOptions the {@link BlobReadOptions} that has to be serialized/deserialized.
   * @param segment the {@link LogSegment} referred to by {@code readOptions}.
   * @param log the {@link Log} that {@code segment} is a part of.
   * @throws IOException
   */
  private void doSerDeTest(BlobReadOptions readOptions, LogSegment segment, Log log) throws IOException {
    for (short i = 0; i <= 1; i++) {
      DataInputStream stream = getSerializedStream(readOptions, i);
      BlobReadOptions deSerReadOptions = BlobReadOptions.fromBytes(stream, STORE_KEY_FACTORY, log);
      assertEquals("Ref count of log segment should have increased", 1, segment.refCount());
      verifyGetters(deSerReadOptions, segment, readOptions.getOffset(), readOptions.getSize(),
          readOptions.getExpiresAtMs(), (MockId) readOptions.getStoreKey());
      deSerReadOptions.close();
      assertEquals("Ref count of log segment should have decreased", 0, segment.refCount());
    }
  }

  /**
   * Gets a serialized format of {@code readOptions} in the version {@code version}.
   * @param readOptions the {@link BlobReadOptions} to serialize.
   * @param version the version to serialize it in.
   * @return a serialized format of {@code readOptions} in the version {@code version}.
   */
  private DataInputStream getSerializedStream(BlobReadOptions readOptions, short version) {
    byte[] bytes;
    switch (version) {
      case BlobReadOptions.VERSION_0:
        // version length + offset length + size length + expires at length + key size
        bytes = new byte[2 + 8 + 8 + 8 + readOptions.getStoreKey().sizeInBytes()];
        ByteBuffer bufWrap = ByteBuffer.wrap(bytes);
        bufWrap.putShort(BlobReadOptions.VERSION_0);
        bufWrap.putLong(readOptions.getOffset());
        bufWrap.putLong(readOptions.getSize());
        bufWrap.putLong(readOptions.getExpiresAtMs());
        bufWrap.put(readOptions.getStoreKey().toBytes());
      case BlobReadOptions.VERSION_1:
        bytes = readOptions.toBytes();
        break;
      default:
        throw new IllegalArgumentException("Version " + version + " of BlobReadOptions does not exist");
    }
    return new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(bytes)));
  }
}
