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
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link LogSegment}.
 */
public class LogSegmentTest {
  private static final int STANDARD_SEGMENT_SIZE = 1024;

  private final File tempDir;
  private final StoreMetrics metrics;

  /**
   * Sets up a temporary directory that can be used.
   * @throws IOException
   */
  public LogSegmentTest()
      throws IOException {
    tempDir = Files.createTempDirectory("logSegmentDir-" + UtilsTest.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    metrics = new StoreMetrics(tempDir.getName(), new MetricRegistry());
  }

  /**
   * Deletes the temporary directory that was created.
   */
  @After
  public void cleanup() {
    File[] files = tempDir.listFiles();
    if (files != null) {
      for (File file : files) {
        assertTrue("The file [" + file.getAbsolutePath() + "] could not be deleted", file.delete());
      }
    }
    assertTrue("The directory [" + tempDir.getAbsolutePath() + "] could not be deleted", tempDir.delete());
  }

  /**
   * Tests appending and reading to make sure data is written and the data read is consistent with the data written.
   * @throws IOException
   */
  @Test
  public void basicWriteAndReadTest()
      throws IOException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE);
    try {
      assertEquals("Start offset is not as expected", 0, segment.getStartOffset());
      assertEquals("Name of segment is inconsistent with what was provided", segmentName, segment.getName());
      assertEquals("Capacity of segment is inconsistent with what was provided", STANDARD_SEGMENT_SIZE,
          segment.getCapacityInBytes());
      int writeSize = 100;
      byte[] buf = TestUtils.getRandomBytes(3 * writeSize);
      // append with buffer
      int written = segment.appendFrom(ByteBuffer.wrap(buf, 0, writeSize));
      assertEquals("Size written did not match size of buffer provided", writeSize, written);
      assertEquals("End offset is not equal to the cumulative bytes written", writeSize, segment.getEndOffset());
      readAndEnsureMatch(segment, 0, Arrays.copyOfRange(buf, 0, writeSize));

      // append with channel
      segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(buf, writeSize, writeSize))),
          writeSize);
      assertEquals("End offset is not equal to the cumulative bytes written", 2 * writeSize, segment.getEndOffset());
      readAndEnsureMatch(segment, writeSize, Arrays.copyOfRange(buf, writeSize, 2 * writeSize));

      // use writeFrom
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(buf, 2 * writeSize, writeSize))),
          segment.getEndOffset(), writeSize);
      assertEquals("End offset is not equal to the cumulative bytes written", 3 * writeSize, segment.getEndOffset());
      readAndEnsureMatch(segment, 2 * writeSize, Arrays.copyOfRange(buf, 2 * writeSize, buf.length));

      readAndEnsureMatch(segment, 0, buf);
      // check file size and end offset (they will not match)
      assertEquals("End offset is not equal to the cumulative bytes written", 3 * writeSize, segment.getEndOffset());
      assertEquals("Size in bytes is not equal to size of the file", STANDARD_SEGMENT_SIZE, segment.sizeInBytes());

      // ensure flush doesn't throw any errors.
      segment.flush();
      // close and reopen segment and ensure persistence.
      segment.close();
      segment = new LogSegment(segmentName, new File(tempDir, segmentName), STANDARD_SEGMENT_SIZE, metrics);
      segment.setEndOffset(buf.length);
      readAndEnsureMatch(segment, 0, buf);
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Verifies getting and closing views and makes sure that data and ref counts are consistent.
   * @throws IOException
   */
  @Test
  public void viewAndRefCountTest()
      throws IOException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE);
    try {
      int readSize = 100;
      int viewCount = 5;
      byte[] data = appendRandomData(segment, readSize * viewCount);

      for (int i = 0; i < viewCount; i++) {
        getAndVerifyView(segment, i * readSize, data, i + 1);
      }

      for (int i = 0; i < viewCount; i++) {
        segment.closeView();
        assertEquals("Ref count is not as expected", viewCount - i - 1, segment.refCount());
      }

      // test boundary offsets
      getAndVerifyView(segment, 0, data, 1);
      segment.closeView();
      getAndVerifyView(segment, (int) segment.getEndOffset(), data, 1);
      segment.closeView();

      // cannot open views at invalid offsets
      int[] invalidOffsets = {-1, (int) (segment.getEndOffset() + 1)};
      for (int offset : invalidOffsets) {
        try {
          segment.getView(offset);
          fail("Getting a view at an invalid offset [" + offset + "] should have failed");
        } catch (IllegalArgumentException e) {
          // expected. Nothing to do.
        }
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests setting end offset - makes sure legal values are set correctly and illegal values are rejected.
   * @throws IOException
   */
  @Test
  public void endOffsetTest()
      throws IOException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE);
    try {
      int segmentSize = 500;
      appendRandomData(segment, segmentSize);
      assertEquals("End offset is not equal to the cumulative bytes written", segmentSize, segment.getEndOffset());

      // should be able to set end offset to something >= 0 and < file size
      int[] offsetsToSet = {0, segmentSize / 2, segmentSize};
      for (int offset : offsetsToSet) {
        segment.setEndOffset(offset);
        assertEquals("End offset is not equal to what was set", offset, segment.getEndOffset());
        assertEquals("File channel positioning is incorrect", offset, segment.getView(0).getSecond().position());
      }

      // cannot set end offset to illegal values (< 0 or > file size)
      int[] invalidOffsets = {-1, (int) (segment.sizeInBytes() + 1)};
      for (int offset : invalidOffsets) {
        try {
          segment.setEndOffset(offset);
          fail("Setting log end offset an invalid offset [" + offset + "] should have failed");
        } catch (IllegalArgumentException e) {
          // expected. Nothing to do.
        }
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests {@link LogSegment#appendFrom(ByteBuffer)} and {@link LogSegment#appendFrom(ReadableByteChannel, long)} for
   * various cases.
   * @throws IOException
   */
  @Test
  public void appendTest()
      throws IOException {
    // buffer append
    doAppendTest(new Appender() {
      @Override
      public void append(LogSegment segment, ByteBuffer buffer)
          throws IOException {
        int writeSize = buffer.remaining();
        int written = segment.appendFrom(buffer);
        assertEquals("Size written did not match size of buffer provided", writeSize, written);
      }
    });

    // channel append
    doAppendTest(new Appender() {
      @Override
      public void append(LogSegment segment, ByteBuffer buffer)
          throws IOException {
        int writeSize = buffer.remaining();
        segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeSize);
        assertFalse("The buffer was not completely written", buffer.hasRemaining());
      }
    });
  }

  /**
   * Tests {@link LogSegment#readInto(ByteBuffer, long)} for various cases.
   * @throws IOException
   */
  @Test
  public void readTest()
      throws IOException {
    Random random = new Random();
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE);
    try {
      byte[] data = appendRandomData(segment, 2 * STANDARD_SEGMENT_SIZE / 3);
      readAndEnsureMatch(segment, 0, data);
      int readCount = 10;
      for (int i = 0; i < readCount; i++) {
        int position = random.nextInt(data.length);
        int size = random.nextInt(data.length - position);
        readAndEnsureMatch(segment, position, Arrays.copyOfRange(data, position, position + size));
      }
      // error scenarios
      ByteBuffer readBuf = ByteBuffer.wrap(new byte[data.length]);
      // read position < 0
      try {
        segment.readInto(readBuf, -1);
        fail("Should have failed to read because position provided < 0");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      // read position > endOffset
      try {
        segment.readInto(readBuf, segment.getEndOffset() + 1);
        fail("Should have failed to read because position provided > end offset");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      // position + buffer.remaining() > endOffset.
      long readOverFlowCount = metrics.overflowReadError.getCount();
      try {
        segment.readInto(readBuf, 1);
        fail("Should have failed to read because position + buffer.remaining() > endOffset");
      } catch (IllegalArgumentException e) {
        assertEquals("Read overflow should have been reported", readOverFlowCount + 1,
            metrics.overflowReadError.getCount());
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      segment.close();
      // read after close
      ByteBuffer buffer = ByteBuffer.allocate(1);
      try {
        segment.readInto(buffer, 0);
        fail("Should have failed to read because segment is closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests {@link LogSegment#writeFrom(ReadableByteChannel, long, long)} for various cases.
   * @throws IOException
   */
  @Test
  public void writeFromTest()
      throws IOException {
    String currSegmentName = "log_current";
    LogSegment segment = getSegment(currSegmentName, STANDARD_SEGMENT_SIZE);
    try {
      byte[] bufOne = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 3);
      byte[] bufTwo = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2);

      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), 0, bufOne.length);
      assertEquals("End offset in current segment is not as expected", bufOne.length, segment.getEndOffset());
      readAndEnsureMatch(segment, 0, bufOne);

      // overwrite using bufTwo
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufTwo))), 0, bufTwo.length);
      assertEquals("End offset in current segment is not as expected", bufTwo.length, segment.getEndOffset());
      readAndEnsureMatch(segment, 0, bufTwo);

      // overwrite using bufOne
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), 0, bufOne.length);
      // end offset should not have changed
      assertEquals("End offset in current segment is not as expected", bufTwo.length, segment.getEndOffset());
      readAndEnsureMatch(segment, 0, bufOne);
      readAndEnsureMatch(segment, bufOne.length, Arrays.copyOfRange(bufTwo, bufOne.length, bufTwo.length));

      // write at random locations
      for (int i = 0; i < 10; i++) {
        long offset = Utils.getRandomLong(TestUtils.RANDOM, segment.getCapacityInBytes() - bufOne.length);
        segment
            .writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), offset, bufOne.length);
        readAndEnsureMatch(segment, offset, bufOne);
      }

      // try to overwrite using a channel that won't fit
      ByteBuffer failBuf = ByteBuffer.wrap(TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE + 1));
      long writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(failBuf)), 0, failBuf.remaining());
        fail("WriteFrom should have failed because data won't fit");
      } catch (IllegalArgumentException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", 0, failBuf.position());
      }

      // data cannot be written at invalid offsets.
      // < 0
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), -1, buffer.remaining());
        fail("WriteFrom should have failed because offset provided for write < 0");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }

      // > capacity
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), STANDARD_SEGMENT_SIZE + 1,
            buffer.remaining());
        fail("WriteFrom should have failed because offset provided was greater than capacity");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }

      segment.close();
      // ensure that writeFrom fails.
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), 0, buffer.remaining());
        fail("WriteFrom should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests for bad construction cases of {@link LogSegment}.
   * @throws IOException
   */
  @Test
  public void badConstructionTest()
      throws IOException {
    // try to construct with a file that does not exist.
    try {
      new LogSegment("log_non_existent", new File(tempDir, "log_non_existent"), STANDARD_SEGMENT_SIZE, metrics);
      fail("Construction should have failed because the backing file does not exist");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // try to construct with a file that is a directory
    try {
      new LogSegment(tempDir.getName(), new File(tempDir.getParent(), tempDir.getName()), STANDARD_SEGMENT_SIZE,
          metrics);
      fail("Construction should have failed because the backing file does not exist");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  // helpers
  // general

  /**
   * Creates and gets a {@link LogSegment}.
   * @param segmentName the name of the segment as well as the file backing the segment.
   * @param capacityInBytes the capacity of the file/segment.
   * @return instance of {@link LogSegment} that is backed by the file with name {@code segmentName} of capacity
   * {@code capacityInBytes}.
   * @throws IOException
   */
  private LogSegment getSegment(String segmentName, long capacityInBytes)
      throws IOException {
    File file = new File(tempDir, segmentName);
    if (file.exists()) {
      assertTrue(file.getAbsolutePath() + " already exists and could not be deleted", file.delete());
    }
    assertTrue("Segment file could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    file.deleteOnExit();
    try (RandomAccessFile raf = new RandomAccessFile(tempDir + File.separator + segmentName, "rw")) {
      raf.setLength(capacityInBytes);
      LogSegment segment = new LogSegment(segmentName, file, capacityInBytes, metrics);
      segment.setEndOffset(0);
      return segment;
    }
  }

  /**
   * Appends random data of size {@code size} to given {@code segment}.
   * @param segment the {@link LogSegment} to append data to.
   * @param size the size of the data that should be appended.
   * @return the data that was appended.
   * @throws IOException
   */
  private byte[] appendRandomData(LogSegment segment, int size)
      throws IOException {
    byte[] buf = TestUtils.getRandomBytes(size);
    segment.appendFrom(ByteBuffer.wrap(buf));
    return buf;
  }

  /**
   * Reads data starting from {@code offsetToStartRead} of {@code segment} and matches it with {@code original}.
   * @param segment the {@link LogSegment} to read from.
   * @param offsetToStartRead the offset in {@code segment} to start reading from.
   * @param original the byte array to compare against.
   * @throws IOException
   */
  private void readAndEnsureMatch(LogSegment segment, long offsetToStartRead, byte[] original)
      throws IOException {
    ByteBuffer readBuf = ByteBuffer.wrap(new byte[original.length]);
    segment.readInto(readBuf, offsetToStartRead);
    assertArrayEquals("Data read does not match data written", original, readBuf.array());
  }

  /**
   * Closes the {@code segment} and deletes the backing file.
   * @param segment the {@link LogSegment} that needs to be closed and whose backing file needs to be deleted.
   * @throws IOException
   */
  private void closeSegmentAndDeleteFile(LogSegment segment)
      throws IOException {
    segment.close();
    assertFalse("File channel is not closed", segment.getView(0).getSecond().isOpen());
    File segmentFile = new File(tempDir, segment.getName());
    assertTrue("The segment file [" + segmentFile.getAbsolutePath() + "] could not be deleted", segmentFile.delete());
  }

  // viewAndRefCountTest() helpers

  /**
   * Gets a view of the given {@code segment} and verifies the ref count and the data obtained from the view against
   * {@code expectedRefCount} and {@code dataInSegment} respectively.
   * @param segment the {@link LogSegment} to get a view from.
   * @param offset the offset for which a view is required.
   * @param dataInSegment the entire data in the {@link LogSegment}.
   * @param expectedRefCount the expected return value of {@link LogSegment#refCount()} once the view is obtained from
   *                         the {@code segment}
   * @throws IOException
   */
  private void getAndVerifyView(LogSegment segment, int offset, byte[] dataInSegment, long expectedRefCount)
      throws IOException {
    Random random = new Random();
    Pair<File, FileChannel> view = segment.getView(offset);
    assertNotNull("File object received in view is null", view.getFirst());
    assertNotNull("FileChannel object received in view is null", view.getSecond());
    assertEquals("Ref count is not as expected", expectedRefCount, segment.refCount());
    int sizeToRead = random.nextInt(dataInSegment.length - offset + 1);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[sizeToRead]);
    view.getSecond().read(buffer, offset);
    assertArrayEquals("Data read from file does not match data written",
        Arrays.copyOfRange(dataInSegment, offset, offset + sizeToRead), buffer.array());
  }

  // appendTest() helpers

  /**
   * Using the given {@code appender}'s {@link Appender#append(LogSegment, ByteBuffer)} function, tests for various
   * cases for append operations.
   * @param appender the {@link Appender} to use
   * @throws IOException
   */
  private void doAppendTest(Appender appender)
      throws IOException {
    String currSegmentName = "log_current";
    LogSegment segment = getSegment(currSegmentName, STANDARD_SEGMENT_SIZE);
    try {
      byte[] bufOne = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2);
      byte[] bufTwo = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 3);

      appender.append(segment, ByteBuffer.wrap(bufOne));
      assertEquals("End offset in current segment is not equal to the cumulative bytes written", bufOne.length,
          segment.getEndOffset());

      appender.append(segment, ByteBuffer.wrap(bufTwo));
      assertEquals("End offset in current segment is not equal to the cumulative bytes written",
          bufOne.length + bufTwo.length, segment.getEndOffset());

      // try to do a write that won't fit
      ByteBuffer failBuf = ByteBuffer.wrap(TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE + 1));
      long writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        appender.append(segment, failBuf);
        fail("Append should have failed because data won't fit in the segment");
      } catch (IllegalArgumentException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", 0, failBuf.position());
      }

      // read and ensure data matches
      readAndEnsureMatch(segment, 0, bufOne);
      readAndEnsureMatch(segment, bufOne.length, bufTwo);

      segment.close();
      // ensure that append fails.
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      try {
        appender.append(segment, buffer);
        fail("Append should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Interface for abstracting append operations.
   */
  private interface Appender {
    /**
     * Appends the data of {@code buffer} to {@code segment}.
     * @param segment the {@link LogSegment} to append {@code buffer} to.
     * @param buffer the data to append to {@code segment}.
     * @throws IOException
     */
    void append(LogSegment segment, ByteBuffer buffer)
        throws IOException;
  }
}

