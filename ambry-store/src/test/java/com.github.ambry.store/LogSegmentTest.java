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
      segment.state = LogSegment.State.ACTIVE;
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
   * various cases and state changes.
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
    String currSegmentName = "log_current";
    String nextSegmentName = "log_next";
    LogSegment currSegment = getSegment(currSegmentName, STANDARD_SEGMENT_SIZE);
    LogSegment nextSegment = getSegment(nextSegmentName, STANDARD_SEGMENT_SIZE);
    try {
      // setup the segments by writing some data and connecting them logically.
      byte[] currData = appendRandomData(currSegment, 2 * STANDARD_SEGMENT_SIZE / 3);
      byte[] nextData = appendRandomData(nextSegment, STANDARD_SEGMENT_SIZE / 2);
      byte[] combined = new byte[currData.length + nextData.length];
      System.arraycopy(currData, 0, combined, 0, currData.length);
      System.arraycopy(nextData, 0, combined, currData.length, nextData.length);
      currSegment.state = LogSegment.State.SEALED;
      currSegment.next = nextSegment;

      // read data in current segment only
      readAndEnsureMatch(currSegment, 0, currData);
      // read data across both segments
      readAndEnsureMatch(currSegment, 0, combined);
      // read at end offset of current segment (edge case)
      readAndEnsureMatch(currSegment, currSegment.getEndOffset(), nextData);

      int readCount = 10;
      for (int i = 0; i < readCount; i++) {
        int position = random.nextInt(currData.length);
        int size = random.nextInt(currData.length - position);
        // read randomly within current segment
        readAndEnsureMatch(currSegment, position, Arrays.copyOfRange(currData, position, position + size));
        // read randomly across both the current segment and next segment
        readAndEnsureMatch(currSegment, position,
            Arrays.copyOfRange(combined, position, position + size + nextData.length));
      }

      // error scenarios
      ByteBuffer readBuf = ByteBuffer.wrap(new byte[combined.length]);
      // read position < 0
      try {
        currSegment.readInto(readBuf, -1);
        fail("Should have failed to read because position provided < 0");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      // read position > endOffset
      try {
        currSegment.readInto(readBuf, currSegment.getEndOffset() + 1);
        fail("Should have failed to read because position provided > end offset");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      currSegment.next = null;
      // try to read more than what is available in the current segment
      long readOverFlowCount = metrics.overflowReadError.getCount();
      try {
        currSegment.readInto(readBuf, 0);
        fail("Should have failed to read because buffer size is more than current segment size");
      } catch (IllegalStateException e) {
        assertEquals("Read overflow should have been reported", readOverFlowCount + 1,
            metrics.overflowReadError.getCount());
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      currSegment.close();
      // read after close
      ByteBuffer buffer = ByteBuffer.allocate(1);
      try {
        currSegment.readInto(buffer, 0);
        fail("Should have failed to read because segment is closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      // no state changes should have occurred.
      assertEquals("Current segment should be in SEALED state", LogSegment.State.SEALED, currSegment.state);
      assertEquals("Next segment should be in ACTIVE state", LogSegment.State.ACTIVE, nextSegment.state);
      closeSegmentAndDeleteFile(currSegment);
      closeSegmentAndDeleteFile(nextSegment);
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
    String nextSegmentName = "log_next";
    LogSegment currSegment = getSegment(currSegmentName, STANDARD_SEGMENT_SIZE);
    LogSegment nextSegment = getSegment(nextSegmentName, STANDARD_SEGMENT_SIZE);
    try {
      byte[] fullBuf =
          TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2 + STANDARD_SEGMENT_SIZE / 3 + STANDARD_SEGMENT_SIZE / 2);

      // first write will fit into current segment.
      int firstWriteSize = STANDARD_SEGMENT_SIZE / 2;
      ByteBuffer firstBuf = ByteBuffer.wrap(fullBuf, 0, firstWriteSize);
      // second write will fit into current segment.
      int secondWriteSize = STANDARD_SEGMENT_SIZE / 3;
      ByteBuffer secondBuf = ByteBuffer.wrap(fullBuf, firstWriteSize, secondWriteSize);
      // third write will not fit into current segment and will need to be written across current and next
      int thirdWriteSize = STANDARD_SEGMENT_SIZE / 2;
      ByteBuffer thirdBuf = ByteBuffer.wrap(fullBuf, firstWriteSize + secondWriteSize, thirdWriteSize);

      // do the first write. Fits in current segment
      currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(firstBuf)), currSegment.getEndOffset(),
          firstWriteSize);
      assertEquals("End offset in current segment is not equal to the cumulative bytes written", firstWriteSize,
          currSegment.getEndOffset());

      // do the second write. Fits in current segment
      currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(secondBuf)), currSegment.getEndOffset(),
          secondWriteSize);
      assertEquals("End offset in current segment is not equal to the cumulative bytes written",
          firstWriteSize + secondWriteSize, currSegment.getEndOffset());

      // try to do the third write on the current segment. Should fail because the data won't fit and there is no next
      long writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(thirdBuf)), currSegment.getEndOffset(),
            thirdWriteSize);
        fail("WriteFrom should have failed because data won't fit in the current segment and there is no next");
      } catch (IllegalStateException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", firstWriteSize + secondWriteSize, thirdBuf.position());
      }

      // now add a next so that writes can be transparently forwarded
      currSegment.next = nextSegment;
      currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(thirdBuf)), currSegment.getEndOffset(),
          thirdWriteSize);

      // the write would have fit partially in the current segment. The rest go into the next segment
      assertEquals("End offset of current segment is incorrect", STANDARD_SEGMENT_SIZE, currSegment.getEndOffset());
      assertEquals("End offset of next segment is incorrect", fullBuf.length - STANDARD_SEGMENT_SIZE,
          nextSegment.getEndOffset());

      // read and ensure data matches
      readAndEnsureMatch(currSegment, 0, Arrays.copyOfRange(fullBuf, 0, STANDARD_SEGMENT_SIZE));
      readAndEnsureMatch(nextSegment, 0, Arrays.copyOfRange(fullBuf, STANDARD_SEGMENT_SIZE, fullBuf.length));

      // data cannot be written at invalid offsets.
      // <0
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      try {
        currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), -1, buffer.remaining());
        fail("WriteFrom should have failed because offset provided for write < 0");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }

      // > capacity
      try {
        currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), STANDARD_SEGMENT_SIZE + 1,
            buffer.remaining());
        fail("WriteFrom should have failed because offset provided was greater than capacity");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }

      // overwrite and check
      byte[] overwriteBuf = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2);
      ByteBuffer overwriteBuffer = ByteBuffer.wrap(overwriteBuf);
      int offsetToWrite = STANDARD_SEGMENT_SIZE / 3;
      // write isolated to single segment
      currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(overwriteBuffer)), offsetToWrite,
          overwriteBuffer.remaining());
      assertEquals("End offset in current segment should not have changed", STANDARD_SEGMENT_SIZE,
          currSegment.getEndOffset());
      assertEquals("End offset of next segment should not have changed", fullBuf.length - STANDARD_SEGMENT_SIZE,
          nextSegment.getEndOffset());
      readAndEnsureMatch(currSegment, offsetToWrite, overwriteBuf);

      // write is across segments
      overwriteBuffer.rewind();
      // move end offset to make sure that end offset is updated correctly when writeFroms happen.
      offsetToWrite = 2 * STANDARD_SEGMENT_SIZE / 3;
      currSegment.setEndOffset(STANDARD_SEGMENT_SIZE - 1);
      currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(overwriteBuffer)), offsetToWrite,
          overwriteBuffer.remaining());
      assertEquals("End offset in current segment is incorrect", STANDARD_SEGMENT_SIZE, currSegment.getEndOffset());
      assertEquals("End offset of next segment should not have changed", fullBuf.length - STANDARD_SEGMENT_SIZE,
          nextSegment.getEndOffset());
      readAndEnsureMatch(currSegment, offsetToWrite,
          Arrays.copyOfRange(overwriteBuf, 0, STANDARD_SEGMENT_SIZE - offsetToWrite));
      readAndEnsureMatch(nextSegment, 0,
          Arrays.copyOfRange(overwriteBuf, STANDARD_SEGMENT_SIZE - offsetToWrite, overwriteBuf.length));

      // write is at capacity of current segment (edge case)
      overwriteBuffer.rewind();
      currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(overwriteBuffer)), STANDARD_SEGMENT_SIZE,
          overwriteBuffer.remaining());
      assertEquals("End offset in current segment should not have changed", STANDARD_SEGMENT_SIZE,
          currSegment.getEndOffset());
      assertEquals("End offset of next segment should be equal to cumulative data written", overwriteBuf.length,
          nextSegment.getEndOffset());
      readAndEnsureMatch(nextSegment, 0, overwriteBuf);

      currSegment.close();
      // ensure that writeFrom fails.
      try {
        currSegment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), 0, buffer.remaining());
        fail("WriteFrom should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      // no state changes should have occurred.
      assertEquals("Current segment should be in FREE state", LogSegment.State.FREE, currSegment.state);
      assertEquals("Next segment should be in FREE state", LogSegment.State.FREE, nextSegment.state);
      closeSegmentAndDeleteFile(currSegment);
      closeSegmentAndDeleteFile(nextSegment);
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
   * state changes and cases for append operations.
   * @param appender the {@link Appender} to use
   * @throws IOException
   */
  private void doAppendTest(Appender appender)
      throws IOException {
    String currSegmentName = "log_current";
    String nextSegmentName = "log_next";
    LogSegment currSegment = getSegment(currSegmentName, STANDARD_SEGMENT_SIZE);
    LogSegment nextSegment = getSegment(nextSegmentName, STANDARD_SEGMENT_SIZE);
    try {
      byte[] fullBuf =
          TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2 + STANDARD_SEGMENT_SIZE / 3 + STANDARD_SEGMENT_SIZE - 1);

      // first write will fit into current segment. Will cause state transition for current segment (FREE -> ACTIVE).
      int firstWriteSize = STANDARD_SEGMENT_SIZE / 2;
      ByteBuffer firstBuf = ByteBuffer.wrap(fullBuf, 0, firstWriteSize);
      // second write will fit into current segment. Will not cause any state transitions.
      int secondWriteSize = STANDARD_SEGMENT_SIZE / 3;
      ByteBuffer secondBuf = ByteBuffer.wrap(fullBuf, firstWriteSize, secondWriteSize);
      // third write will not fit into current segment and will need to forwarded. Will cause state transition for
      // current segment (ACTIVE -> SEALED), for next segment (FREE -> ACTIVE).
      int thirdWriteSize = STANDARD_SEGMENT_SIZE / 2;
      ByteBuffer thirdBuf = ByteBuffer.wrap(fullBuf, firstWriteSize + secondWriteSize, thirdWriteSize);
      // fourth write will be forwarded to the next segment (where it will fit). Will not cause any state transitions.
      int fourthWriteSize = fullBuf.length - thirdWriteSize - secondWriteSize - firstWriteSize;
      ByteBuffer fourthBuf = ByteBuffer.wrap(fullBuf, fullBuf.length - fourthWriteSize, fourthWriteSize);

      // do the first write. Fits in current segment
      appender.append(currSegment, firstBuf);
      assertEquals("End offset in current segment is not equal to the cumulative bytes written", firstWriteSize,
          currSegment.getEndOffset());
      assertEquals("Current segment should be in ACTIVE state", LogSegment.State.ACTIVE, currSegment.state);

      // do the second write. Fits in current segment
      appender.append(currSegment, secondBuf);
      assertEquals("End offset in current segment is not equal to the cumulative bytes written",
          firstWriteSize + secondWriteSize, currSegment.getEndOffset());
      assertEquals("Current segment should be in ACTIVE state", LogSegment.State.ACTIVE, currSegment.state);

      // try to do the third write on the current segment. Should fail because the data won't fit and there is no next
      long writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        appender.append(currSegment, thirdBuf);
        fail("Append should have failed because data won't fit in the current segment and there is no next");
      } catch (IllegalStateException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", firstWriteSize + secondWriteSize, thirdBuf.position());
      }

      // currSegment should be still ACTIVE
      assertEquals("Current segment should be in ACTIVE state", LogSegment.State.ACTIVE, currSegment.state);
      // now add a next so that writes can be transparently forwarded
      currSegment.next = nextSegment;
      appender.append(currSegment, thirdBuf);
      assertEquals("Current segment should be in SEALED state", LogSegment.State.SEALED, currSegment.state);
      assertEquals("Next segment should be in ACTIVE state", LogSegment.State.ACTIVE, nextSegment.state);

      // the whole write should have gone to the next segment i.e. no partial writes
      assertEquals("End offset of current segment is incorrect", firstWriteSize + secondWriteSize,
          currSegment.getEndOffset());
      assertEquals("End offset of next segment is incorrect", thirdWriteSize, nextSegment.getEndOffset());

      // remove next again to check that writes to segments that are sealed fail
      currSegment.next = null;
      // current segment is sealed, so the write will fail.
      writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        appender.append(currSegment, fourthBuf);
        fail("Append should have failed because current segment is sealed and there is no next");
      } catch (IllegalStateException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", fullBuf.length - fourthWriteSize, fourthBuf.position());
      }
      // currSegment should be still SEALED
      assertEquals("Current segment should be in SEALED state", LogSegment.State.SEALED, currSegment.state);

      // add next back again so that writes are forwarded.
      currSegment.next = nextSegment;
      appender.append(currSegment, fourthBuf);
      assertEquals("Current segment should be in SEALED state", LogSegment.State.SEALED, currSegment.state);
      assertEquals("Next segment should be in ACTIVE state", LogSegment.State.ACTIVE, nextSegment.state);

      // the whole write should have gone to the next segment i.e. no partial writes
      assertEquals("End offset of current segment is incorrect", firstWriteSize + secondWriteSize,
          currSegment.getEndOffset());
      assertEquals("End offset of next segment is incorrect", thirdWriteSize + fourthWriteSize,
          nextSegment.getEndOffset());

      // read and ensure data matches
      readAndEnsureMatch(currSegment, 0, Arrays.copyOfRange(fullBuf, 0, firstWriteSize));
      readAndEnsureMatch(currSegment, firstWriteSize,
          Arrays.copyOfRange(fullBuf, firstWriteSize, firstWriteSize + secondWriteSize));
      readAndEnsureMatch(nextSegment, 0,
          Arrays.copyOfRange(fullBuf, firstWriteSize + secondWriteSize, fullBuf.length - fourthWriteSize));
      readAndEnsureMatch(nextSegment, thirdWriteSize,
          Arrays.copyOfRange(fullBuf, fullBuf.length - fourthWriteSize, fullBuf.length));

      currSegment.close();
      nextSegment.close();
      // ensure that append fails.
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      try {
        appender.append(currSegment, buffer);
        fail("Append should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(currSegment);
      closeSegmentAndDeleteFile(nextSegment);
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
