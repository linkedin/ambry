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
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link LogSegment}.
 */
public class LogSegmentTest {
  private static final int STANDARD_SEGMENT_SIZE = 1024;
  private static final int BIG_SEGMENT_SIZE = 3 * LogSegment.BYTE_BUFFER_SIZE_FOR_APPEND;

  private final File tempDir;
  private final StoreConfig config;
  private final StoreMetrics metrics;

  /**
   * Sets up a temporary directory that can be used.
   * @throws IOException
   */
  public LogSegmentTest() throws IOException {
    tempDir = Files.createTempDirectory("logSegmentDir-" + TestUtils.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    Properties props = new Properties();
    props.setProperty("store.set.file.permission.enabled", Boolean.toString(true));
    config = new StoreConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    metrics = new StoreMetrics(metricRegistry);
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
    validateMockitoUsage();
  }

  /**
   * Test that the file permissions are correctly set based on permissions specified in {@link StoreConfig}.
   * @throws Exception
   */
  @Test
  public void setFilePermissionsTest() throws Exception {
    Properties props = new Properties();
    props.setProperty("store.set.file.permission.enabled", Boolean.toString(true));
    props.setProperty("store.data.file.permission", "rw-rw-r--");
    StoreConfig initialConfig = new StoreConfig(new VerifiableProperties(props));
    File segmentFile = new File(tempDir, "test_segment");
    assertTrue("Fail to create segment file", segmentFile.createNewFile());
    segmentFile.deleteOnExit();
    // create log segment instance by writing into brand new file (using initialConfig, file permission = "rw-rw-r--")
    new LogSegment(segmentFile.getName(), segmentFile, STANDARD_SEGMENT_SIZE, initialConfig, metrics, true);
    Set<PosixFilePermission> filePerm = Files.getPosixFilePermissions(segmentFile.toPath());
    assertEquals("File permissions are not expected", "rw-rw-r--", PosixFilePermissions.toString(filePerm));
    // create log segment instance by reading from existing file (using default store config, file permission = "rw-rw----")
    new LogSegment(segmentFile.getName(), segmentFile, config, metrics);
    filePerm = Files.getPosixFilePermissions(segmentFile.toPath());
    assertEquals("File permissions are not expected", "rw-rw----", PosixFilePermissions.toString(filePerm));
  }

  /**
   * Tests appending and reading to make sure data is written and the data read is consistent with the data written.
   * @throws IOException
   */
  @Test
  public void basicWriteAndReadTest() throws IOException, StoreException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, true);
    segment.initBufferForAppend();
    try {
      assertEquals("Name of segment is inconsistent with what was provided", segmentName, segment.getName());
      assertEquals("Capacity of segment is inconsistent with what was provided", STANDARD_SEGMENT_SIZE,
          segment.getCapacityInBytes());
      assertEquals("Start offset is not equal to header size", LogSegment.HEADER_SIZE, segment.getStartOffset());
      int writeSize = 100;
      byte[] buf = TestUtils.getRandomBytes(3 * writeSize);
      long writeStartOffset = segment.getStartOffset();
      // append with buffer
      int written = segment.appendFrom(ByteBuffer.wrap(buf, 0, writeSize));
      assertEquals("Size written did not match size of buffer provided", writeSize, written);
      assertEquals("End offset is not as expected", writeStartOffset + writeSize, segment.getEndOffset());
      readAndEnsureMatch(segment, writeStartOffset, Arrays.copyOfRange(buf, 0, writeSize));

      // append with channel
      segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(buf, writeSize, writeSize))),
          writeSize);
      assertEquals("End offset is not as expected", writeStartOffset + 2 * writeSize, segment.getEndOffset());
      readAndEnsureMatch(segment, writeStartOffset + writeSize, Arrays.copyOfRange(buf, writeSize, 2 * writeSize));

      // should throw exception if channel's available data less than requested.
      try {
        segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.allocate(writeSize))),
            writeSize + 1);
        fail("Should throw exception.");
      } catch (StoreException e) {
      }

      // use writeFrom
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(buf, 2 * writeSize, writeSize))),
          segment.getEndOffset(), writeSize);
      assertEquals("End offset is not as expected", writeStartOffset + 3 * writeSize, segment.getEndOffset());
      readAndEnsureMatch(segment, writeStartOffset + 2 * writeSize, Arrays.copyOfRange(buf, 2 * writeSize, buf.length));

      readAndEnsureMatch(segment, writeStartOffset, buf);
      // check file size and end offset (they will not match)
      assertEquals("End offset is not equal to the cumulative bytes written", writeStartOffset + 3 * writeSize,
          segment.getEndOffset());
      assertEquals("Size in bytes is not equal to size written", writeStartOffset + 3 * writeSize,
          segment.sizeInBytes());
      assertEquals("Capacity is not equal to allocated size ", STANDARD_SEGMENT_SIZE, segment.getCapacityInBytes());

      // ensure flush doesn't throw any errors.
      segment.flush();
      // close and reopen segment and ensure persistence.
      segment.close(false);
      segment = new LogSegment(segmentName, new File(tempDir, segmentName), config, metrics);
      segment.setEndOffset(writeStartOffset + buf.length);
      readAndEnsureMatch(segment, writeStartOffset, buf);
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Verifies getting and closing views and makes sure that data and ref counts are consistent.
   * @throws IOException
   */
  @Test
  public void viewAndRefCountTest() throws IOException, StoreException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, true);
    try {
      long startOffset = segment.getStartOffset();
      int readSize = 100;
      int viewCount = 5;
      byte[] data = appendRandomData(segment, readSize * viewCount);

      for (int i = 0; i < viewCount; i++) {
        getAndVerifyView(segment, startOffset, i * readSize, data, i + 1);
      }

      for (int i = 0; i < viewCount; i++) {
        segment.closeView();
        assertEquals("Ref count is not as expected", viewCount - i - 1, segment.refCount());
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
  public void endOffsetTest() throws IOException, StoreException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, true);
    try {
      long writeStartOffset = segment.getStartOffset();
      int segmentSize = 500;
      appendRandomData(segment, segmentSize);
      assertEquals("End offset is not as expected", writeStartOffset + segmentSize, segment.getEndOffset());

      // should be able to set end offset to something >= initial offset and <= file size
      int[] offsetsToSet = {(int) (writeStartOffset), segmentSize / 2, segmentSize};
      for (int offset : offsetsToSet) {
        segment.setEndOffset(offset);
        assertEquals("End offset is not equal to what was set", offset, segment.getEndOffset());
        assertEquals("File channel positioning is incorrect", offset, segment.getView().getSecond().position());
      }

      // cannot set end offset to illegal values (< initial offset or > file size)
      int[] invalidOffsets = {(int) (writeStartOffset - 1), (int) (segment.sizeInBytes() + 1)};
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
  public void appendTest() throws IOException, StoreException {
    // buffer append
    doAppendTest(new Appender() {
      @Override
      public void append(LogSegment segment, ByteBuffer buffer) throws StoreException {
        int writeSize = buffer.remaining();
        int written = segment.appendFrom(buffer);
        assertEquals("Size written did not match size of buffer provided", writeSize, written);
      }
    });

    // channel append
    doAppendTest(new Appender() {
      @Override
      public void append(LogSegment segment, ByteBuffer buffer) throws StoreException {
        int writeSize = buffer.remaining();
        segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeSize);
        assertFalse("The buffer was not completely written", buffer.hasRemaining());
      }
    });

    // direct IO append
    if (Utils.isLinux()) {
      doAppendTest(new Appender() {
        @Override
        public void append(LogSegment segment, ByteBuffer buffer) throws StoreException {
          int writeSize = buffer.remaining();
          segment.appendFromDirectly(buffer.array(), 0, writeSize);
        }
      });
    }
  }

  /**
   * Tests {@link LogSegment#readInto(ByteBuffer, long)} and {@link LogSegment#readIntoDirectly(byte[], long, int)}(if
   * current OS is Linux) for various cases.
   * @throws IOException
   */
  @Test
  public void readTest() throws IOException, StoreException {
    Random random = new Random();
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, true);
    try {
      long writeStartOffset = segment.getStartOffset();
      byte[] data = appendRandomData(segment, 2 * STANDARD_SEGMENT_SIZE / 3);
      readAndEnsureMatch(segment, writeStartOffset, data);
      readDirectlyAndEnsureMatch(segment, writeStartOffset, data);
      int readCount = 10;
      for (int i = 0; i < readCount; i++) {
        int position = random.nextInt(data.length);
        int size = random.nextInt(data.length - position);
        readAndEnsureMatch(segment, writeStartOffset + position, Arrays.copyOfRange(data, position, position + size));
        readDirectlyAndEnsureMatch(segment, writeStartOffset + position,
            Arrays.copyOfRange(data, position, position + size));
      }

      // check for position > endOffset and < data size written to the segment
      // setting end offset to 1/3 * (sizeInBytes - startOffset)
      segment.setEndOffset(segment.getStartOffset() + (segment.sizeInBytes() - segment.getStartOffset()) / 3);
      int position = (int) segment.getEndOffset() + random.nextInt(data.length - (int) segment.getEndOffset());
      int size = random.nextInt(data.length - position);
      readAndEnsureMatch(segment, writeStartOffset + position, Arrays.copyOfRange(data, position, position + size));
      readDirectlyAndEnsureMatch(segment, writeStartOffset + position,
          Arrays.copyOfRange(data, position, position + size));

      // error scenarios for general IO
      byte[] byteArray = new byte[data.length];
      ByteBuffer readBuf = ByteBuffer.wrap(byteArray);
      // data cannot be read at invalid offsets.
      long[] invalidOffsets = {writeStartOffset - 1, segment.sizeInBytes(), segment.sizeInBytes() + 1};
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      for (long invalidOffset : invalidOffsets) {
        try {
          segment.readInto(readBuf, invalidOffset);
          fail("Should have failed to read because position provided is invalid");
        } catch (IndexOutOfBoundsException e) {
          assertEquals("Position of buffer has changed", 0, buffer.position());
        }
      }
      if (Utils.isLinux()) {
        for (long invalidOffset : invalidOffsets) {
          try {
            segment.readIntoDirectly(byteArray, invalidOffset, data.length);
            fail("Should have failed to read because position provided is invalid");
          } catch (IndexOutOfBoundsException e) {
            assertEquals("Position of buffer has changed", 0, buffer.position());
          }
        }
      }

      // position + buffer.remaining > sizeInBytes
      long readOverFlowCount = metrics.overflowReadError.getCount();
      byteArray = new byte[2];
      readBuf = ByteBuffer.allocate(2);
      segment.setEndOffset(segment.getStartOffset());
      position = (int) segment.sizeInBytes() - 1;
      try {
        segment.readInto(readBuf, position);
        fail("Should have failed to read because position + buffer.remaining() > sizeInBytes");
      } catch (IndexOutOfBoundsException e) {
        assertEquals("Read overflow should have been reported", readOverFlowCount + 1,
            metrics.overflowReadError.getCount());
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }
      if (Utils.isLinux()) {
        try {
          segment.readIntoDirectly(byteArray, position, byteArray.length);
          fail("Should have failed to read because position + buffer.remaining() > sizeInBytes");
        } catch (IndexOutOfBoundsException e) {
          assertEquals("Read overflow should have been reported", readOverFlowCount + 2,
              metrics.overflowReadError.getCount());
        }
      }

      segment.close(false);
      // read after close
      buffer = ByteBuffer.allocate(1);
      try {
        segment.readInto(buffer, writeStartOffset);
        fail("Should have failed to read because segment is closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
      if (Utils.isLinux()) {
        byteArray = new byte[1];
        try {
          segment.readIntoDirectly(byteArray, writeStartOffset, byteArray.length);
          fail("Should have failed to read because segment is closed");
        } catch (ClosedChannelException e) {
        }
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
  public void writeFromTest() throws IOException, StoreException {
    String currSegmentName = "log_current";
    LogSegment segment = getSegment(currSegmentName, STANDARD_SEGMENT_SIZE, true);
    try {
      long writeStartOffset = segment.getStartOffset();
      byte[] bufOne = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 3);
      byte[] bufTwo = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2);

      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), writeStartOffset,
          bufOne.length);
      assertEquals("End offset is not as expected", writeStartOffset + bufOne.length, segment.getEndOffset());
      readAndEnsureMatch(segment, writeStartOffset, bufOne);

      // overwrite using bufTwo
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufTwo))), writeStartOffset,
          bufTwo.length);
      assertEquals("End offset is not as expected", writeStartOffset + bufTwo.length, segment.getEndOffset());
      readAndEnsureMatch(segment, writeStartOffset, bufTwo);

      // overwrite using bufOne
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), writeStartOffset,
          bufOne.length);
      // end offset should not have changed
      assertEquals("End offset is not as expected", writeStartOffset + bufTwo.length, segment.getEndOffset());
      readAndEnsureMatch(segment, writeStartOffset, bufOne);
      readAndEnsureMatch(segment, writeStartOffset + bufOne.length,
          Arrays.copyOfRange(bufTwo, bufOne.length, bufTwo.length));

      // write at random locations
      for (int i = 0; i < 10; i++) {
        long offset = writeStartOffset + Utils.getRandomLong(TestUtils.RANDOM,
            segment.sizeInBytes() - bufOne.length - writeStartOffset);
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), offset,
            bufOne.length);
        readAndEnsureMatch(segment, offset, bufOne);
      }

      // try to overwrite using a channel that won't fit
      ByteBuffer failBuf =
          ByteBuffer.wrap(TestUtils.getRandomBytes((int) (STANDARD_SEGMENT_SIZE - writeStartOffset + 1)));
      long writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(failBuf)), writeStartOffset,
            failBuf.remaining());
        fail("WriteFrom should have failed because data won't fit");
      } catch (IndexOutOfBoundsException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", 0, failBuf.position());
      }

      // data cannot be written at invalid offsets.
      long[] invalidOffsets = {writeStartOffset - 1, STANDARD_SEGMENT_SIZE, STANDARD_SEGMENT_SIZE + 1};
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      for (long invalidOffset : invalidOffsets) {
        try {
          segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), invalidOffset, buffer.remaining());
          fail("WriteFrom should have failed because offset provided for write is invalid");
        } catch (IndexOutOfBoundsException e) {
          assertEquals("Position of buffer has changed", 0, buffer.position());
        }
      }

      segment.close(false);
      // ensure that writeFrom fails.
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeStartOffset, buffer.remaining());
        fail("WriteFrom should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Test a normal shutdown of log segment. Verify that disk flush operation is invoked.
   * @throws Exception
   */
  @Test
  public void closeLogSegmentTest() throws Exception {
    Pair<LogSegment, FileChannel> segmentAndFileChannel = getSegmentAndFileChannel("log_current1");
    LogSegment segment = segmentAndFileChannel.getFirst();
    FileChannel mockFileChannel = segmentAndFileChannel.getSecond();

    // test that log segment is closed successfully, ensure that the flush method is invoked
    segment.close(false);
    verify(mockFileChannel).force(true);
    assertFalse("File channel is not closed", segment.getView().getSecond().isOpen());
    assertTrue("File couldn't be deleted.", (new File(tempDir, segment.getName()).delete()));
  }

  /**
   * Test that closing log segment and skip any disk flush operation. Verify that flush method is never invoked.
   * @throws Exception
   */
  @Test
  public void closeLogSegmentAndSkipFlushTest() throws Exception {
    Pair<LogSegment, FileChannel> segmentAndFileChannel = getSegmentAndFileChannel("log_current2");
    LogSegment segment = segmentAndFileChannel.getFirst();
    FileChannel mockFileChannel = segmentAndFileChannel.getSecond();

    // test that log segment is being closed due to disk I/O error and flush operation should be skipped
    segment.close(true);
    verify(mockFileChannel, times(0)).force(true);
    assertFalse("File channel is not closed", segment.getView().getSecond().isOpen());
    assertTrue("File couldn't be deleted.", (new File(tempDir, segment.getName()).delete()));
  }

  /**
   * Test that closing log segment (skip disk flush) and exception occurs. The shutdown process should continue.
   */
  @Test
  public void closeLogSegmentWithExceptionTest() throws Exception {
    Pair<LogSegment, FileChannel> segmentAndFileChannel = getSegmentAndFileChannel("log_current3");
    LogSegment segment = segmentAndFileChannel.getFirst();
    FileChannel mockFileChannel = segmentAndFileChannel.getSecond();

    // test that log segment is being closed due to disk I/O error (shouldSkipDiskFlush = true) and exception occurs
    // when closing file channel
    doThrow(new IOException("close channel failure")).when(mockFileChannel).close();
    segment.close(true);
    verify(mockFileChannel, times(0)).force(true);
    assertTrue("File couldn't be deleted.", (new File(tempDir, segment.getName()).delete()));

    segmentAndFileChannel = getSegmentAndFileChannel("log_current4");
    segment = segmentAndFileChannel.getFirst();
    mockFileChannel = segmentAndFileChannel.getSecond();

    // test that log segment is being closed during normal shutdown (shouldSkipDiskFlush = false) and exception occurs
    doThrow(new IOException("close channel failure")).when(mockFileChannel).close();
    try {
      segment.close(false);
      fail("should fail because IOException occurred");
    } catch (IOException e) {
      //expected
    }
    verify(mockFileChannel).force(true);
    assertTrue("File couldn't be deleted.", (new File(tempDir, segment.getName()).delete()));
  }

  /**
   * Tests for special constructor cases.
   * @throws IOException
   */
  @Test
  public void constructorTest() throws IOException, StoreException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, false);
    assertEquals("Start offset should be 0 when no headers are written", 0, segment.getStartOffset());
  }

  /**
   * Tests for bad construction cases of {@link LogSegment}.
   * @throws IOException
   */
  @Test
  public void badConstructionTest() throws IOException, StoreException {
    // try to construct with a file that does not exist.
    String name = "log_non_existent";
    File file = new File(tempDir, name);
    try {
      new LogSegment(name, file, STANDARD_SEGMENT_SIZE, config, metrics, true);
      fail("Construction should have failed because the backing file does not exist");
    } catch (StoreException e) {
      // expected. Nothing to do.
    }

    try {
      new LogSegment(name, file, config, metrics);
      fail("Construction should have failed because the backing file does not exist");
    } catch (StoreException e) {
      // expected. Nothing to do.
    }

    // try to construct with a file that is a directory
    name = tempDir.getName();
    file = new File(tempDir.getParent(), name);
    try {
      new LogSegment(name, file, STANDARD_SEGMENT_SIZE, config, metrics, true);
      fail("Construction should have failed because the backing file does not exist");
    } catch (StoreException e) {
      // expected. Nothing to do.
    }

    name = tempDir.getName();
    file = new File(tempDir.getParent(), name);
    try {
      new LogSegment(name, file, config, metrics);
      fail("Construction should have failed because the backing file does not exist");
    } catch (StoreException e) {
      // expected. Nothing to do.
    }

    // unknown version
    LogSegment segment = getSegment("dummy_log", STANDARD_SEGMENT_SIZE, true);
    file = segment.getView().getFirst();
    byte[] header = getHeader(segment);
    byte savedByte = header[0];
    // mess with version
    header[0] = (byte) (header[0] + 10);
    writeHeader(segment, header);
    try {
      new LogSegment(name, file, config, metrics);
      fail("Construction should have failed because version is unknown");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // bad CRC
    // fix version but mess with data after version
    header[0] = savedByte;
    header[2] = header[2] == (byte) 1 ? (byte) 0 : (byte) 1;
    writeHeader(segment, header);
    try {
      new LogSegment(name, file, config, metrics);
      fail("Construction should have failed because crc check should have failed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
    closeSegmentAndDeleteFile(segment);
  }

  private byte[] getHeader(LogSegment segment) throws IOException {
    FileChannel channel = segment.getView().getSecond();
    ByteBuffer header = ByteBuffer.allocate(LogSegment.HEADER_SIZE);
    channel.read(header, 0);
    return header.array();
  }

  private void writeHeader(LogSegment segment, byte[] buf) throws IOException {
    FileChannel channel = segment.getView().getSecond();
    ByteBuffer buffer = ByteBuffer.wrap(buf);
    while (buffer.hasRemaining()) {
      channel.write(buffer, 0);
    }
  }

  // helpers
  // general

  /**
   * Creates and gets a {@link LogSegment}.
   * @param segmentName the name of the segment as well as the file backing the segment.
   * @param capacityInBytes the capacity of the file/segment.
   * @param writeHeaders if {@code true}, writes headers for the segment.
   * @return instance of {@link LogSegment} that is backed by the file with name {@code segmentName} of capacity
   * {@code capacityInBytes}.
   * @throws IOException
   */
  private LogSegment getSegment(String segmentName, long capacityInBytes, boolean writeHeaders)
      throws IOException, StoreException {
    File file = new File(tempDir, segmentName);
    if (file.exists()) {
      assertTrue(file.getAbsolutePath() + " already exists and could not be deleted", file.delete());
    }
    assertTrue("Segment file could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    file.deleteOnExit();
    try (RandomAccessFile raf = new RandomAccessFile(tempDir + File.separator + segmentName, "rw")) {
      return new LogSegment(segmentName, file, capacityInBytes, config, metrics, writeHeaders);
    }
  }

  /**
   * Create a {@link LogSegment} and a mock file channel associated with it for testing.
   * @param name the name of log segment
   * @return a pair that contains log segment and mock file channel
   * @throws Exception
   */
  private Pair<LogSegment, FileChannel> getSegmentAndFileChannel(String name) throws Exception {
    File file = new File(tempDir, name);
    if (file.exists()) {
      assertTrue(file.getAbsolutePath() + " already exists and could not be deleted", file.delete());
    }
    assertTrue("Segment file could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    file.deleteOnExit();
    FileChannel fileChannel = Utils.openChannel(file, true);
    FileChannel mockFileChannel = Mockito.spy(fileChannel);
    LogSegment segment = new LogSegment(file, STANDARD_SEGMENT_SIZE, config, metrics, mockFileChannel);
    return new Pair<>(segment, mockFileChannel);
  }

  /**
   * Appends random data of size {@code size} to given {@code segment}.
   * @param segment the {@link LogSegment} to append data to.
   * @param size the size of the data that should be appended.
   * @return the data that was appended.
   * @throws StoreException
   */
  private byte[] appendRandomData(LogSegment segment, int size) throws StoreException {
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
  private void readAndEnsureMatch(LogSegment segment, long offsetToStartRead, byte[] original) throws IOException {
    ByteBuffer readBuf = ByteBuffer.wrap(new byte[original.length]);
    segment.readInto(readBuf, offsetToStartRead);
    assertArrayEquals("Data read does not match data written", original, readBuf.array());
  }

  /**
   * Reads data(direct IO) starting from {@code offsetToStartRead} of {@code segment} and matches it with {@code original}.
   * @param segment the {@link LogSegment} to read from.
   * @param offsetToStartRead the offset in {@code segment} to start reading from.
   * @param original the byte array to compare against.
   * @throws IOException
   */
  private void readDirectlyAndEnsureMatch(LogSegment segment, long offsetToStartRead, byte[] original)
      throws IOException {
    if (Utils.isLinux()) {
      byte[] byteArray = new byte[original.length];
      segment.readIntoDirectly(byteArray, offsetToStartRead, original.length);
      assertArrayEquals("Data read does not match data written", original, byteArray);
    }
  }

  /**
   * Closes the {@code segment} and deletes the backing file.
   * @param segment the {@link LogSegment} that needs to be closed and whose backing file needs to be deleted.
   * @throws IOException
   */
  private void closeSegmentAndDeleteFile(LogSegment segment) throws IOException {
    segment.close(false);
    assertFalse("File channel is not closed", segment.getView().getSecond().isOpen());
    File segmentFile = new File(tempDir, segment.getName());
    assertTrue("The segment file [" + segmentFile.getAbsolutePath() + "] could not be deleted", segmentFile.delete());
  }

  // viewAndRefCountTest() helpers

  /**
   * Gets a view of the given {@code segment} and verifies the ref count and the data obtained from the view against
   * {@code expectedRefCount} and {@code dataInSegment} respectively.
   * @param segment the {@link LogSegment} to get a view from.
   * @param writeStartOffset the offset at which write was started on the segment.
   * @param offset the offset for which a view is required.
   * @param dataInSegment the entire data in the {@link LogSegment}.
   * @param expectedRefCount the expected return value of {@link LogSegment#refCount()} once the view is obtained from
   *                         the {@code segment}
   * @throws IOException
   */
  private void getAndVerifyView(LogSegment segment, long writeStartOffset, int offset, byte[] dataInSegment,
      long expectedRefCount) throws IOException {
    Random random = new Random();
    Pair<File, FileChannel> view = segment.getView();
    assertNotNull("File object received in view is null", view.getFirst());
    assertNotNull("FileChannel object received in view is null", view.getSecond());
    assertEquals("Ref count is not as expected", expectedRefCount, segment.refCount());
    int sizeToRead = random.nextInt(dataInSegment.length - offset + 1);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[sizeToRead]);
    view.getSecond().read(buffer, writeStartOffset + offset);
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
  private void doAppendTest(Appender appender) throws IOException, StoreException {
    String currSegmentName = "log_current";
    LogSegment segment = getSegment(currSegmentName, BIG_SEGMENT_SIZE, true);
    segment.initBufferForAppend();
    try {
      long writeStartOffset = segment.getStartOffset();
      byte[] bufOne = TestUtils.getRandomBytes(BIG_SEGMENT_SIZE / 6);
      byte[] bufTwo = TestUtils.getRandomBytes(BIG_SEGMENT_SIZE / 9);
      byte[] bufThree = TestUtils.getRandomBytes(LogSegment.BYTE_BUFFER_SIZE_FOR_APPEND * 2 + 3);

      appender.append(segment, ByteBuffer.wrap(bufOne));
      assertEquals("End offset is not as expected", writeStartOffset + bufOne.length, segment.getEndOffset());

      appender.append(segment, ByteBuffer.wrap(bufTwo));
      assertEquals("End offset is not as expected", writeStartOffset + bufOne.length + bufTwo.length,
          segment.getEndOffset());

      appender.append(segment, ByteBuffer.wrap(bufThree));
      assertEquals("End offset is not as expected", writeStartOffset + bufOne.length + bufTwo.length + bufThree.length,
          segment.getEndOffset());

      // try to do a write that won't fit
      ByteBuffer failBuf = ByteBuffer.wrap(TestUtils.getRandomBytes((int) (BIG_SEGMENT_SIZE - writeStartOffset + 1)));
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
      readAndEnsureMatch(segment, writeStartOffset, bufOne);
      readAndEnsureMatch(segment, writeStartOffset + bufOne.length, bufTwo);
      readAndEnsureMatch(segment, writeStartOffset + bufOne.length + bufTwo.length, bufThree);
      readDirectlyAndEnsureMatch(segment, writeStartOffset, bufOne);
      readDirectlyAndEnsureMatch(segment, writeStartOffset + bufOne.length, bufTwo);
      readDirectlyAndEnsureMatch(segment, writeStartOffset + bufOne.length + bufTwo.length, bufThree);

      segment.close(false);
      // ensure that append fails.
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      try {
        appender.append(segment, buffer);
        fail("Append should have failed because segments are closed");
      } catch (StoreException e) {
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
     * @throws StoreException
     */
    void append(LogSegment segment, ByteBuffer buffer) throws StoreException;
  }
}

