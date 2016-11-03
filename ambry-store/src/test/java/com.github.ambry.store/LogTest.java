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
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link Log}.
 */
public class LogTest {
  private static final long LOG_CAPACITY = 5 * 1024;
  private static final long SEGMENT_CAPACITY = 1024;
  private static final Appender BUFFER_APPENDER = new Appender() {
    @Override
    public void append(Log log, ByteBuffer buffer)
        throws IOException {
      int writeSize = buffer.remaining();
      int written = log.appendFrom(buffer);
      assertEquals("Size written did not match size of buffer provided", writeSize, written);
    }
  };
  private static final Appender CHANNEL_APPENDER = new Appender() {
    @Override
    public void append(Log log, ByteBuffer buffer)
        throws IOException {
      int writeSize = buffer.remaining();
      log.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeSize);
      assertFalse("The buffer was not completely written", buffer.hasRemaining());
    }
  };

  private final File tempDir;
  private final StoreMetrics metrics;

  /**
   * Creates a temporary directory to store the segment files.
   * @throws IOException
   */
  public LogTest()
      throws IOException {
    tempDir = Files.createTempDirectory("logDir-" + UtilsTest.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    metrics = new StoreMetrics(tempDir.getName(), new MetricRegistry());
  }

  /**
   * Cleans up the temporary directory and deletes it.
   */
  @After
  public void cleanup() {
    cleanDirectory(tempDir);
    assertTrue("The directory [" + tempDir.getAbsolutePath() + "] could not be deleted", tempDir.delete());
  }

  /**
   * Tests almost all the functions and cases of {@link Log}.
   * </p>
   * Each individual test has the following variable parameters.
   * 1. The capacity of each segment.
   * 2. The size of the write on the log.
   * 3. The segment that is being marked as active (beginning, middle, end etc).
   * 4. The number of segment files that have been created and already exist in the folder.
   * 5. The type of append operation being used.
   * @throws IOException
   */
  @Test
  public void comprehensiveTest()
      throws IOException {
    Appender[] appenders = {BUFFER_APPENDER, CHANNEL_APPENDER};
    for (Appender appender : appenders) {
      // for single segment log
      setupAndDoComprehensiveTest(LOG_CAPACITY, LOG_CAPACITY, appender);
      setupAndDoComprehensiveTest(LOG_CAPACITY, LOG_CAPACITY + 1, appender);
      // for multiple segment log
      setupAndDoComprehensiveTest(LOG_CAPACITY, SEGMENT_CAPACITY, appender);
    }
  }

  /**
   * Tests cases where bad arguments are provided to the {@link Log} constructor.
   * @throws IOException
   */
  @Test
  public void constructionBadArgsTest()
      throws IOException {
    List<Pair<Long, Long>> logAndSegmentSizes = new ArrayList<>();
    // <=0 values for capacities
    logAndSegmentSizes.add(new Pair<>(-1L, SEGMENT_CAPACITY));
    logAndSegmentSizes.add(new Pair<>(LOG_CAPACITY, -1L));
    logAndSegmentSizes.add(new Pair<>(0L, SEGMENT_CAPACITY));
    logAndSegmentSizes.add(new Pair<>(LOG_CAPACITY, 0L));
    // log capacity is not perfectly divisible by segment capacity
    logAndSegmentSizes.add(new Pair<>(LOG_CAPACITY, LOG_CAPACITY - 1));

    for (Pair<Long, Long> logAndSegmentSize : logAndSegmentSizes) {
      try {
        new Log(tempDir.getAbsolutePath(), logAndSegmentSize.getFirst(), logAndSegmentSize.getSecond(), metrics);
        fail("Construction should have failed");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Tests cases where bad arguments are provided to the append operations.
   * @throws IOException
   */
  @Test
  public void appendErrorCasesTest()
      throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    try {
      // write exceeds size of a single segment.
      ByteBuffer buffer =
          ByteBuffer.wrap(TestUtils.getRandomBytes((int) (SEGMENT_CAPACITY + 1 - LogSegment.HEADER_SIZE)));
      try {
        log.appendFrom(buffer);
        fail("Cannot append a write of size greater than log segment size");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }

      try {
        log.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), buffer.remaining());
        fail("Cannot append a write of size greater than log segment size");
      } catch (IllegalArgumentException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#setActiveSegment(String)}.
   * @throws IOException
   */
  @Test
  public void setActiveSegmentBadArgsTest()
      throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    long numSegments = LOG_CAPACITY / SEGMENT_CAPACITY;
    try {
      log.setActiveSegment(LogSegmentNameHelper.getName(numSegments + 1, 0));
      fail("Should have failed to set a non exsistent segment as active");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests the deprecated {@link Log#getView(List)} function.
   * @throws IOException
   */
  @Test
  public void getViewTest()
      throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    try {
      long writeStartOffset = log.getEndOffset().getOffset();
      long writeSize = (int) (SEGMENT_CAPACITY - writeStartOffset);
      byte[] buf = TestUtils.getRandomBytes((int) (writeSize));
      log.appendFrom(ByteBuffer.wrap(buf));
      List<BlobReadOptions> list = new ArrayList<>();
      list.add(new BlobReadOptions(writeStartOffset, writeSize, -1, null));
      StoreMessageReadSet readSet = log.getView(list);
      ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(buf.length));
      readSet.writeTo(0, channel, 0, buf.length);
      assertArrayEquals("Data read does not match data written", buf, channel.getBuffer().array());
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  // helpers

  // general

  /**
   * Deletes all the files in {@code tempDir}.
   * @param tempDir the directory whose files have to be deleted.
   */
  private void cleanDirectory(File tempDir) {
    File[] files = tempDir.listFiles();
    if (files != null) {
      for (File file : files) {
        assertTrue("The file [" + file.getAbsolutePath() + "] could not be deleted", file.delete());
      }
    }
  }

  // comprehensiveTest() helpers

  /**
   * Sets up all the required variables for the comprehensive test.
   * @param logCapacity the capacity of the log.
   * @param segmentCapacity the capacity of a single segment in the log.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void setupAndDoComprehensiveTest(long logCapacity, long segmentCapacity, Appender appender)
      throws IOException {
    long numSegments = (logCapacity - 1) / segmentCapacity + 1;
    long maxWriteSize = Math.min(logCapacity, segmentCapacity);
    if (numSegments > 1) {
      // backwards compatibility.
      maxWriteSize -= LogSegment.HEADER_SIZE;
    }
    long[] writeSizes = {1, maxWriteSize, Utils.getRandomLong(TestUtils.RANDOM, maxWriteSize - 2) + 2};
    // the number of segment files to create before creating the log.
    for (int i = 0; i <= numSegments; i++) {
      // the sizes of the writes to the log.
      for (long writeSize : writeSizes) {
        // the index of the pre-created segment file that will be marked as active.
        for (int j = 0; j <= i; j++) {
          List<String> expectedSegmentNames;
          if (i == 0) {
            // first startup case - segment file is not pre-created but will be created by the Log.
            expectedSegmentNames = new ArrayList<>();
            expectedSegmentNames.add(LogSegmentNameHelper.generateFirstSegmentName(numSegments));
          } else if (i == j) {
            // invalid index for anything other than i == 0.
            break;
          } else {
            // subsequent startup cases where there have been a few segments created and maybe some are filled.
            expectedSegmentNames = createSegmentFiles(i, numSegments, segmentCapacity);
          }
          expectedSegmentNames = Collections.unmodifiableList(expectedSegmentNames);
          doComprehensiveTest(logCapacity, segmentCapacity, writeSize, expectedSegmentNames, j, appender);
        }
      }
    }
  }

  /**
   * Creates {@code numToCreate} segment files on disk.
   * @param numToCreate the number of segment files to create.
   * @param numFinalSegments the total number of segment files in the log.
   * @return the names of the created files.
   * @throws IOException
   */
  private List<String> createSegmentFiles(int numToCreate, long numFinalSegments, long segmentCapacity)
      throws IOException {
    if (numToCreate > numFinalSegments) {
      throw new IllegalArgumentException("num segments to create cannot be more than num final segments");
    }
    List<String> segmentNames = new ArrayList<>(numToCreate);
    if (numFinalSegments == 1) {
      String name = LogSegmentNameHelper.generateFirstSegmentName(numFinalSegments);
      create(LogSegmentNameHelper.nameToFilename(name), segmentCapacity);
      segmentNames.add(name);
    } else {
      for (int i = 0; i < numToCreate; i++) {
        long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        String name = LogSegmentNameHelper.getName(pos, gen);
        create(LogSegmentNameHelper.nameToFilename(name), segmentCapacity);
        segmentNames.add(name);
      }
    }
    Collections.sort(segmentNames, LogSegmentNameHelper.COMPARATOR);
    return segmentNames;
  }

  /**
   * Creates a file with name {@code filename}.
   * @param filename the desired name of the file to be created.
   * @param capacityInBytes the capacity of the file in bytes. The file is not preallocated, but this information is
   *                        used to write the {@link LogSegment} headers.
   * @return a {@link File} instance pointing the newly created file named {@code filename}.
   * @throws IOException
   */
  private File create(String filename, long capacityInBytes)
      throws IOException {
    File file = new File(tempDir, filename);
    if (file.exists()) {
      assertTrue(file.getAbsolutePath() + " already exists and could not be deleted", file.delete());
    }
    assertTrue("Segment file could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    file.deleteOnExit();
    // create a log segment so that it writes the headers
    LogSegment segment =
        new LogSegment(LogSegmentNameHelper.nameFromFilename(filename), file, capacityInBytes, metrics, true);
    segment.close();
    return file;
  }

  /**
   * Does the comprehensive test.
   * 1. Creates the log.
   * 2. Checks that the pre created segment files (if any) have been picked up.
   * 3. Sets the active segment.
   * 4. Performs writes until the log is filled and checks end offsets and segment names/counts.
   * 5. Flushes, closes and validates close.
   * @param logCapacity the log capacity.
   * @param segmentCapacity the capacity of each segment.
   * @param writeSize the size of each write to the log.
   * @param expectedSegmentNames the expected names of the segments in the log.
   * @param segmentIdxToMarkActive the index of the name of the active segment in {@code expectedSegmentNames}. Also its
   *                               absolute index in the {@link Log}.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void doComprehensiveTest(long logCapacity, long segmentCapacity, long writeSize,
      List<String> expectedSegmentNames, int segmentIdxToMarkActive, Appender appender)
      throws IOException {
    long numSegments = (logCapacity - 1) / segmentCapacity + 1;
    Log log = new Log(tempDir.getAbsolutePath(), logCapacity, segmentCapacity, metrics);
    try {
      // only preloaded segments should be in expectedSegmentNames.
      checkLog(log, Math.min(logCapacity, segmentCapacity), numSegments, expectedSegmentNames);
      String activeSegmentName = expectedSegmentNames.get(segmentIdxToMarkActive);
      log.setActiveSegment(activeSegmentName);
      List<String> allSegmentNames = getSegmentNames(numSegments, expectedSegmentNames);
      writeAndCheckLog(log, logCapacity, Math.min(logCapacity, segmentCapacity), numSegments - segmentIdxToMarkActive,
          writeSize, allSegmentNames, segmentIdxToMarkActive, appender);
      // log full - so all segments should be there
      assertEquals("Unexpected number of segments", numSegments, allSegmentNames.size());
      checkLog(log, Math.min(logCapacity, segmentCapacity), numSegments, allSegmentNames);
      flushCloseAndValidate(log);
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Checks the log to ensure segment names, capacities and count.
   * @param log the {@link Log} instance to check.
   * @param expectedSegmentCapacity the expected capacity of each segment.
   * @param expectedSegmentNames the expected names of all segments that should have been created in the {@code log}.
   * @throws IOException
   */
  private void checkLog(Log log, long expectedSegmentCapacity, long numFinalSegments, List<String> expectedSegmentNames)
      throws IOException {
    // get all log segments from the log and validates them
    for (String segmentName : expectedSegmentNames) {
      LogSegment segment = log.getSegment(segmentName);
      assertEquals("Segment name is not as expected", segmentName, segment.getName());
      assertEquals("Segment capacity not as expected", expectedSegmentCapacity, segment.getCapacityInBytes());
      assertEquals("Segment returned by getSegment() is incorrect", segment, log.getSegment(segment.getName()));
    }
    assertEquals("Log segments reported is wrong", expectedSegmentNames.size(), log.getSegmentCount());
    long startOffsetInSegment = numFinalSegments > 1 ? LogSegment.HEADER_SIZE : 0;
    Offset expectedStartOffset = new Offset(expectedSegmentNames.get(0), startOffsetInSegment);
    assertEquals("Start offset is wrong", expectedStartOffset, log.getStartOffset());
  }

  /**
   * Writes data to the log and checks for end offset (segment and log), roll over and used capacity.
   * @param log the {@link Log} instance to use.
   * @param logCapacity the total capacity of the {@code log}.
   * @param segmentCapacity the capacity of each segment in the {@code log}.
   * @param segmentsLeft the number of segments in the log that still have capacity remaining.
   * @param writeSize the size of each write to the log.
   * @param segmentNames the names of *all* the segments in the log including the ones that may not yet have been
   *                     created.
   * @param activeSegmentIdx the index of the name of the active segment in {@code segmentNames}. Also its absolute
   *                         index in the {@link Log}.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void writeAndCheckLog(Log log, long logCapacity, long segmentCapacity, long segmentsLeft, long writeSize,
      List<String> segmentNames, int activeSegmentIdx, Appender appender)
      throws IOException {
    byte[] buf = TestUtils.getRandomBytes((int) writeSize);
    long expectedUsedCapacity = logCapacity - segmentCapacity * segmentsLeft;
    int nextSegmentIdx = activeSegmentIdx + 1;
    String expectedNextSegmentName = null;
    LogSegment expectedActiveSegment = log.getSegment(segmentNames.get(activeSegmentIdx));
    if (segmentNames.size() > nextSegmentIdx) {
      expectedNextSegmentName = segmentNames.get(nextSegmentIdx);
    }
    // add header space (if any) from the active segment.
    long currentSegmentWriteSize = expectedActiveSegment.getEndOffset();
    expectedUsedCapacity += currentSegmentWriteSize;
    while (expectedUsedCapacity + writeSize <= logCapacity) {
      appender.append(log, ByteBuffer.wrap(buf));
      currentSegmentWriteSize += writeSize;
      expectedUsedCapacity += writeSize;
      if (currentSegmentWriteSize > segmentCapacity) {
        // calculate the end offset to ensure no partial writes.
        currentSegmentWriteSize =
            LogSegment.HEADER_SIZE + currentSegmentWriteSize - expectedActiveSegment.getEndOffset();
        // add the "wasted" space to expectedUsedCapacity
        expectedUsedCapacity += LogSegment.HEADER_SIZE + segmentCapacity - expectedActiveSegment.getEndOffset();
        expectedActiveSegment = log.getSegment(segmentNames.get(nextSegmentIdx));
        assertNotNull("Next active segment is null", expectedActiveSegment);
        assertEquals("Next active segment name does not match", expectedNextSegmentName,
            expectedActiveSegment.getName());
        nextSegmentIdx++;
        if (segmentNames.size() > nextSegmentIdx) {
          expectedNextSegmentName = segmentNames.get(nextSegmentIdx);
        }
      }
      assertEquals("Active segment end offset not as expected", currentSegmentWriteSize,
          expectedActiveSegment.getEndOffset());
      assertEquals("End offset not as expected", new Offset(expectedActiveSegment.getName(), currentSegmentWriteSize),
          log.getEndOffset());
      assertEquals("Used capacity not as expected", expectedUsedCapacity, log.getUsedCapacity());
    }
    // try one more write that should fail
    try {
      appender.append(log, ByteBuffer.wrap(buf));
      fail("Should have failed because max capacity has been reached");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Returns {@code count} number of expected segment file names.
   * @param count the number of total segment names needed (including the ones in {@code existingNames}).
   * @param existingNames the names of log segments that are already known.
   * @return expected segment file names of {@code count} segment files.
   */
  private List<String> getSegmentNames(long count, List<String> existingNames) {
    List<String> segmentNames = new ArrayList<>();
    if (existingNames != null) {
      segmentNames.addAll(existingNames);
    }
    if (count == segmentNames.size()) {
      return segmentNames;
    } else if (segmentNames.size() == 0) {
      segmentNames.add(LogSegmentNameHelper.generateFirstSegmentName(count));
    }
    for (int i = segmentNames.size(); i < count; i++) {
      String nextSegmentName = LogSegmentNameHelper.getNextPositionName(segmentNames.get(i - 1));
      segmentNames.add(nextSegmentName);
    }
    return segmentNames;
  }

  /**
   * Flushes and closes the log and validates that the log has been correctly closed.
   * @param log the {@link Log} intance to flush and close.
   * @throws IOException
   */
  private void flushCloseAndValidate(Log log)
      throws IOException {
    // flush should not throw any exceptions
    log.flush();
    // close log and ensure segments are closed
    log.close();
    Iterator<Map.Entry<String, LogSegment>> iterator = log.getSegmentIterator();
    while (iterator.hasNext()) {
      assertFalse("LogSegment has not been closed", iterator.next().getValue().getView().getSecond().isOpen());
    }
  }

  /**
   * Interface for abstracting append operations.
   */
  private interface Appender {
    /**
     * Appends the data of {@code buffer} to {@code log}.
     * @param log the {@link Log} to append {@code buffer} to.
     * @param buffer the data to append to {@code log}.
     * @throws IOException
     */
    void append(Log log, ByteBuffer buffer)
        throws IOException;
  }
}
