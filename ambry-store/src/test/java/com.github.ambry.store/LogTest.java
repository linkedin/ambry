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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    public void append(Log log, ByteBuffer buffer) throws IOException {
      int writeSize = buffer.remaining();
      int written = log.appendFrom(buffer);
      assertEquals("Size written did not match size of buffer provided", writeSize, written);
    }
  };
  private static final Appender CHANNEL_APPENDER = new Appender() {
    @Override
    public void append(Log log, ByteBuffer buffer) throws IOException {
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
  public LogTest() throws IOException {
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
  public void comprehensiveTest() throws IOException {
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
  public void constructionBadArgsTest() throws IOException {
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

    // file which is not a directory
    File file = create(LogSegmentNameHelper.nameToFilename(LogSegmentNameHelper.generateFirstSegmentName(1)));
    try {
      new Log(file.getAbsolutePath(), 1, 1, metrics);
      fail("Construction should have failed");
    } catch (IOException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests cases where bad arguments are provided to the append operations.
   * @throws IOException
   */
  @Test
  public void appendErrorCasesTest() throws IOException {
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
  public void setActiveSegmentBadArgsTest() throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    long numSegments = LOG_CAPACITY / SEGMENT_CAPACITY;
    try {
      log.setActiveSegment(LogSegmentNameHelper.getName(numSegments + 1, 0));
      fail("Should have failed to set a non existent segment as active");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#getDifference(Offset, Offset)}.
   * @throws IOException
   */
  @Test
  public void getDifferenceBadArgsTest() throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    long numSegments = LOG_CAPACITY / SEGMENT_CAPACITY;
    Offset badSegmentOffset = new Offset(LogSegmentNameHelper.getName(numSegments + 1, 0), 0);
    Offset badOffsetOffset =
        new Offset(log.getFirstSegment().getName(), log.getFirstSegment().getCapacityInBytes() + 1);
    List<Pair<Offset, Offset>> pairsToCheck = new ArrayList<>();
    pairsToCheck.add(new Pair<>(log.getStartOffset(), badSegmentOffset));
    pairsToCheck.add(new Pair<>(badSegmentOffset, log.getEndOffset()));
    pairsToCheck.add(new Pair<>(log.getStartOffset(), badOffsetOffset));
    pairsToCheck.add(new Pair<>(badOffsetOffset, log.getEndOffset()));

    try {
      for (Pair<Offset, Offset> pairToCheck : pairsToCheck) {
        try {
          log.getDifference(pairToCheck.getFirst(), pairToCheck.getSecond());
          fail("Should have failed to get difference with invalid offset input. Input was [" + pairToCheck.getFirst()
              + ", " + pairToCheck.getSecond() + "]");
        } catch (IllegalArgumentException e) {
          // expected. Nothing to do.
        }
      }
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#getNextSegment(LogSegment)}.
   * @throws IOException
   */
  @Test
  public void getNextSegmentBadArgsTest() throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    File file = create(LogSegmentNameHelper.nameToFilename(LogSegmentNameHelper.generateFirstSegmentName(1)));
    LogSegment segment = new LogSegment(LogSegmentNameHelper.getName(1, 1), file, 1, metrics, false);
    try {
      log.getNextSegment(segment);
      fail("Getting next segment should have failed because provided segment does not exist in the log");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#getFileSpanForMessage(Offset, long)}.
   * @throws IOException
   */
  @Test
  public void getFileSpanForMessageBadArgsTest() throws IOException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, SEGMENT_CAPACITY, metrics);
    try {
      LogSegment firstSegment = log.getFirstSegment();
      log.setActiveSegment(firstSegment.getName());
      Offset endOffsetOfPrevMessage = new Offset(firstSegment.getName(), firstSegment.getEndOffset() + 1);
      try {
        log.getFileSpanForMessage(endOffsetOfPrevMessage, 1);
        fail("Should have failed because endOffsetOfPrevMessage > endOffset of log segment");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
      // write a single byte into the log
      endOffsetOfPrevMessage = log.getStartOffset();
      CHANNEL_APPENDER.append(log, ByteBuffer.allocate(1));
      try {
        // provide the wrong size
        log.getFileSpanForMessage(endOffsetOfPrevMessage, 2);
        fail("Should have failed because endOffsetOfPrevMessage + size > endOffset of log segment");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
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
      File file = create(LogSegmentNameHelper.nameToFilename(name));
      new LogSegment(name, file, segmentCapacity, metrics, false).close();
      segmentNames.add(name);
    } else {
      for (int i = 0; i < numToCreate; i++) {
        long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        String name = LogSegmentNameHelper.getName(pos, gen);
        File file = create(LogSegmentNameHelper.nameToFilename(name));
        new LogSegment(name, file, segmentCapacity, metrics, true).close();
        segmentNames.add(name);
      }
    }
    Collections.sort(segmentNames, LogSegmentNameHelper.COMPARATOR);
    return segmentNames;
  }

  /**
   * Creates a file with name {@code filename}.
   * @param filename the desired name of the file to be created.
   * @return a {@link File} instance pointing the newly created file named {@code filename}.
   * @throws IOException
   */
  private File create(String filename) throws IOException {
    File file = new File(tempDir, filename);
    if (file.exists()) {
      assertTrue(file.getAbsolutePath() + " already exists and could not be deleted", file.delete());
    }
    assertTrue("Segment file could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    file.deleteOnExit();
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
      List<String> expectedSegmentNames, int segmentIdxToMarkActive, Appender appender) throws IOException {
    long numSegments = (logCapacity - 1) / segmentCapacity + 1;
    Log log = new Log(tempDir.getAbsolutePath(), logCapacity, segmentCapacity, metrics);
    try {
      // only preloaded segments should be in expectedSegmentNames.
      checkLog(log, Math.min(logCapacity, segmentCapacity), numSegments, expectedSegmentNames);
      String activeSegmentName = expectedSegmentNames.get(segmentIdxToMarkActive);
      log.setActiveSegment(activeSegmentName);
      // all segment files from segmentIdxToMarkActive + 1 to expectedSegmentNames.size() - 1 will be freed.
      List<String> prunedSegmentNames = expectedSegmentNames.subList(0, segmentIdxToMarkActive + 1);
      checkLog(log, Math.min(logCapacity, segmentCapacity), numSegments, prunedSegmentNames);
      List<String> allSegmentNames = getSegmentNames(numSegments, prunedSegmentNames);
      writeAndCheckLog(log, logCapacity, Math.min(logCapacity, segmentCapacity), numSegments - segmentIdxToMarkActive,
          writeSize, allSegmentNames, segmentIdxToMarkActive, appender);
      // log full - so all segments should be there
      assertEquals("Unexpected number of segments", numSegments, allSegmentNames.size());
      checkLog(log, Math.min(logCapacity, segmentCapacity), numSegments, allSegmentNames);
      doDifferenceTest(log, allSegmentNames);
      flushCloseAndValidate(log);
      checkLogReload(logCapacity, Math.min(logCapacity, segmentCapacity), allSegmentNames);
    } finally {
      log.close();
      cleanDirectory(tempDir);
    }
  }

  /**
   * Checks the log to ensure segment names, capacities and count.
   * @param log the {@link Log} instance to check.
   * @param expectedSegmentCapacity the expected capacity of each segment.
   * @param numFinalSegments the max number of segments of the log.
   * @param expectedSegmentNames the expected names of all segments that should have been created in the {@code log}.
   * @throws IOException
   */
  private void checkLog(Log log, long expectedSegmentCapacity, long numFinalSegments, List<String> expectedSegmentNames)
      throws IOException {
    LogSegment nextSegment = log.getFirstSegment();
    for (String segmentName : expectedSegmentNames) {
      assertEquals("Next segment is not as expected", segmentName, nextSegment.getName());
      LogSegment segment = log.getSegment(segmentName);
      assertEquals("Segment name is not as expected", segmentName, segment.getName());
      assertEquals("Segment capacity not as expected", expectedSegmentCapacity, segment.getCapacityInBytes());
      assertEquals("Segment returned by getSegment() is incorrect", segment, log.getSegment(segment.getName()));
      nextSegment = log.getNextSegment(segment);
    }
    assertNull("Next segment should be null", nextSegment);
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
      List<String> segmentNames, int activeSegmentIdx, Appender appender) throws IOException {
    byte[] buf = TestUtils.getRandomBytes((int) writeSize);
    long expectedUsedCapacity = logCapacity - segmentCapacity * segmentsLeft;
    int nextSegmentIdx = activeSegmentIdx + 1;
    LogSegment expectedActiveSegment = log.getSegment(segmentNames.get(activeSegmentIdx));
    String activeSegName = expectedActiveSegment.getName();
    // add header space (if any) from the active segment.
    long currentSegmentWriteSize = expectedActiveSegment.getEndOffset();
    expectedUsedCapacity += currentSegmentWriteSize;
    while (expectedUsedCapacity + writeSize <= logCapacity) {
      Offset endOffsetOfLastMessage = new Offset(activeSegName, currentSegmentWriteSize);
      appender.append(log, ByteBuffer.wrap(buf));
      FileSpan fileSpanOfMessage = log.getFileSpanForMessage(endOffsetOfLastMessage, writeSize);
      FileSpan expectedFileSpanForMessage =
          new FileSpan(endOffsetOfLastMessage, new Offset(activeSegName, currentSegmentWriteSize + writeSize));
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
        activeSegName = expectedActiveSegment.getName();
        // currentSegmentWriteSize must be equal to LogSegment.HEADER_SIZE + writeSize
        assertEquals("Unexpected size of new active segment", writeSize + LogSegment.HEADER_SIZE,
            currentSegmentWriteSize);
        expectedFileSpanForMessage = new FileSpan(new Offset(activeSegName, expectedActiveSegment.getStartOffset()),
            new Offset(activeSegName, currentSegmentWriteSize));
        nextSegmentIdx++;
      }
      assertEquals("StartOffset of message  not as expected", expectedFileSpanForMessage.getStartOffset(),
          fileSpanOfMessage.getStartOffset());
      assertEquals("EndOffset of message  not as expected", expectedFileSpanForMessage.getEndOffset(),
          fileSpanOfMessage.getEndOffset());
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
   * Tests {@link Log#getDifference(Offset, Offset)} for all cases.
   * 1. Same segment and offset
   * 2. Same segment, different offsets
   * 3. Boundary offsets (combination of -start of lower, end of lower- and -start of higher, end of higher-
   * 4. Random offsets that are not boundary offsets.
   * @param log the {@link Log} instance to use.
   * @param segmentNames the names of all the segments in the {@code log}.
   */
  private void doDifferenceTest(Log log, List<String> segmentNames) {
    int lowerIdx = 0;
    int higherIdx = 0;
    if (segmentNames.size() > 1) {
      // pick any segment except the last one
      lowerIdx = TestUtils.RANDOM.nextInt(segmentNames.size() - 1);
      // pick a segment higher than lowerIdx
      higherIdx = lowerIdx + 1 + TestUtils.RANDOM.nextInt(segmentNames.size() - lowerIdx - 1);
    }
    String lowerSegmentName = segmentNames.get(lowerIdx);
    String higherSegmentName = segmentNames.get(higherIdx);
    LogSegment lowerSegment = log.getSegment(lowerSegmentName);
    LogSegment higherSegment = log.getSegment(higherSegmentName);

    // get boundary offsets
    Offset lowerLowBound = new Offset(lowerSegmentName, 0);
    Offset lowerHighBound = new Offset(lowerSegmentName, lowerSegment.getEndOffset());
    Offset higherLowBound = new Offset(higherSegmentName, 0);
    Offset higherHighBound = new Offset(higherSegmentName, higherSegment.getEndOffset());

    // get random offsets that are not 0 or the end offset
    Offset lower =
        new Offset(lowerSegmentName, Utils.getRandomLong(TestUtils.RANDOM, lowerSegment.getEndOffset() - 1) + 1);
    Offset higher =
        new Offset(higherSegmentName, Utils.getRandomLong(TestUtils.RANDOM, higherSegment.getEndOffset() - 1) + 1);

    checkDifference(log, lower, higher, higherIdx - lowerIdx, lowerSegment.getCapacityInBytes());
    checkDifference(log, lowerLowBound, higherLowBound, higherIdx - lowerIdx, lowerSegment.getCapacityInBytes());
    checkDifference(log, lowerLowBound, higherHighBound, higherIdx - lowerIdx, lowerSegment.getCapacityInBytes());
    checkDifference(log, lowerHighBound, higherLowBound, higherIdx - lowerIdx, lowerSegment.getCapacityInBytes());
    checkDifference(log, lowerHighBound, higherHighBound, higherIdx - lowerIdx, lowerSegment.getCapacityInBytes());
    checkDifference(log, lower, lower, 0, lowerSegment.getCapacityInBytes());
    checkDifference(log, lowerLowBound, lower, 0, lowerSegment.getCapacityInBytes());
  }

  /**
   * Checks the output of {@link Log#getDifference(Offset, Offset)} for {@code higher} and {@code lower} against the
   * expected value.
   * @param log the {@link Log} instance to use.
   * @param lower the {@link Offset} that is the "lower" offset.
   * @param higher the {@link Offset} that is the "higher" offset.
   * @param numSegmentHops the number of segment hops it takes to get from {@code lower} to {@code higher}.
   * @param segmentCapacity the capacity of each segment.
   */
  private void checkDifference(Log log, Offset lower, Offset higher, int numSegmentHops, long segmentCapacity) {
    long expectedDifference;
    if (numSegmentHops == 0) {
      expectedDifference = higher.getOffset() - lower.getOffset();
    } else {
      // capacities of full segments b/w higher and lower
      expectedDifference = (numSegmentHops - 1) * segmentCapacity;
      // diff b/w lower segment cap and the offset in the lower segment.
      expectedDifference += segmentCapacity - lower.getOffset();
      // the offset in the higher segment
      expectedDifference += higher.getOffset();
    }
    assertEquals("Difference returned is not as expected", expectedDifference, log.getDifference(higher, lower));
    assertEquals("Difference returned is not as expected", -expectedDifference, log.getDifference(lower, higher));
  }

  /**
   * Reloads a {@link Log} (by creating a new instance) that already has segments and mimics changed configs and ensures
   * that the config is ignored.
   * @param originalLogCapacity the original total capacity of the log.
   * @param originalSegmentCapacity the original segment capacity of the log.
   * @param allSegmentNames the expected names of the all the segments.
   * @throws IOException
   */
  private void checkLogReload(long originalLogCapacity, long originalSegmentCapacity, List<String> allSegmentNames)
      throws IOException {
    // modify the segment capacity (mimics modifying the config)
    long[] newConfigs = {originalSegmentCapacity - 1, originalSegmentCapacity + 1};
    for (long newConfig : newConfigs) {
      Log log = new Log(tempDir.getAbsolutePath(), originalLogCapacity, newConfig, metrics);
      try {
        // the new config should be ignored.
        checkLog(log, originalSegmentCapacity, allSegmentNames.size(), allSegmentNames);
      } finally {
        log.close();
      }
    }
  }

  /**
   * Flushes and closes the log and validates that the log has been correctly closed.
   * @param log the {@link Log} intance to flush and close.
   * @throws IOException
   */
  private void flushCloseAndValidate(Log log) throws IOException {
    // flush should not throw any exceptions
    log.flush();
    // close log and ensure segments are closed
    log.close();
    LogSegment segment = log.getFirstSegment();
    while (segment != null) {
      assertFalse("LogSegment has not been closed", segment.getView().getSecond().isOpen());
      segment = log.getNextSegment(segment);
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
    void append(Log log, ByteBuffer buffer) throws IOException;
  }
}
