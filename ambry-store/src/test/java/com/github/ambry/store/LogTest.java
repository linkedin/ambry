/*
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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static com.github.ambry.store.StoreTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link Log}.
 */
@RunWith(Parameterized.class)
public class LogTest {
  private static final long SEGMENT_CAPACITY = 512;
  private static final long LOG_CAPACITY = 12 * SEGMENT_CAPACITY;
  private static final Appender BUFFER_APPENDER = new Appender() {
    @Override
    public void append(Log log, ByteBuffer buffer) throws StoreException {
      int writeSize = buffer.remaining();
      int written = log.appendFrom(buffer);
      assertEquals("Size written did not match size of buffer provided", writeSize, written);
    }
  };
  private static final Appender CHANNEL_APPENDER = new Appender() {
    @Override
    public void append(Log log, ByteBuffer buffer) throws StoreException {
      int writeSize = buffer.remaining();
      log.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeSize);
      assertFalse("The buffer was not completely written", buffer.hasRemaining());
    }
  };

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  private final File tempDir;
  private final StoreMetrics metrics;
  private final Set<Long> positionsGenerated = new HashSet<>();
  private final boolean setFilePermissionEnabled;

  /**
   * Creates a temporary directory to store the segment files.
   * @throws IOException
   */
  public LogTest(boolean setFilePermissionEnabled) throws IOException {
    tempDir = Files.createTempDirectory("logDir-" + TestUtils.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    MetricRegistry metricRegistry = new MetricRegistry();
    metrics = new StoreMetrics(metricRegistry);
    this.setFilePermissionEnabled = setFilePermissionEnabled;
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
  public void comprehensiveTest() throws IOException, StoreException {
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
   * Tests cases where construction of {@link Log} failed either because of bad arguments or because of store exception.
   * @throws IOException
   */
  @Test
  public void constructionFailureTest() throws IOException, StoreException {
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
        new Log(tempDir.getAbsolutePath(), logAndSegmentSize.getFirst(), StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
            createStoreConfig(logAndSegmentSize.getSecond(), setFilePermissionEnabled), metrics);
        fail("Construction should have failed");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }

    // file which is not a directory
    File file = create(LogSegmentNameHelper.nameToFilename(LogSegmentNameHelper.generateFirstSegmentName(false)));
    try {
      new Log(file.getAbsolutePath(), 1, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
          createStoreConfig(1, setFilePermissionEnabled), metrics);
      fail("Construction should have failed");
    } catch (StoreException e) {
      // expected. Nothing to do.
    }

    // Store exception occurred during construction
    DiskSpaceAllocator mockDiskAllocator = Mockito.mock(DiskSpaceAllocator.class);
    doThrow(new IOException(StoreException.IO_ERROR_STR)).when(mockDiskAllocator)
        .allocate(any(File.class), anyLong(), anyString(), anyBoolean());
    List<LogSegment> segmentsToLoad = Collections.emptyList();
    try {
      new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, mockDiskAllocator,
          createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics, true, segmentsToLoad,
          Collections.EMPTY_LIST.iterator());
      fail("Should have failed to when constructing log");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.IOError, e.getErrorCode());
    }
  }

  /**
   * Tests cases where bad arguments are provided to the append operations.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void appendErrorCasesTest() throws IOException, StoreException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics);
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
      log.close(false);
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#setActiveSegment(String)}.
   * @throws IOException
   */
  @Test
  public void setActiveSegmentBadArgsTest() throws IOException, StoreException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics);
    long numSegments = LOG_CAPACITY / SEGMENT_CAPACITY;
    try {
      log.setActiveSegment(LogSegmentNameHelper.getName(numSegments + 1, 0));
      fail("Should have failed to set a non existent segment as active");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      log.close(false);
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#getNextSegment(LogSegment)}.
   * @throws IOException
   */
  @Test
  public void getNextSegmentBadArgsTest() throws IOException, StoreException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics);
    LogSegment segment = getLogSegment(LogSegmentNameHelper.getName(1, 1), SEGMENT_CAPACITY, true);
    try {
      log.getNextSegment(segment);
      fail("Getting next segment should have failed because provided segment does not exist in the log");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      log.close(false);
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#getPrevSegment(LogSegment)}.
   * @throws IOException
   */
  @Test
  public void getPrevSegmentBadArgsTest() throws IOException, StoreException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics);
    LogSegment segment = getLogSegment(LogSegmentNameHelper.getName(1, 1), SEGMENT_CAPACITY, true);
    try {
      log.getPrevSegment(segment);
      fail("Getting prev segment should have failed because provided segment does not exist in the log");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    } finally {
      log.close(false);
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests cases where bad arguments are provided to {@link Log#getFileSpanForMessage(Offset, long)}.
   * @throws IOException
   */
  @Test
  public void getFileSpanForMessageBadArgsTest() throws IOException, StoreException {
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics);
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
      endOffsetOfPrevMessage = log.getEndOffset();
      CHANNEL_APPENDER.append(log, ByteBuffer.allocate(1));
      try {
        // provide the wrong size
        log.getFileSpanForMessage(endOffsetOfPrevMessage, 2);
        fail("Should have failed because endOffsetOfPrevMessage + size > endOffset of log segment");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    } finally {
      log.close(false);
      cleanDirectory(tempDir);
    }
  }

  /**
   * Tests all cases of {@link Log#addSegment(LogSegment, boolean)} and {@link Log#dropSegment(String, boolean)}.
   * @throws IOException
   */
  @Test
  public void addAndDropSegmentTest() throws IOException, StoreException {
    // start with a segment that has a high position to allow for addition of segments
    long activeSegmentPos = 2 * LOG_CAPACITY / SEGMENT_CAPACITY;
    LogSegment loadedSegment = getLogSegment(LogSegmentNameHelper.getName(activeSegmentPos, 0), SEGMENT_CAPACITY, true);
    List<LogSegment> segmentsToLoad = Collections.singletonList(loadedSegment);
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics, true, segmentsToLoad,
        Collections.EMPTY_LIST.iterator());

    // add a segment
    String segmentName = LogSegmentNameHelper.getName(0, 0);
    LogSegment uncountedSegment = getLogSegment(segmentName, SEGMENT_CAPACITY, true);
    log.addSegment(uncountedSegment, false);
    assertEquals("Log segment instance not as expected", uncountedSegment, log.getSegment(segmentName));

    // cannot add past the active segment
    segmentName = LogSegmentNameHelper.getName(activeSegmentPos + 1, 0);
    LogSegment segment = getLogSegment(segmentName, SEGMENT_CAPACITY, true);
    try {
      log.addSegment(segment, false);
      fail("Should not be able to add past the active segment");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // since the previous one did not ask for count of segments to be increased, we should be able to add max - 1
    // more segments. This increments used segment count to the max.
    int max = (int) (LOG_CAPACITY / SEGMENT_CAPACITY);
    for (int i = 1; i < max; i++) {
      segmentName = LogSegmentNameHelper.getName(i, 0);
      segment = getLogSegment(segmentName, SEGMENT_CAPACITY, true);
      log.addSegment(segment, true);
    }

    // fill up the active segment
    ByteBuffer buffer =
        ByteBuffer.allocate((int) (loadedSegment.getCapacityInBytes() - loadedSegment.getStartOffset()));
    CHANNEL_APPENDER.append(log, buffer);
    // write fails because no more log segments can be allocated
    buffer = ByteBuffer.allocate(1);
    try {
      CHANNEL_APPENDER.append(log, buffer);
      fail("Write should have failed because no more log segments should be allocated");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    // drop the uncounted segment
    assertEquals("Segment not as expected", uncountedSegment, log.getSegment(uncountedSegment.getName()));
    File segmentFile = uncountedSegment.getView().getFirst();
    log.dropSegment(uncountedSegment.getName(), false);
    assertNull("Segment should not be present", log.getSegment(uncountedSegment.getName()));
    assertFalse("Segment file should not be present", segmentFile.exists());

    // cannot drop a segment that does not exist
    // cannot drop the active segment
    String[] segmentsToDrop = {uncountedSegment.getName(), loadedSegment.getName()};
    for (String segmentToDrop : segmentsToDrop) {
      try {
        log.dropSegment(segmentToDrop, false);
        fail("Should have failed to drop segment");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }

    // drop a segment and decrement total segment count
    log.dropSegment(log.getFirstSegment().getName(), true);

    // should be able to write now
    buffer = ByteBuffer.allocate(1);
    CHANNEL_APPENDER.append(log, buffer);

    // verify that dropSegment method can catch IOException and correctly convert it to StoreException.
    DiskSpaceAllocator mockDiskAllocator = Mockito.mock(DiskSpaceAllocator.class);
    doThrow(new IOException(StoreException.IO_ERROR_STR)).when(mockDiskAllocator)
        .free(any(File.class), anyLong(), anyString(), anyBoolean());
    segmentsToLoad = Collections.singletonList(
        getLogSegment(LogSegmentNameHelper.getName(activeSegmentPos, 0), SEGMENT_CAPACITY, true));
    Log mockLog = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, mockDiskAllocator,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics, true, segmentsToLoad,
        Collections.EMPTY_LIST.iterator());
    mockLog.addSegment(getLogSegment(LogSegmentNameHelper.getName(1, 0), SEGMENT_CAPACITY, true), true);
    try {
      mockLog.dropSegment(mockLog.getFirstSegment().getName(), true);
      fail("Should have failed to drop segment");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.IOError, e.getErrorCode());
    }
  }

  /**
   * Checks that the constructor that receives segments and segment names iterator,
   * {@link Log#Log(String, long, DiskSpaceAllocator, StoreConfig, StoreMetrics, boolean, List, Iterator)}, loads
   * the segments correctly and uses the iterator to name new segments and uses the default algorithm once the names
   * run out.
   * @throws IOException
   */
  @Test
  public void logSegmentCustomNamesTest() throws IOException, StoreException {
    int numSegments = (int) (LOG_CAPACITY / SEGMENT_CAPACITY);
    LogSegment segment = getLogSegment(LogSegmentNameHelper.getName(0, 0), SEGMENT_CAPACITY, true);
    long startPos = 2 * numSegments;
    List<Pair<String, String>> expectedSegmentAndFileNames = new ArrayList<>(numSegments);
    expectedSegmentAndFileNames.add(new Pair<>(segment.getName(), segment.getView().getFirst().getName()));
    List<Pair<String, String>> segmentNameAndFileNamesDesired = new ArrayList<>();
    String lastName = null;
    for (int i = 0; i < 2; i++) {
      lastName = LogSegmentNameHelper.getName(startPos + i, 0);
      String fileName = LogSegmentNameHelper.nameToFilename(lastName) + "_modified";
      segmentNameAndFileNamesDesired.add(new Pair<>(lastName, fileName));
      expectedSegmentAndFileNames.add(new Pair<>(lastName, fileName));
    }
    for (int i = expectedSegmentAndFileNames.size(); i < numSegments; i++) {
      lastName = LogSegmentNameHelper.getNextPositionName(lastName);
      String fileName = LogSegmentNameHelper.nameToFilename(lastName);
      expectedSegmentAndFileNames.add(new Pair<>(lastName, fileName));
    }

    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics, true,
        Collections.singletonList(segment), segmentNameAndFileNamesDesired.iterator());
    // write enough so that all segments are allocated
    ByteBuffer buffer = ByteBuffer.allocate((int) (segment.getCapacityInBytes() - segment.getStartOffset()));
    for (int i = 0; i < numSegments; i++) {
      buffer.rewind();
      CHANNEL_APPENDER.append(log, buffer);
    }

    segment = log.getFirstSegment();
    for (Pair<String, String> nameAndFilename : expectedSegmentAndFileNames) {
      assertEquals("Segment name does not match", nameAndFilename.getFirst(), segment.getName());
      assertEquals("Segment file does not match", nameAndFilename.getSecond(), segment.getView().getFirst().getName());
      segment = log.getNextSegment(segment);
    }
    assertNull("There should be no more segments", segment);
  }

  /**
   * Test several failure scenarios where exception occurs when instantiating new log segment in ensureCapacity() method.
   * The method should catch the exception, free log segment and restore the counter.
   * @throws Exception
   */
  @Test
  public void ensureCapacityFailureTest() throws Exception {
    int numSegments = (int) (LOG_CAPACITY / SEGMENT_CAPACITY);
    LogSegment segment = getLogSegment(LogSegmentNameHelper.getName(0, 0), SEGMENT_CAPACITY, true);
    String lastName = LogSegmentNameHelper.getName(0, 0);
    List<Pair<String, String>> segmentNameAndFileNamesDesired = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      lastName = LogSegmentNameHelper.getNextPositionName(lastName);
      String fileName = LogSegmentNameHelper.nameToFilename(lastName);
      segmentNameAndFileNamesDesired.add(new Pair<>(lastName, fileName));
    }

    File file = new File("1_0_log");
    File mockFile = Mockito.spy(file);
    when(mockFile.exists()).thenReturn(false);
    DiskSpaceAllocator mockDiskAllocator = Mockito.spy(StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR);
    Log log = new Log(tempDir.getAbsolutePath(), LOG_CAPACITY, mockDiskAllocator,
        createStoreConfig(SEGMENT_CAPACITY, setFilePermissionEnabled), metrics, true,
        Collections.singletonList(segment), segmentNameAndFileNamesDesired.iterator());
    Log mockLog = Mockito.spy(log);
    when(mockLog.allocate(anyString(), anyLong(), anyBoolean())).thenReturn(mockFile);
    long initialUnallocatedSegments = mockLog.getRemainingUnallocatedSegments();

    // write enough so that all segments are allocated
    ByteBuffer buffer = ByteBuffer.allocate((int) (segment.getCapacityInBytes() - segment.getStartOffset()));
    // write first segment
    buffer.rewind();
    CHANNEL_APPENDER.append(mockLog, buffer);

    // Test 1: try to write second segment triggering roll over and calls ensureCapacity()
    try {
      buffer.rewind();
      CHANNEL_APPENDER.append(mockLog, buffer);
      fail("Should fail because log segment instantiation encounters FileNotFound exception");
    } catch (StoreException e) {
      assertEquals("Mismatch in store error code", StoreErrorCodes.Unknown_Error, e.getErrorCode());
      assertEquals(
          "The number of unallocated segments should decrease because exception occurred when freeing the segment",
          initialUnallocatedSegments - 1, mockLog.getRemainingUnallocatedSegments());
    }

    // Test 2: the segment with exception is freed successfully and remainingUnallocatedSegments counter is restored.
    doAnswer((Answer) invocation -> null).when(mockDiskAllocator)
        .free(any(File.class), any(Long.class), anyString(), anyBoolean());
    try {
      buffer.rewind();
      CHANNEL_APPENDER.append(mockLog, buffer);
      fail("Should fail because log segment instantiation encounters FileNotFound exception");
    } catch (StoreException e) {
      assertEquals("Mismatch in store error code", StoreErrorCodes.File_Not_Found, e.getErrorCode());
      // Note that the number should be initialUnallocatedSegments - 1 because in previous test the segment with exception
      // didn't get freed due to exception in diskSpaceAllocator.free().
      assertEquals(
          "The number of unallocated segments should stay unchanged because exception segment is successfully freed",
          initialUnallocatedSegments - 1, mockLog.getRemainingUnallocatedSegments());
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

  /**
   * Returns a {@link LogSegment} instance with name {@code name} and capacity {@code capacityInBytes}.
   * @param name the name of the {@link LogSegment} instance.
   * @param capacityInBytes the capacity of the {@link LogSegment} instance.
   * @param writeHeader {@code true} if headers should be written.
   * @return a {@link LogSegment} instance with name {@code name} and capacity {@code capacityInBytes}.
   * @throws IOException
   */
  private LogSegment getLogSegment(String name, long capacityInBytes, boolean writeHeader)
      throws IOException, StoreException {
    File file = create(LogSegmentNameHelper.nameToFilename(name));
    return new LogSegment(name, file, capacityInBytes, createStoreConfig(capacityInBytes, setFilePermissionEnabled),
        metrics, writeHeader);
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
      throws IOException, StoreException {
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
            expectedSegmentNames.add(LogSegmentNameHelper.generateFirstSegmentName(numSegments > 1));
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
      throws IOException, StoreException {
    if (numToCreate > numFinalSegments) {
      throw new IllegalArgumentException("num segments to create cannot be more than num final segments");
    }
    List<String> segmentNames = new ArrayList<>(numToCreate);
    if (numFinalSegments == 1) {
      String name = LogSegmentNameHelper.generateFirstSegmentName(false);
      File file = create(LogSegmentNameHelper.nameToFilename(name));
      new LogSegment(name, file, segmentCapacity, createStoreConfig(segmentCapacity, setFilePermissionEnabled), metrics,
          false).close(false);
      segmentNames.add(name);
    } else {
      for (int i = 0; i < numToCreate; i++) {
        long pos;
        do {
          pos = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
        } while (positionsGenerated.contains(pos));
        positionsGenerated.add(pos);
        long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        String name = LogSegmentNameHelper.getName(pos, gen);
        File file = create(LogSegmentNameHelper.nameToFilename(name));
        new LogSegment(name, file, segmentCapacity, createStoreConfig(segmentCapacity, setFilePermissionEnabled),
            metrics, true).close(false);
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
      List<String> expectedSegmentNames, int segmentIdxToMarkActive, Appender appender)
      throws IOException, StoreException {
    long numSegments = (logCapacity - 1) / segmentCapacity + 1;
    Log log = new Log(tempDir.getAbsolutePath(), logCapacity, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(segmentCapacity, setFilePermissionEnabled), metrics);
    assertEquals("Total capacity not as expected", logCapacity, log.getCapacityInBytes());
    assertEquals("Segment capacity not as expected", Math.min(logCapacity, segmentCapacity), log.getSegmentCapacity());
    try {
      // only preloaded segments should be in expectedSegmentNames.
      checkLog(log, Math.min(logCapacity, segmentCapacity), expectedSegmentNames);
      String activeSegmentName = expectedSegmentNames.get(segmentIdxToMarkActive);
      log.setActiveSegment(activeSegmentName);
      // all segment files from segmentIdxToMarkActive + 1 to expectedSegmentNames.size() - 1 will be freed.
      List<String> prunedSegmentNames = expectedSegmentNames.subList(0, segmentIdxToMarkActive + 1);
      checkLog(log, Math.min(logCapacity, segmentCapacity), prunedSegmentNames);
      List<String> allSegmentNames = getSegmentNames(numSegments, prunedSegmentNames);
      writeAndCheckLog(log, logCapacity, Math.min(logCapacity, segmentCapacity), numSegments - segmentIdxToMarkActive,
          writeSize, allSegmentNames, segmentIdxToMarkActive, appender);
      // log full - so all segments should be there
      assertEquals("Unexpected number of segments", numSegments, allSegmentNames.size());
      checkLog(log, Math.min(logCapacity, segmentCapacity), allSegmentNames);
      flushCloseAndValidate(log);
      checkLogReload(logCapacity, Math.min(logCapacity, segmentCapacity), allSegmentNames);
    } finally {
      log.close(false);
      cleanDirectory(tempDir);
    }
  }

  /**
   * Checks the log to ensure segment names, capacities and count.
   * @param log the {@link Log} instance to check.
   * @param expectedSegmentCapacity the expected capacity of each segment.
   * @param expectedSegmentNames the expected names of all segments that should have been created in the {@code log}.
   */
  private void checkLog(Log log, long expectedSegmentCapacity, List<String> expectedSegmentNames) {
    LogSegment nextSegment = log.getFirstSegment();
    assertNull("Prev segment should be null", log.getPrevSegment(nextSegment));
    for (String segmentName : expectedSegmentNames) {
      assertNotNull(
          "Next segment is null -  expectedSegmentNames=" + expectedSegmentNames + ". Expected next=" + segmentName,
          nextSegment);
      assertEquals("Next segment is not as expected - expectedSegmentNames=" + expectedSegmentNames, segmentName,
          nextSegment.getName());
      LogSegment segment = log.getSegment(segmentName);
      assertEquals("Segment name is not as expected - expectedSegmentNames=" + expectedSegmentNames, segmentName,
          segment.getName());
      assertEquals("Segment capacity not as expected", expectedSegmentCapacity, segment.getCapacityInBytes());
      assertEquals("Segment returned by getSegment() is incorrect", segment, log.getSegment(segment.getName()));
      nextSegment = log.getNextSegment(segment);
      if (nextSegment != null) {
        assertEquals("Prev segment not as expected - expectedSegmentNames=" + expectedSegmentNames, segment,
            log.getPrevSegment(nextSegment));
      }
    }
    assertNull(
        "Next segment should be null - expectedSegmentNames=" + expectedSegmentNames + ", nextSegment=" + nextSegment,
        nextSegment);
    assertEquals("Unexpected result for isLogSegmented()", log.getCapacityInBytes() != expectedSegmentCapacity,
        log.isLogSegmented());
    long expectedUnallocatedSegments =
        (log.getCapacityInBytes() / expectedSegmentCapacity) - expectedSegmentNames.size();
    assertEquals("DiskSpaceRequirements has wrong number of unallocated segments", expectedUnallocatedSegments,
        log.getRemainingUnallocatedSegments());
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
   * @throws StoreException
   */
  private void writeAndCheckLog(Log log, long logCapacity, long segmentCapacity, long segmentsLeft, long writeSize,
      List<String> segmentNames, int activeSegmentIdx, Appender appender) throws StoreException {
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
    }
    // try one more write that should fail
    try {
      appender.append(log, ByteBuffer.wrap(buf));
      fail("Should have failed because max capacity has been reached");
    } catch (IllegalArgumentException | IllegalStateException e) {
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
      segmentNames.add(LogSegmentNameHelper.generateFirstSegmentName(count > 1));
    }
    for (int i = segmentNames.size(); i < count; i++) {
      String nextSegmentName = LogSegmentNameHelper.getNextPositionName(segmentNames.get(i - 1));
      segmentNames.add(nextSegmentName);
    }
    return segmentNames;
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
      throws IOException, StoreException {
    // modify the segment capacity (mimics modifying the config)
    long[] newConfigs = {originalSegmentCapacity - 1, originalSegmentCapacity + 1};
    for (long newConfig : newConfigs) {
      Log log = new Log(tempDir.getAbsolutePath(), originalLogCapacity, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
          createStoreConfig(newConfig, true), metrics);
      try {
        // the new config should be ignored.
        checkLog(log, originalSegmentCapacity, allSegmentNames);
      } finally {
        log.close(false);
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
    log.close(false);
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
     * @throws StoreException
     */
    void append(Log log, ByteBuffer buffer) throws StoreException;
  }
}
