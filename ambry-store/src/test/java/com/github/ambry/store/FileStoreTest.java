/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import java.nio.file.Files;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link FileStore} class.
 * Tests file operations, metadata handling, concurrent access, and error conditions.
 */
@RunWith(MockitoJUnitRunner.class)
public class FileStoreTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private FileStore fileStore;
  private File tempDir;
  private File reserveFileDir;
  private FileCopyBasedReplicationConfig _fileCopyBasedReplicationConfig;
  private DiskSpaceAllocator diskSpaceAllocator;
  private static final int SEGMENT_CAPACITY = 512;
  private static final int SEGMENT_COUNT = 2;
  private static final String STORE_ID = "0";
  private static final String STORE_DIR_PREFIX = "reserve_store_";

  /**
   * Sets up the test environment before each test.
   * Creates temporary directory and initializes FileStore instance.
   */
  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("FileStoreTest").toFile();
    Properties props = new Properties();
    _fileCopyBasedReplicationConfig = new FileCopyBasedReplicationConfig(new VerifiableProperties(props));
    reserveFileDir = new File(tempDir, "reserve-files");
    StorageManagerMetrics storeManagerMetrics = new StorageManagerMetrics(new MetricRegistry());
    diskSpaceAllocator = new DiskSpaceAllocator(true, reserveFileDir, 0, storeManagerMetrics);
    diskSpaceAllocator.initializePool(
        Arrays.asList(new DiskSpaceRequirements(STORE_ID, SEGMENT_CAPACITY, SEGMENT_COUNT, 0)));
    fileStore = new FileStore(tempDir.getAbsolutePath(), diskSpaceAllocator);
    fileStore.start(SEGMENT_CAPACITY);
  }

  /**
   * Tests start/stop state transitions of FileStore.
   * Verifies running state is correctly tracked.
   */
  @Test
  public void testStartStopState() throws StoreException {
    assertTrue("FileStore should be running after start", fileStore.isRunning());
    fileStore.stop();
    assertFalse("FileStore should not be running after stop", fileStore.isRunning());
    fileStore.start(SEGMENT_CAPACITY);
    assertTrue("FileStore should be running after restart", fileStore.isRunning());
  }

  /**
   * Tests that operations fail when FileStore is not running.
   * Expects FileStoreException with appropriate message.
   */
  @Test
  public void testOperationsWhenNotRunning() throws StoreException, IOException {
    fileStore.stop();
    expectedException.expect(FileStoreException.class);
    expectedException.expectMessage("FileStore is not running");
    fileStore.readStoreFileChunkFromDisk("test.txt", 0, 10, false);
  }

  /**
   * Tests reading data from a file using getStreamForFileRead.
   * Verifies content is correctly read from specified offset.
   */
  @Test
  public void testGetStreamForFileRead() throws StoreException, IOException {
    // Create test file
    File testFile = new File(tempDir, "test.txt");
    String content = "test data content";
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(content.getBytes());
    fos.close();

    StoreFileChunk result = fileStore.readStoreFileChunkFromDisk(testFile.getName(), 0, content.length(), false);
    assertNotNull("Result should not be null", result);

    ByteBuffer buf = result.toBuffer();
    byte[] readContent = new byte[buf.remaining()];
    buf.get(readContent);
    assertEquals("Content should match", content, new String(readContent));
  }

  /**
   * Tests writing a chunk of data to a file.
   * Verifies:
   * - File is created successfully
   * - Data is written correctly
   * - Content can be read back and matches original
   *
   * @throws Exception if any file operations fail
   */
  @Test
  public void testPutChunkToFile() throws Exception {
    // Test file names
    String chunkFileName = "output-chunk.txt";
    File outputFile = new File(tempDir, chunkFileName);
    byte[] data = "test data".getBytes();

    // Create temporary input file with test data
    File tempInputFile = new File(tempDir, "input-chunk.txt");
    try (FileOutputStream fos = new FileOutputStream(tempInputFile)) {
        fos.write(data);
    }

    // Ensure directory structure exists
    assertTrue("Failed to create test directory",
        outputFile.getParentFile().exists() || outputFile.getParentFile().mkdirs());

    // Write data using DataInputStream
    try (DataInputStream fis = new DataInputStream(Files.newInputStream(tempInputFile.toPath()))) {
        fileStore.writeStoreFileChunkToDisk(outputFile.getAbsolutePath(), new StoreFileChunk(fis, data.length));
    }

    // Verify written data matches original
    byte[] readData = new byte[data.length];
    try (FileInputStream fis = new FileInputStream(outputFile)) {
        assertEquals("Incorrect number of bytes read", data.length, fis.read(readData));
    }
    assertArrayEquals("Data mismatch", data, readData);
  }

  /**
   * Tests moving regular files from a source directory to destination.
   * @throws Exception if any file operatio fail
   */
  @Test
  public void testMoveRegularFiles() throws StoreException, Exception {
    File srcDir = new File(tempDir, "src");
    File destDir = new File(tempDir, "dest");

    // Ensure directories exists.
    srcDir.mkdirs();
    destDir.mkdirs();

    // Create 10 test files in srcDir
    for (int i = 0; i < 10; i++) {
      File file = new File(srcDir, "file_to_move_" + i);
      try (FileOutputStream fos = new FileOutputStream(file)) {
        fos.write(("test data " + i).getBytes());
      }
    }

    // Invoke files move helper
    fileStore.moveAllRegularFiles(srcDir.getAbsolutePath(), destDir.getAbsolutePath());

    // Verify that all files were moved successfully
    for (int i = 0; i < 10; i++) {
      File srcFile = new File(srcDir, "file_to_move_" + i);
      File destFile = new File(destDir, "file_to_move_" + i);

      assertFalse("File should not exist in source dir: " + srcFile.getName(), srcFile.exists());
      assertTrue("File should exist in dest dir: " + destFile.getName(), destFile.exists());

      String content = new String(Files.readAllBytes(destFile.toPath()));
      assertEquals("test data " + i, content);
    }

    // Negative test: If file already exists with same name in destination, then should throw an exception.
    File commonFileSrc = new File(srcDir, "common_file");
    File commonFileDest = new File(destDir, "common_file");
    try (FileOutputStream fos = new FileOutputStream(commonFileSrc)) {
      fos.write("common data".getBytes());
    }
    try (FileOutputStream fos = new FileOutputStream(commonFileDest)) {
      fos.write("common data".getBytes());
    }

    // Invoke files move helper
    expectedException.expect(FileStoreException.class);
    fileStore.moveAllRegularFiles(srcDir.getAbsolutePath(), destDir.getAbsolutePath());
    expectedException.expectMessage("Error while moving files");
  }

  /**
   * Helper method to create test LogInfo objects.
   * Generates multiple LogInfo entries with unique names and sizes.
   * @return List of LogInfo objects for testing
   */
  private List<LogInfo> createMultipleLogInfo() {
    List<LogInfo> logInfoList = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      FileInfo sealedSegment = new StoreFileInfo("log" + i + ".txt", 1000L * (i + 1));

      List<FileInfo> indexSegments = new ArrayList<>();
      indexSegments.add(new StoreFileInfo("index" + i + "_1.txt", 100L * (i + 1)));
      indexSegments.add(new StoreFileInfo("index" + i + "_2.txt", 200L * (i + 1)));

      List<FileInfo> bloomFilters = new ArrayList<>();
      bloomFilters.add(new StoreFileInfo("bloom" + i + ".txt", 50L * (i + 1)));

      logInfoList.add(new StoreLogInfo(sealedSegment, indexSegments, bloomFilters));
    }

    return logInfoList;
  }

  /**
   * Tests concurrent file read operations.
   * Verifies:
   * - Multiple threads can read simultaneously
   * - Each thread gets correct data
   * - System handles concurrent access efficiently
   *
   * @throws Exception if concurrent operations fail
   */
  @Test
  public void testConcurrentFileReads() throws Exception {
    // Create test file with known content
    File testFile = new File(tempDir, "concurrent-test.txt");
    String content = "test data for concurrent reads";
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(content.getBytes());
    fos.close();

    // Setup concurrent read test
    int numThreads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch latch = new CountDownLatch(numThreads);
    List<Future<ByteBuffer>> futures = new ArrayList<>();

    // Submit concurrent read tasks
    for (int i = 0; i < numThreads; i++) {
        final int offset = i;
        futures.add(executor.submit(new Callable<ByteBuffer>() {
            @Override
            public ByteBuffer call() throws Exception {
                try {
                    StoreFileChunk result = fileStore.readStoreFileChunkFromDisk(testFile.getName(), offset, 2, false);
                    latch.countDown();
                    return result.toBuffer();
                } catch (Exception e) {
                    throw e;
                }
            }
        }));
    }

    // Wait for all reads to complete
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

    // Verify results
    for (Future<ByteBuffer> future : futures) {
        ByteBuffer result = future.get();
        assertNotNull("Result should not be null", result);
        assertTrue("Buffer should have data", result.remaining() > 0);
    }
  }

  /**
   * Cleans up test resources after each test.
   * Stops FileStore and allows TemporaryFolder to clean up files.
   */
  @After
  public void tearDown() throws Exception {
    if (fileStore != null) {
      fileStore.stop();
    }
    // TemporaryFolder rule handles cleanup automatically
  }

  /**
   * Tests the successful allocation of a file using the allocateFile method.
   * Verifies that:
   * - The file is created successfully.
   * - The reserve pool count is updated correctly.
   */
  @Test
  public void testAllocateFileSuccess() throws Exception {
    String filePath = tempDir.getAbsolutePath() + File.separator + "testFile.txt";
    File file = new File(filePath);
    // Validate if pool has enough files
    assertFalse(file.exists());
    assertEquals(SEGMENT_COUNT, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
    // Call allocateFile
    fileStore.allocateFile(filePath, STORE_ID);
    // Validate if file is created
    assertTrue(file.exists());
    assertEquals(SEGMENT_COUNT-1, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
  }

  /**
   * Tests the behavior of the allocateFile method when the file already exists.
   * Expects a FileStoreException to be thrown.
   */
  @Test
  public void testAllocateFileAlreadyExists() throws Exception {
    String filePath = tempDir.getAbsolutePath() + File.separator + "testFile.txt";
    File file = new File(filePath);
    if (file.createNewFile()) {
      assertException(FileStoreException.class, () -> fileStore.allocateFile(filePath, STORE_ID), null);
    } else {
      throw new RuntimeException("Failed to create test file");
    }
  }

  /**
   * Tests the successful cleanup of a file using the cleanFile method.
   * Verifies that:
   * - The file is deleted successfully.
   * - The reserve pool count is updated correctly.
   */
  @Test
  public void testCleanFileSuccess() throws Exception {
    String filePath = tempDir.getAbsolutePath() + File.separator + "testFile.txt";
    File file = new File(filePath);
    // Validate if pool has enough files
    assertFalse(file.exists());
    assertEquals(SEGMENT_COUNT, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
    // Call allocateFile
    fileStore.allocateFile(filePath, STORE_ID);
    // Validate if file is created
    assertTrue(file.exists());
    assertEquals(SEGMENT_COUNT-1, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
    // Clean File
    fileStore.cleanFile(filePath, STORE_ID);
    // Validate if file is deleted
    assertFalse(file.exists());
    assertEquals(SEGMENT_COUNT, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
  }

  /**
   * Tests the behavior of the cleanFile method when the file does not exist.
   * Expects a FileStoreException to be thrown.
   */
  @Test
  public void testCleanFileWhichDoesntExist() throws Exception {
    String filePath = tempDir.getAbsolutePath() + File.separator + "testFile.txt";
    assertException(FileStoreException.class, () -> fileStore.cleanFile(filePath, STORE_ID), null);
  }

  /**
   * Returns the number of segments remaining in the reserve pool for the given store ID and segment size.
   * @param storeId the ID of the store.
   * @param segmentSize the size of the segment.
   * @return the number of segments remaining in the reserve pool.
   */
  int getReservePoolRemainingSegmentCount(String storeId, int segmentSize) {
    File storeReserveDir = new File(reserveFileDir, STORE_DIR_PREFIX + storeId);
    File fileSizeDir = new File(storeReserveDir, DiskSpaceAllocator.generateFileSizeDirName(segmentSize));
    String[] filenameList = fileSizeDir.list();
    return filenameList == null ? 0 : filenameList.length;
  }
}
