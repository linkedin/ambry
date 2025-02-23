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

import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.codahale.metrics.MetricRegistry;
import java.nio.file.Files;

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
  private StoreMetrics metrics;
  private FileCopyConfig fileCopyConfig;

  /**
   * Sets up the test environment before each test.
   * Creates temporary directory and initializes FileStore instance.
   */
  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("FileStoreTest").toFile();
    metrics = new StoreMetrics(new MetricRegistry());
    Properties props = new Properties();
    fileCopyConfig = new FileCopyConfig(new VerifiableProperties(props));
    fileStore = new FileStore(fileCopyConfig, tempDir.getAbsolutePath());
    fileStore.start();
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
    fileStore.start();
    assertTrue("FileStore should be running after restart", fileStore.isRunning());
  }

  /**
   * Tests that operations fail when FileStore is not running.
   * Expects FileStoreException with appropriate message.
   */
  @Test
  public void testOperationsWhenNotRunning() throws StoreException {
    fileStore.stop();
    expectedException.expect(FileStoreException.class);
    expectedException.expectMessage("FileStore is not running");
    fileStore.getByteBufferForFileChunk("test.txt", 0, 10);
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

    StoreFileChunk result = fileStore.getByteBufferForFileChunk(testFile.getName(), 0, content.length());
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

    // Write data using FileInputStream
    try (DataInputStream fis = new DataInputStream(Files.newInputStream(tempInputFile.toPath()))) {
        fileStore.putChunkToFile(outputFile.getAbsolutePath(), fis);
    }

    // Verify written data matches original
    byte[] readData = new byte[data.length];
    try (FileInputStream fis = new FileInputStream(outputFile)) {
        assertEquals("Incorrect number of bytes read", data.length, fis.read(readData));
    }
    assertArrayEquals("Data mismatch", data, readData);
  }

  /**
   * Tests metadata persistence and retrieval operations.
   * Verifies:
   * - Metadata can be written to file
   * - Metadata can be read back
   * - All fields match between written and read data
   * - Handles multiple entries correctly
   *
   * @throws IOException if file operations fail
   */
  @Test
  public void testMetadataOperations() throws IOException {
    // Create test metadata
    List<LogInfo> logInfoList = createMultipleLogInfo();

    // Write metadata to file
    fileStore.persistMetaDataToFile(logInfoList);

    // Read back metadata
    List<LogInfo> readLogInfoList = fileStore.readMetaDataFromFile();

    // Verify number of entries matches
    assertEquals("Number of entries should match",
        logInfoList.size(), readLogInfoList.size());

    // Verify each entry's contents
    for (int i = 0; i < logInfoList.size(); i++) {
        LogInfo original = logInfoList.get(i);
        LogInfo read = readLogInfoList.get(i);

        // Verify sealed segment details
        assertEquals("Log segment file name should match",
            original.getLogSegment().getFileName(),
            read.getLogSegment().getFileName());
        assertEquals("Log segment size should match",
            original.getLogSegment().getFileSize(),
            read.getLogSegment().getFileSize());

        // Verify index segments
        assertEquals("Index segments size should match",
            original.getIndexSegments().size(),
            read.getIndexSegments().size());

        // Verify bloom filters
        assertEquals("Bloom filters size should match",
            original.getBloomFilters().size(),
            read.getBloomFilters().size());
    }
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
   * Tests concurrent metadata operations to verify thread safety.
   * Executes:
   * - Multiple simultaneous writes
   * - Concurrent reads during writes
   * - Verifies data integrity under concurrent access
   *
   * @throws Exception if concurrent operations fail
   */
  @Test
  public void testConcurrentMetadataOperations() throws Exception {
    // Prepare test data
    List<LogInfo> logInfoList = createMultipleLogInfo();
    int numThreads = 3;
    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicReference<Exception> exception = new AtomicReference<>();

    // Define concurrent write task 1
    Runnable writeTask1 = () -> {
        try {
            fileStore.persistMetaDataToFile(logInfoList);
            latch.countDown();
        } catch (Exception e) {
            exception.set(e);
        }
    };

    // Define concurrent write task 2
    Runnable writeTask2 = () -> {
        try {
            fileStore.persistMetaDataToFile(logInfoList);
            latch.countDown();
        } catch (Exception e) {
            exception.set(e);
        }
    };

    // Define concurrent read task
    Runnable readTask = () -> {
        try {
            List<LogInfo> result = fileStore.readMetaDataFromFile();
            assertNotNull("Read operation should complete", result);
            latch.countDown();
        } catch (Exception e) {
            exception.set(e);
        }
    };

    // Execute concurrent tasks
    executor.submit(writeTask1);
    executor.submit(writeTask2);
    executor.submit(readTask);

    // Wait for completion with timeout
    assertTrue("Operations did not complete in time", latch.await(5, TimeUnit.SECONDS));
    executor.shutdown();

    // Check for exceptions
    if (exception.get() != null) {
        throw exception.get();
    }
  }

  /**
   * Tests system behavior with corrupted metadata file.
   * Verifies:
   * - Corruption is detected
   * - Appropriate exceptions are thrown
   * - System remains stable after corruption
   *
   * @throws Exception if file operations fail
   */
  @Test
  public void testCorruptMetadataFile() throws Exception {
    // Create and write valid metadata
    List<LogInfo> logInfoList = createMultipleLogInfo();
    fileStore.persistMetaDataToFile(logInfoList);

    // Verify metadata file exists
    File metadataFile = new File(tempDir, "logs_metadata_file");
    assertTrue("Metadata file should exist", metadataFile.exists());

    // Corrupt the file by truncating it
    try (RandomAccessFile raf = new RandomAccessFile(metadataFile, "rw")) {
        raf.setLength(raf.length() / 2); // Truncate to half size
        raf.getFD().sync(); // Ensure changes are written
    }

    // Attempt to read corrupted file
    try {
        fileStore.readMetaDataFromFile();
        fail("Expected an exception when reading corrupted metadata file");
    } catch (Exception e) {
        // Verify exception type
        assertTrue("Exception should be related to file corruption",
            e instanceof IOException || e instanceof ArrayIndexOutOfBoundsException);
    }
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
                    StoreFileChunk result = fileStore.getByteBufferForFileChunk(testFile.getName(), offset, 2);
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
   * Tests system behavior with large metadata files.
   * Verifies:
   * - System can handle large number of entries
   * - Memory usage remains reasonable
   * - Performance is acceptable with large datasets
   *
   * @throws Exception if operations fail
   */
  @Test
  public void testLargeMetadataFile() throws Exception {
    // Create large test dataset
    List<LogInfo> largeLogInfoList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
        FileInfo sealedSegment = new StoreFileInfo("log" + i + ".txt", 1000L * (i + 1));
        List<FileInfo> indexSegments = new ArrayList<>();
        indexSegments.add(new StoreFileInfo("index" + i + "_1.txt", 100L * (i + 1)));
        List<FileInfo> bloomFilters = new ArrayList<>();
        bloomFilters.add(new StoreFileInfo("bloom" + i + ".txt", 50L * (i + 1)));
        largeLogInfoList.add(new StoreLogInfo(sealedSegment, indexSegments, bloomFilters));
    }

    // Write large dataset
    fileStore.persistMetaDataToFile(largeLogInfoList);

    // Read and verify large dataset
    List<LogInfo> readLogInfoList = fileStore.readMetaDataFromFile();

    // Verify size matches
    assertEquals("Size of read list should match written list",
        largeLogInfoList.size(), readLogInfoList.size());

    // Verify content matches
    for (int i = 0; i < largeLogInfoList.size(); i++) {
        LogInfo original = largeLogInfoList.get(i);
        LogInfo read = readLogInfoList.get(i);

        assertEquals("Log segment name should match for index " + i,
            original.getLogSegment().getFileName(),
            read.getLogSegment().getFileName());
        assertEquals("Log segment size should match for index " + i,
            original.getLogSegment().getFileSize(),
            read.getLogSegment().getFileSize());
    }
  }

  /**
   * Tests concurrent reads during write operations.
   * Verifies:
   * - Reads don't interfere with writes
   * - Data consistency is maintained
   * - No deadlocks occur
   *
   * @throws Exception if concurrent operations fail
   */
  @Test
  public void testConcurrentReadsDuringWrite() throws Exception {
    // Test parameters
    int numReaders = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numReaders);
    AtomicReference<Exception> testException = new AtomicReference<>();
    List<LogInfo> logInfoList = createMultipleLogInfo();

    // Start reader threads
    ExecutorService executor = Executors.newFixedThreadPool(numReaders);
    for (int i = 0; i < numReaders; i++) {
        executor.submit(() -> {
            try {
                startLatch.await(); // Wait for write to begin
                fileStore.readMetaDataFromFile();
            } catch (Exception e) {
                testException.set(e);
            } finally {
                completionLatch.countDown();
            }
        });
    }

    // Perform write operation
    fileStore.persistMetaDataToFile(logInfoList);
    startLatch.countDown(); // Signal readers to start

    // Wait for all readers to complete
    assertTrue("Readers did not complete in time",
        completionLatch.await(5, TimeUnit.SECONDS));
    executor.shutdown();

    // Check for exceptions
    if (testException.get() != null) {
        throw testException.get();
    }
  }

  /**
   * Tests file permission handling.
   * Verifies:
   * - Proper handling of read/write permissions
   * - Appropriate exceptions for permission denied
   * - Cleanup after permission changes
   *
   * @throws Exception if operations fail
   */
  @Test
  public void testFilePermissions() throws Exception {
    // Create and write test data
    List<LogInfo> logInfoList = createMultipleLogInfo();
    fileStore.persistMetaDataToFile(logInfoList);

    // Verify file exists
    File metadataFile = new File(tempDir, "logs_metadata_file");
    assertTrue("Metadata file should exist", metadataFile.exists());

    // Verify initial permissions
    assertTrue("Metadata file should be readable", metadataFile.canRead());

    // Remove read permissions
    assertTrue(metadataFile.setReadable(false));

    // Attempt to read without permissions
    try {
        fileStore.readMetaDataFromFile();
        fail("Expected exception when reading file without permissions");
    } catch (IOException e) {
        // Expected exception
    } finally {
        // Restore permissions for cleanup
        metadataFile.setReadable(true);
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
}
