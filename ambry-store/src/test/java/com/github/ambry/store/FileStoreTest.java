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

import com.github.ambry.clustermap.FileStoreException;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.config.VerifiableProperties;
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
import java.io.EOFException;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.StoreMetrics;
import java.nio.file.Files;
import java.io.FileNotFoundException;

import static org.junit.Assert.*;

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

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("FileStoreTest").toFile();
    metrics = new StoreMetrics(new MetricRegistry());
    Properties props = new Properties();
    fileCopyConfig = new FileCopyConfig(new VerifiableProperties(props));
    fileStore = new FileStore(fileCopyConfig);
    fileStore.start();
  }

  @Test
  public void testStartStopState() throws StoreException {
    assertTrue("FileStore should be running after start", fileStore.isRunning());
    fileStore.stop();
    assertFalse("FileStore should not be running after stop", fileStore.isRunning());
    fileStore.start();
    assertTrue("FileStore should be running after restart", fileStore.isRunning());
  }

  @Test
  public void testOperationsWhenNotRunning() throws IOException {
    fileStore.stop();
    expectedException.expect(FileStoreException.class);
    expectedException.expectMessage("FileStore is not running");
    fileStore.getStreamForFileRead(tempDir.getAbsolutePath(), "test.txt", 0, 10);
  }

  @Test
  public void testGetStreamForFileRead() throws IOException {
    // Create test file
    File testFile = new File(tempDir, "test.txt");
    String content = "test data content";
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(content.getBytes());
    fos.close();

    ByteBuffer result = fileStore.getStreamForFileRead(
        tempDir.getAbsolutePath(),
        testFile.getName(),
        0,
        content.length()
    );

    byte[] readContent = new byte[result.remaining()];
    result.get(readContent);
    assertEquals("Content should match", content, new String(readContent));
  }

  @Test
  public void testPutChunkToFile() throws Exception {
    String chunkFileName = "output-chunk.txt";
    File outputFile = new File(tempDir, chunkFileName);
    byte[] data = "test data".getBytes();

    // Create a temporary file with test data
    File tempInputFile = new File(tempDir, "input-chunk.txt");
    try (FileOutputStream fos = new FileOutputStream(tempInputFile)) {
        fos.write(data);
    }

    // Ensure parent directory exists
    assertTrue("Failed to create test directory", outputFile.getParentFile().exists() || outputFile.getParentFile().mkdirs());

    // Write data using FileInputStream
    try (FileInputStream fis = new FileInputStream(tempInputFile)) {
        fileStore.putChunkToFile(outputFile.getAbsolutePath(), fis);
    }

    // Verify written data
    byte[] readData = new byte[data.length];
    try (FileInputStream fis = new FileInputStream(outputFile)) {
        assertEquals("Incorrect number of bytes read", data.length, fis.read(readData));
    }
    assertArrayEquals("Data mismatch", data, readData);
  }

  @Test
  public void testMetadataOperations() throws IOException {
    List<LogInfo> logInfoList = createMultipleLogInfo();
    fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), logInfoList);

    List<LogInfo> readLogInfoList = fileStore.readMetaDataFromFile(tempDir.getAbsolutePath());
    assertEquals("Number of entries should match", logInfoList.size(), readLogInfoList.size());

    for (int i = 0; i < logInfoList.size(); i++) {
      LogInfo original = logInfoList.get(i);
      LogInfo read = readLogInfoList.get(i);

      assertEquals("Log segment file name should match",
          original.getSealedSegment().getFileName(),
          read.getSealedSegment().getFileName());
      assertEquals("Log segment size should match",
          original.getSealedSegment().getFileSize(),
          read.getSealedSegment().getFileSize());

      assertEquals("Index segments size should match",
          original.getIndexSegments().size(),
          read.getIndexSegments().size());
      assertEquals("Bloom filters size should match",
          original.getBloomFilters().size(),
          read.getBloomFilters().size());
    }
  }

  private List<LogInfo> createMultipleLogInfo() {
    List<LogInfo> logInfoList = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      FileInfo sealedSegment = new FileInfo("log" + i + ".txt", 1000L * (i + 1));

      List<FileInfo> indexSegments = new ArrayList<>();
      indexSegments.add(new FileInfo("index" + i + "_1.txt", 100L * (i + 1)));
      indexSegments.add(new FileInfo("index" + i + "_2.txt", 200L * (i + 1)));

      List<FileInfo> bloomFilters = new ArrayList<>();
      bloomFilters.add(new FileInfo("bloom" + i + ".txt", 50L * (i + 1)));

      logInfoList.add(new LogInfo(sealedSegment, indexSegments, bloomFilters));
    }

    return logInfoList;
  }

  @Test
  public void testConcurrentMetadataOperations() throws Exception {
    List<LogInfo> logInfoList = createMultipleLogInfo();
    int numThreads = 3;
    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicReference<Exception> exception = new AtomicReference<>();

    // Create tasks for concurrent operations
    Runnable writeTask1 = () -> {
        try {
            fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), logInfoList);
            latch.countDown();
        } catch (Exception e) {
            exception.set(e);
        }
    };

    Runnable writeTask2 = () -> {
        try {
            fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), logInfoList);
            latch.countDown();
        } catch (Exception e) {
            exception.set(e);
        }
    };

    Runnable readTask = () -> {
        try {
            List<LogInfo> result = fileStore.readMetaDataFromFile(tempDir.getAbsolutePath());
            assertNotNull("Read operation should complete", result);
            latch.countDown();
        } catch (Exception e) {
            exception.set(e);
        }
    };

    // Submit tasks
    executor.submit(writeTask1);
    executor.submit(writeTask2);
    executor.submit(readTask);

    // Wait for completion with timeout
    assertTrue("Operations did not complete in time", latch.await(5, TimeUnit.SECONDS));
    executor.shutdown();
    if (exception.get() != null) {
        throw exception.get();
    }
  }

  @Test
  public void testCorruptMetadataFile() throws Exception {
    // First create and write valid metadata
    List<LogInfo> logInfoList = createMultipleLogInfo();
    fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), logInfoList);

    // Get the metadata file
    File metadataFile = new File(tempDir, "sealed_logs_metadata_file");
    assertTrue("Metadata file should exist", metadataFile.exists());

    // Corrupt the file by truncating it
    try (RandomAccessFile raf = new RandomAccessFile(metadataFile, "rw")) {
        raf.setLength(raf.length() / 2); // Truncate file to half its size
        raf.getFD().sync(); // Ensure writes are flushed
    }

    try {
        fileStore.readMetaDataFromFile(tempDir.getAbsolutePath());
        fail("Expected an exception when reading corrupted metadata file");
    } catch (Exception e) {
        // Test passes if any exception is thrown
        assertTrue("Exception should be related to file corruption",
            e instanceof IOException || e instanceof ArrayIndexOutOfBoundsException);
    }
  }

  @Test
  public void testConcurrentFileReads() throws Exception {
    // Create test file
    File testFile = new File(tempDir, "concurrent-test.txt");
    String content = "test data for concurrent reads";
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(content.getBytes());
    fos.close();

    int numThreads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch latch = new CountDownLatch(numThreads);
    List<Future<ByteBuffer>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final int offset = i;
      futures.add(executor.submit(new Callable<ByteBuffer>() {
        @Override
        public ByteBuffer call() throws Exception {
          try {
            ByteBuffer result = fileStore.getStreamForFileRead(
                tempDir.getAbsolutePath(),
                testFile.getName(),
                offset,
                2
            );
            latch.countDown();
            return result;
          } catch (Exception e) {
            throw e;
          }
        }
      }));
    }

    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

    for (Future<ByteBuffer> future : futures) {
      ByteBuffer result = future.get();
      assertNotNull("Result should not be null", result);
      assertTrue("Buffer should have data", result.remaining() > 0);
    }
  }

  @Test
  public void testLargeMetadataFile() throws Exception {
    // Create a large list of log infos
    List<LogInfo> largeLogInfoList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
        FileInfo sealedSegment = new FileInfo("log" + i + ".txt", 1000L * (i + 1));

        List<FileInfo> indexSegments = new ArrayList<>();
        indexSegments.add(new FileInfo("index" + i + "_1.txt", 100L * (i + 1)));

        List<FileInfo> bloomFilters = new ArrayList<>();
        bloomFilters.add(new FileInfo("bloom" + i + ".txt", 50L * (i + 1)));

        largeLogInfoList.add(new LogInfo(sealedSegment, indexSegments, bloomFilters));
    }

    fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), largeLogInfoList);
    List<LogInfo> readLogInfoList = fileStore.readMetaDataFromFile(tempDir.getAbsolutePath());

    assertEquals("Size of read list should match written list",
        largeLogInfoList.size(), readLogInfoList.size());

    for (int i = 0; i < largeLogInfoList.size(); i++) {
        LogInfo original = largeLogInfoList.get(i);
        LogInfo read = readLogInfoList.get(i);

        assertEquals("Log segment name should match for index " + i,
            original.getSealedSegment().getFileName(),
            read.getSealedSegment().getFileName());
        assertEquals("Log segment size should match for index " + i,
            original.getSealedSegment().getFileSize(),
            read.getSealedSegment().getFileSize());
    }
  }

  @Test
  public void testConcurrentReadsDuringWrite() throws Exception {
    int numReaders = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numReaders);
    AtomicReference<Exception> testException = new AtomicReference<>();
    List<LogInfo> logInfoList = createMultipleLogInfo();

    // Start readers
    ExecutorService executor = Executors.newFixedThreadPool(numReaders);
    for (int i = 0; i < numReaders; i++) {
        executor.submit(() -> {
            try {
                startLatch.await();
                fileStore.readMetaDataFromFile(tempDir.getAbsolutePath());
            } catch (Exception e) {
                testException.set(e);
            } finally {
                completionLatch.countDown();
            }
        });
    }

    // Write metadata while readers are waiting
    fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), logInfoList);
    startLatch.countDown();

    // Wait for all readers to complete
    assertTrue("Readers did not complete in time",
        completionLatch.await(5, TimeUnit.SECONDS));
    executor.shutdown();

    if (testException.get() != null) {
        throw testException.get();
    }
  }

  @Test
  public void testFilePermissions() throws Exception {
    List<LogInfo> logInfoList = createMultipleLogInfo();
    fileStore.persistMetaDataToFile(tempDir.getAbsolutePath(), logInfoList);

    File metadataFile = new File(tempDir, "sealed_logs_metadata_file");
    assertTrue("Metadata file should exist", metadataFile.exists());

    // Test read permissions
    assertTrue("Metadata file should be readable", metadataFile.canRead());

    // Remove read permissions
    assertTrue(metadataFile.setReadable(false));

    try {
        fileStore.readMetaDataFromFile(tempDir.getAbsolutePath());
        fail("Expected exception when reading file without permissions");
    } catch (IOException e) {
        // Expected
    } finally {
        // Restore permissions for cleanup
        metadataFile.setReadable(true);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (fileStore != null) {
      fileStore.stop();
    }
    // TemporaryFolder rule will handle cleanup automatically
  }
}