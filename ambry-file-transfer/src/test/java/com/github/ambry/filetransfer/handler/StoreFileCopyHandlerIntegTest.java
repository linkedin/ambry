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
package com.github.ambry.filetransfer.handler;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.DiskSpaceAllocator;
import com.github.ambry.store.DiskSpaceRequirements;
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.FileStore;
import com.github.ambry.store.LogSegmentName;
import com.github.ambry.store.StorageManagerMetrics;
import com.github.ambry.store.StoreFileInfo;
import com.github.ambry.store.StoreLogInfo;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.FileOutputStream;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link StoreFileCopyHandler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class StoreFileCopyHandlerIntegTest extends StoreFileCopyHandlerTest {
  private final Path tempDir;
  private final File reserveFileDir;
  private File sourcePartitionDir;
  private File targetPartitionDir;
  private final FileStore fileStore;
  private final DiskSpaceAllocator diskSpaceAllocator;
  private final StorageManagerMetrics storeManagerMetrics;
  private static final int SEGMENT_CAPACITY = 25 * 1024 * 1024; // 25 MB
  private static final int SEGMENT_COUNT = 2;
  private static final String STORE_ID = "0";
  private static final String STORE_DIR_PREFIX = "reserve_store_";

  public StoreFileCopyHandlerIntegTest() throws IOException {
    tempDir = Files.createTempDirectory("StoreFileCopyHandlerIntegTest-" +
        new Random().nextInt(1000)).toFile().toPath();
    File tempDirFile = tempDir.toFile();
    reserveFileDir = new File(tempDirFile, "reserve-files");
    storeManagerMetrics = new StorageManagerMetrics(new MetricRegistry());
    diskSpaceAllocator = new DiskSpaceAllocator(true, reserveFileDir, 0, storeManagerMetrics);
    fileStore = new FileStore( "", diskSpaceAllocator);
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

  /**
   * Sets up the test environment.
   * Creating source and target directories for file copy.
   * source and target directories further contain partition sub-directory
   */
  @Before
  public void setUp() throws Exception {
    super.setUp();
    diskSpaceAllocator.initializePool(Arrays.asList(new DiskSpaceRequirements(STORE_ID, SEGMENT_CAPACITY, SEGMENT_COUNT, 0)));
    fileStore.start(SEGMENT_CAPACITY);
    when(handler.getStoreManager().getFileStore(any())).thenReturn(fileStore);

    File sourceDir = tempDir.resolve("source").toFile();
    File targetDir = tempDir.resolve("target").toFile();

    when(fileCopyInfo.getSourceReplicaId()).thenReturn(mock(ReplicaId.class));
    when(fileCopyInfo.getSourceReplicaId().getPartitionId()).thenReturn(mock(PartitionId.class));
    when(fileCopyInfo.getSourceReplicaId().getMountPath()).thenReturn(sourceDir.getAbsolutePath());

    sourcePartitionDir = tempDir.resolve(sourceDir.toPath() + File.separator +
        fileCopyInfo.getSourceReplicaId().getPartitionId().getId()).toFile();
    targetPartitionDir = tempDir.resolve(targetDir.toPath() + File.separator +
        fileCopyInfo.getTargetReplicaId().getPartitionId().getId()).toFile();

    assertTrue(sourcePartitionDir.mkdirs());
    assertTrue(targetPartitionDir.mkdirs());
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  /**
   * Tests that the file copy handler can copy files from source to target.
   * It creates index files in the target directory,
   * and then verifies that the files are copied correctly to the source directory.
   */
  @Test
  public void testFileCopiedIndexFilesAreValid() throws Exception {
    // Arrange -
    // 1. Clean up source and target directories
    // 2. Create 2 index files in the target directory. Index file 1 is of size 1KB and index file 2 is of size 50KB.
    // 3. Set up Spy on FCHandler with FileCopyGetMetaDataResponse to return the index files.
    // 4. Set up Spy on FCHandler with FileCopyGetChunkResponse to return the index files.
    cleanUpDirectories(Arrays.asList(sourcePartitionDir, targetPartitionDir));

    LogSegmentName logSegmentName = getRandomLogSegmentName();
    String logSegmentFileName = logSegmentName + "_log";
    String indexFileName1 = logSegmentName + "1_index";
    String indexFileName2 = logSegmentName + "2_index";

    File logSegment = new File(targetPartitionDir, logSegmentFileName);
    File indexFile1 = new File(targetPartitionDir, indexFileName1);
    File indexFile2 = new File(targetPartitionDir, indexFileName2);

    createSampleFile(indexFile1, 1024); // 1 KB
    createSampleFile(indexFile2, 50 * 1024); // 50KB

    List<FileInfo> indexInfos = Arrays.asList(new StoreFileInfo[] {
        new StoreFileInfo(indexFileName1, indexFile1.length()),
        new StoreFileInfo(indexFileName2, indexFile2.length())
    });
    StoreLogInfo storeLogInfo = new StoreLogInfo(new StoreFileInfo(logSegmentFileName, logSegment.length()),
        indexInfos, new ArrayList<>());
    FileCopyGetMetaDataResponse fileCopyGetMetaDataResponse = new FileCopyGetMetaDataResponse(
        FileCopyGetMetaDataResponse.FILE_COPY_PROTOCOL_METADATA_RESPONSE_VERSION_V_1, 0, "clientId",
        1, Collections.singletonList(storeLogInfo), "snapshotId",
        ServerErrorCode.NoError, null);

    StoreFileCopyHandler spyHandler = spy(super.handler);
    doReturn(fileCopyGetMetaDataResponse)
        .when(spyHandler)
        .getFileCopyGetMetaDataResponse(any());

    final FileCopyGetChunkResponse fileCopyGetChunkResponseForIndexFile = getFileCopyGetChunkResponse(indexFile1);
    final FileCopyGetChunkResponse fileCopyGetChunkResponseForIndexFile2 = getFileCopyGetChunkResponse(indexFile2);

    doReturn(fileCopyGetChunkResponseForIndexFile).
    doReturn(fileCopyGetChunkResponseForIndexFile2)
        .when(spyHandler)
        .getFileCopyGetChunkResponse(any(), any(), any(), any(), anyBoolean());

    // Act - Run FCHandler.copy using Spy handler
    // TODO: Temporary directory should have been created before FileCopyHandler.copy() is called.
    File fileCopyTempDirectory = new File(sourcePartitionDir, "fileCopyTempDirectory");
    assertFalse("Temporary directory already exists!", fileCopyTempDirectory.exists());
    assertTrue(fileCopyTempDirectory.mkdirs());
    spyHandler.copy(fileCopyInfo);
    assertTrue(fileCopyTempDirectory.delete());

    // Assert -
    // 1. Check that no sub-directories are created in the source directory
    // 2. Check that the index files are copied correctly to the source directory
    // 3. Check that the number of index files in the source directory is 2
    assertNoSubDirectories(sourcePartitionDir);
    assertCopiedFileIsValid(indexFile1, new File(sourcePartitionDir, indexFileName1));
    assertCopiedFileIsValid(indexFile2, new File(sourcePartitionDir, indexFileName2));
    assertEquals(2, getNumberOfFilesMatchingSuffix(sourcePartitionDir, "_index"));
  }

  /**
   * Tests that the file copy handler can copy log segments from source to target.
   * It creates a log segment file in the target directory,
   * and then verifies that the file is copied correctly to the source directory.
   */
  @Test
  public void testFileCopiedLogSegmentIsValid() throws Exception {
    // Arrange -
    // 1. Clean up source and target directories
    // 2. Create a log segment file in the target directory of size 25MB.
    // 3. Set up Spy on FCHandler with FileCopyGetMetaDataResponse to return the log segment file.
    // 4. Set up Spy on FCHandler with FileCopyGetChunkResponse to return the log segment file.
    cleanUpDirectories(Arrays.asList(sourcePartitionDir, targetPartitionDir));

    LogSegmentName logSegmentName = getRandomLogSegmentName();
    String logSegmentFileName = logSegmentName + "_log";

    File logSegment = new File(targetPartitionDir, logSegmentFileName);
    createSampleFile(logSegment, SEGMENT_CAPACITY); // 25 MB

    StoreLogInfo storeLogInfo = new StoreLogInfo(new StoreFileInfo(logSegmentName.toString(), logSegment.length()),
        new ArrayList<>(), new ArrayList<>());
    FileCopyGetMetaDataResponse fileCopyGetMetaDataResponse = new FileCopyGetMetaDataResponse(
        FileCopyGetMetaDataResponse.FILE_COPY_PROTOCOL_METADATA_RESPONSE_VERSION_V_1, 0, "clientId",
        1, Collections.singletonList(storeLogInfo), "snapshotId", ServerErrorCode.NoError, null);

    StoreFileCopyHandler spyHandler = spy(super.handler);
    doReturn(fileCopyGetMetaDataResponse)
        .when(spyHandler)
        .getFileCopyGetMetaDataResponse(any());

    List<FileCopyGetChunkResponse> chunkResponses = new ArrayList<>();
    int chunksInLogSegment = (int) Math.ceil((double) logSegment.length() / fileCopyBasedReplicationConfig.getFileCopyHandlerChunkSize);

    for (int i = 0; i < chunksInLogSegment; i++) {
      int startOffset = i * fileCopyBasedReplicationConfig.getFileCopyHandlerChunkSize;
      int sizeInBytes = (int)Math.min(fileCopyBasedReplicationConfig.getFileCopyHandlerChunkSize, logSegment.length() - startOffset);

      chunkResponses.add(getFileCopyGetChunkResponse(logSegment, startOffset, sizeInBytes));
    }

    // Expecting 3 chunks for the log segment (log segment size = 25MB, chunk size = 10MB)
    doReturn(chunkResponses.get(0)).
    doReturn(chunkResponses.get(1)).
    doReturn(chunkResponses.get(2))
        .when(spyHandler)
        .getFileCopyGetChunkResponse(any(), any(), any(), any(), anyBoolean());

    // Act - Run FCHandler.copy using Spy handler
    // TODO: Temporary directory should have been created before FileCopyHandler.copy() is called.
    File fileCopyTempDirectory = new File(sourcePartitionDir, "fileCopyTempDirectory");
    assertFalse("Temporary directory already exists!", fileCopyTempDirectory.exists());
    assertTrue(fileCopyTempDirectory.mkdirs());
    assertEquals(SEGMENT_COUNT, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
    spyHandler.copy(fileCopyInfo);
    assertTrue(fileCopyTempDirectory.delete());

    // Assert -
    // 1. Check that no sub-directories are created in the source directory
    // 2. Check that the log segment file is copied correctly to the source directory
    // 3. Check that the number of log segment files in the source directory is 1
    assertNoSubDirectories(sourcePartitionDir);
    assertCopiedFileIsValid(logSegment, new File(sourcePartitionDir, logSegmentFileName));
    assertEquals(1, getNumberOfFilesMatchingSuffix(sourcePartitionDir, "_log"));
    assertEquals(SEGMENT_COUNT-1, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));

    // cleanup the file to give it back to DiskSpaceAllocator
    fileStore.cleanFile(logSegment.getPath(), STORE_ID);
    assertEquals(SEGMENT_COUNT, getReservePoolRemainingSegmentCount(STORE_ID, SEGMENT_CAPACITY));
  }

  /**
   * For the given log segment file, offset and chunkSize, this method creates a {@link FileCopyGetChunkResponse} object
   * with the chunk data read from the file.
   */
  private FileCopyGetChunkResponse getFileCopyGetChunkResponse(File logSegmentFile, int offset, int chunkSize) {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(logSegmentFile, "r")) {
      randomAccessFile.seek(offset);
      ByteBuffer buf = ByteBuffer.allocate(chunkSize);

      while (buf.hasRemaining()) {
        int bytesRead = randomAccessFile.getChannel().read(buf);
        if (bytesRead == -1) {
          break; // EOF reached
        }
      }
      // Prepare buffer for reading
      buf.flip();

      return new FileCopyGetChunkResponse(
          FileCopyGetChunkResponse.FILE_COPY_CHUNK_RESPONSE_VERSION_V_1, 0, "clientId",
          ServerErrorCode.NoError, fileCopyInfo.getTargetReplicaId().getPartitionId(), logSegmentFile.getName(),
          new DataInputStream(new ByteBufferInputStream(buf)), offset, chunkSize, false);
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  /**
   * For the given index file, this method creates a {@link FileCopyGetChunkResponse} object
   * with the chunk data read from the file.
   */
  private FileCopyGetChunkResponse getFileCopyGetChunkResponse(File indexFile)
      throws IOException {
    ByteBuf byteBuf = Unpooled.buffer();
    byte[] indexFile1Bytes = Files.readAllBytes(indexFile.toPath());
    byteBuf.writeBytes(indexFile1Bytes);
    DataInputStream fileStream = new DataInputStream(new ByteBufferInputStream(byteBuf.nioBuffer()));

    return new FileCopyGetChunkResponse(
        FileCopyGetChunkResponse.FILE_COPY_CHUNK_RESPONSE_VERSION_V_1, 0, "clientId",
        ServerErrorCode.NoError, fileCopyInfo.getTargetReplicaId().getPartitionId(), indexFile.getName(), fileStream,
        0, indexFile.length(), false);
  }

  /**
   * Generates a random log segment name.
   * @return a random log segment name.
   */
  private LogSegmentName getRandomLogSegmentName() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 10);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 10);
    return new LogSegmentName(pos, gen);
  }

  /**
   * Creates a sample file with the given size.
   * @param file the file to create.
   * @param size the size of the file in bytes.
   * @throws IOException if an I/O error occurs.
   */
  private void createSampleFile(File file, int size) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[size]);
    }
  }

  /**
   * Asserts that the given directory does not contain any sub-directories.
   * @param dir the directory to check.
   */
  private void assertNoSubDirectories(File dir) {
    assertDirectoryIsValid(dir);

    File[] subDirs = dir.listFiles(File::isDirectory);
    assertNotNull(subDirs);
    assertEquals(0, subDirs.length);
  }

  /**
   * Returns the number of files in the given directory matching the given suffix.
   * @param dir the directory to check.
   * @param suffix the suffix to match.
   * @return the number of files matching the suffix.
   */
  private int getNumberOfFilesMatchingSuffix(File dir, String suffix) {
    assertDirectoryIsValid(dir);

    File[] files = dir.listFiles();
    assertNotNull(files);
    int count = 0;
    for (File file : files) {
      if (file.getName().endsWith(suffix)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Asserts that the given directory is valid.
   * Checks -
   * 1. Directory is not null
   * 2. Directory exists
   * 3. Directory is readable
   * 4. Directory is a directory
   * @param dir the directory to check.
   */
  private void assertDirectoryIsValid(File dir) {
    assertNotNull(dir);
    assertTrue(dir.exists());
    assertTrue(dir.canRead());
    assertTrue(dir.isDirectory());
  }

  /**
   * Asserts that the copied file is valid.
   * Checks -
   * 1. File is not null
   * 2. File exists
   * 3. File is readable
   * 4. File is a file
   * 5. File length matches the expected length
   * 6. File contents match the expected file contents
   * @param expectedFile the expected file.
   * @param actualFile the actual file.
   */
  private void assertCopiedFileIsValid(File expectedFile, File actualFile) throws IOException {
    assertNotNull(actualFile);
    assertTrue(actualFile.exists());
    assertTrue(actualFile.canRead());
    assertTrue(actualFile.isFile());
    assertEquals(expectedFile.length(), actualFile.length());
    assertFilesEqual(expectedFile, actualFile);
  }

  /**
   * Asserts that the contents of two files are equal.
   * @param file1 the first file.
   * @param file2 the second file.
   * @throws IOException if an I/O error occurs.
   */
  private void assertFilesEqual(File file1, File file2) throws IOException {
    try (InputStream is1 = Files.newInputStream(file1.toPath());
        InputStream is2 = Files.newInputStream(file2.toPath())) {
      byte[] buffer1 = new byte[1024];
      byte[] buffer2 = new byte[1024];
      int numRead1;
      int numRead2;
      while ((numRead1 = is1.read(buffer1)) != -1) {
        numRead2 = is2.read(buffer2);
        Assert.assertEquals(numRead1, numRead2);
        assertArrayEquals(buffer1, buffer2);
      }
      Assert.assertEquals(-1, is2.read(buffer2));
    }
  }

  /**
   * Cleans up the given directories by deleting all files in them.
   * @param dirs the directories to clean up.
   */
  private void cleanUpDirectories(List<File> dirs) {
    dirs.forEach(this::deleteAllFiles);
  }

  /**
   * Deletes all files in the given directory.
   * @param dir the directory to delete files from.
   */
  private void deleteAllFiles(File dir) {
    assertDirectoryIsValid(dir);

    try {
      Files.walk(dir.toPath())
          .filter(path -> !path.equals(dir.toPath()))
          .forEach(path -> {
        try {
          Files.delete(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
