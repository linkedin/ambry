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

import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.LogSegmentName;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFileInfo;
import com.github.ambry.store.StoreInfo;
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class StoreFileCopyHandlerIntegTest extends StoreFileCopyHandlerTest {
  final Path tempDir = Files.createTempDirectory("StoreFileCopyHandlerIntegTest-" +
      new Random().nextInt(1000)).toFile().toPath();

  private File sourcePartitionDir;
  private File targetPartitionDir;

  public StoreFileCopyHandlerIntegTest() throws IOException {}

  @Before
  public void setUp() throws StoreException {
    super.setUp();

    File sourceDir = tempDir.resolve("source").toFile();
    File targetDir = tempDir.resolve("target").toFile();
    sourcePartitionDir = tempDir.resolve(sourceDir + File.separator +
        fileCopyInfo.getSourceReplicaId().getPartitionId().getId()).toFile();
    targetPartitionDir = tempDir.resolve(targetDir + File.separator +
        fileCopyInfo.getTargetReplicaId().getPartitionId().getId()).toFile();

    assertTrue(sourcePartitionDir.mkdirs());
    assertTrue(targetPartitionDir.mkdirs());

    when(fileCopyInfo.getSourceReplicaId().getMountPath()).thenReturn(sourceDir.getAbsolutePath());
    when(fileCopyInfo.getTargetReplicaId().getMountPath()).thenReturn(targetDir.getAbsolutePath());
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testFileCopiedIndexFilesAreValid() throws IOException {
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
        FileCopyGetMetaDataResponse.FILE_COPY_PROTOCOL_METADATA_RESPONSE_VERSION_V_1, 0,
        "clientId", 1, Collections.singletonList(storeLogInfo), ServerErrorCode.NoError);

    StoreFileCopyHandler spyHandler = spy(super.handler);
    doReturn(fileCopyGetMetaDataResponse)
        .when(spyHandler)
        .getFileCopyGetMetaDataResponse(any());

    final FileCopyGetChunkResponse fileCopyGetChunkResponseForIndexFile =
        getFileCopyGetChunkResponse(indexFile1, indexFileName1);
    final FileCopyGetChunkResponse fileCopyGetChunkResponseForIndexFile2 =
        getFileCopyGetChunkResponse(indexFile2, indexFileName2);

    doReturn(fileCopyGetChunkResponseForIndexFile).
    doReturn(fileCopyGetChunkResponseForIndexFile2)
        .when(spyHandler)
        .getFileCopyGetChunkResponse(any(), any(), anyBoolean());

    spyHandler.copy(fileCopyInfo);

    assertNoSubDirectories(sourcePartitionDir);
    assertCopiedFileIsValid(indexFile1, new File(sourcePartitionDir, indexFileName1));
    assertCopiedFileIsValid(indexFile2, new File(sourcePartitionDir, indexFileName2));
    assertEquals(2, getNumberOfFilesMatchingSuffix(sourcePartitionDir, "_index"));
  }

  private FileCopyGetChunkResponse getFileCopyGetChunkResponse(File indexFile, String indexFileName)
      throws IOException {
    ByteBuf byteBuf = Unpooled.buffer();
    byte[] indexFile1Bytes = Files.readAllBytes(indexFile.toPath());
    byteBuf.writeBytes(indexFile1Bytes);
    DataInputStream fileStream = new DataInputStream(new ByteBufferInputStream(byteBuf.nioBuffer()));

    return new FileCopyGetChunkResponse(
        FileCopyGetChunkResponse.FILE_COPY_CHUNK_RESPONSE_VERSION_V_1, 0, "clientId",
        ServerErrorCode.NoError, fileCopyInfo.getTargetReplicaId().getPartitionId(), indexFileName, fileStream,
        0, indexFile.length(), false);
  }

  private LogSegmentName getRandomLogSegmentName() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 10);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 10);
    return new LogSegmentName(pos, gen);
  }

  private void createSampleFile(File file, int size) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[size]);
    }
  }

  private void assertNoSubDirectories(File dir) {
    assertDirectoryIsValid(dir);

    File[] subDirs = dir.listFiles(File::isDirectory);
    assertNotNull(subDirs);
    assertEquals(0, subDirs.length);
  }

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

  private void assertDirectoryIsValid(File dir) {
    assertNotNull(dir);
    assertTrue(dir.exists());
    assertTrue(dir.canRead());
    assertTrue(dir.isDirectory());
  }

  private void assertCopiedFileIsValid(File expectedFile, File actualFile) throws IOException {
    assertNotNull(actualFile);
    assertTrue(actualFile.exists());
    assertTrue(actualFile.canRead());
    assertTrue(actualFile.isFile());
    assertEquals(expectedFile.length(), actualFile.length());
    assertFilesEqual(expectedFile, actualFile);
  }

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

  private void cleanUpDirectories(List<File> dirs) {
    dirs.forEach(this::deleteAllFiles);
  }

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