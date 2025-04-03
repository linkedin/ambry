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
import com.github.ambry.store.LogSegmentName;
import com.github.ambry.store.StoreException;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  public StoreFileCopyHandlerIntegTest() throws IOException {}

  @Test
  public void foo() throws IOException {
    File sourceDir = tempDir.resolve("source").toFile();
    File targetDir = tempDir.resolve("target").toFile();
    File sourcePartitionDir = tempDir.resolve(sourceDir + "/" +
        fileCopyInfo.getSourceReplicaId().getPartitionId().getId()).toFile();
    File targetPartitionDir = tempDir.resolve(targetDir + "/" +
        fileCopyInfo.getTargetReplicaId().getPartitionId().getId()).toFile();

    assertTrue(sourcePartitionDir.mkdirs());
    assertTrue(targetPartitionDir.mkdirs());

    LogSegmentName logSegmentName = getRandomLogSegmentName();
    String logSegmentFileName = logSegmentName + "_log";
    String indexFileName1 = logSegmentName + "_index";

    File logSegment = new File(targetPartitionDir, logSegmentFileName);
    File indexFile1 = new File(targetPartitionDir, indexFileName1);

//    createSampleFile(logSegment, 10*1024*1024); // Create a 10MB log segment
    createSampleFile(indexFile1, 1024);   // Create a 1KB index file

    when(fileCopyInfo.getSourceReplicaId().getMountPath()).thenReturn(sourceDir.getAbsolutePath());
    when(fileCopyInfo.getTargetReplicaId().getMountPath()).thenReturn(targetDir.getAbsolutePath());

    StoreLogInfo storeLogInfo = new StoreLogInfo(new StoreFileInfo(logSegmentFileName, logSegment.length()),
        Collections.singletonList(new StoreFileInfo(indexFileName1, indexFile1.length())), new ArrayList<>());
    FileCopyGetMetaDataResponse fileCopyGetMetaDataResponse = new FileCopyGetMetaDataResponse(
        FileCopyGetMetaDataResponse.FILE_COPY_PROTOCOL_METADATA_RESPONSE_VERSION_V_1, 0,
        "clientId", 1, Collections.singletonList(storeLogInfo), ServerErrorCode.NoError);

    StoreFileCopyHandler spyHandler = spy(super.handler);

    doReturn(fileCopyGetMetaDataResponse)
        .when(spyHandler)
        .getFileCopyGetMetaDataResponse(any());

    ByteBuf byteBuf = Unpooled.buffer();
    byte[] indexFile1Bytes = Files.readAllBytes(indexFile1.toPath());
    byteBuf.writeBytes(indexFile1Bytes);
    DataInputStream fileStream = new DataInputStream(new ByteBufferInputStream(byteBuf.nioBuffer()));

    FileCopyGetChunkResponse fileCopyGetChunkResponseForIndexFile = new FileCopyGetChunkResponse(
        FileCopyGetChunkResponse.FILE_COPY_CHUNK_RESPONSE_VERSION_V_1, 0, "clientId",
        ServerErrorCode.NoError, fileCopyInfo.getTargetReplicaId().getPartitionId(),
        indexFileName1, fileStream, 0, indexFile1.length(), false);

    doReturn(fileCopyGetChunkResponseForIndexFile)
        .when(spyHandler)
        .getFileCopyGetChunkResponse(any(), any(), anyBoolean());

//    doAnswer(invocation -> {
//      String filePath = invocation.getArgument(1);
//      StoreFileChunk storeFileChunk = invocation.getArgument(2);
//
//      FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig = new FileCopyBasedReplicationConfig(new VerifiableProperties(new Properties()));
//      FileStore fileStore = new FileStore(fileCopyBasedReplicationConfig, targetDir.getPath());
//      fileStore.start();
//
//      fileStore.writeStoreFileChunkToDisk(filePath, storeFileChunk);
//      return null;
//    }).when(spyHandler).writeStoreFileChunkToDisk(any(), any(), any());

    spyHandler.copy(fileCopyInfo);

    assertTrue(new File(sourcePartitionDir, indexFileName1).exists());
    assertEquals(indexFile1.length(), new File(sourcePartitionDir, indexFileName1).length());
    assertFilesEqual(indexFile1, new File(sourcePartitionDir, indexFileName1));
  }

  private static void assertFilesEqual(File file1, File file2) throws IOException {
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

  @Before
  public void setUp() throws StoreException {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }
}