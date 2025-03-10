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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.filetransfer.workflow.GetChunkDataWorkflow;
import com.github.ambry.filetransfer.workflow.GetMetadataWorkflow;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.FileStoreException;
import com.github.ambry.store.PartitionFileStore;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFileChunk;
import com.github.ambry.store.StoreFileInfo;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StoreFileCopyHandler implements FileCopyHandler {
  private final ConnectionPool connectionPool;
  private final StoreManager storeManager;
  private boolean isRunning = false;
  private final ClusterMap clusterMap;

  // TODO: From cfg2
  private static final int CHUNK_SIZE = 10 * 1024 * 1024; // 10MB

  private static final Logger logger = LoggerFactory.getLogger(StoreFileCopyHandler.class);

  public StoreFileCopyHandler(ConnectionPool connectionPool, StoreManager storeManager, ClusterMap clusterMap) {
    Objects.requireNonNull(connectionPool, "ConnectionPool cannot be null");
    Objects.requireNonNull(storeManager, "StoreManager cannot be null");
    Objects.requireNonNull(clusterMap, "ClusterMap cannot be null");

    this.connectionPool = connectionPool;
    this.storeManager = storeManager;
    this.clusterMap = clusterMap;
  }

  public void start() throws StoreException {
    isRunning = true;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void stop() {
    isRunning = false;
  }

  public void shutdown() {
    isRunning = false;
  }

  @Override
  public void copy(@Nonnull FileCopyInfo fileCopyInfo) throws IOException {
    Objects.requireNonNull(fileCopyInfo, "fileCopyReplicaInfo param cannot be null");
    validateIfStoreFileCopyHandlerIsRunning();

    PartitionFileStore fileStore = storeManager.getFileStore(fileCopyInfo.getSourceReplicaId().getPartitionId());
    final String partitionFilePath = fileCopyInfo.getSourceReplicaId().getMountPath() + File.separator +
        fileCopyInfo.getSourceReplicaId().getPartitionId().getId();

    final FileCopyGetMetaDataResponse metadataResponse = getFileCopyGetMetaDataResponse(fileCopyInfo);

    metadataResponse.getLogInfos().forEach(logInfo -> {
      logInfo.getIndexSegments().forEach(indexFile -> fetchAndPersistIndexFile(fileCopyInfo, indexFile, partitionFilePath, fileStore));

      FileInfo logFileInfo = new StoreFileInfo(logInfo.getLogSegment().getFileName() + "_log", logInfo.getLogSegment().getFileSize());
      fetchAndPersistLogSegment(fileCopyInfo, logFileInfo, partitionFilePath, fileStore);
    });
  }

  private FileCopyGetMetaDataResponse getFileCopyGetMetaDataResponse(FileCopyInfo fileCopyInfo) {
    FileCopyGetMetaDataResponse metadataResponse = null;
    try {
      metadataResponse = OperationRetryHandler.executeWithRetry(
          () -> new GetMetadataWorkflow(connectionPool, fileCopyInfo).execute(), "GetMetadataOperation");
    } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
      logger.error("Failed to fetch metadata for file {}", fileCopyInfo, e);
      //      throw new FileCopyHandlerException("Failed to fetch metadata for file " + fileCopyInfo, e,
      //          FileCopyHandlerException.FileStoreErrorCode.);
    } catch (Exception e) {
      //      throw new FileCopyHandlerException("Failed to fetch metadata for file " + fileCopyInfo, e,
      //          FileCopyHandlerException.FileStoreErrorCode.UnknownError);
    }
    return metadataResponse;
  }

  private void fetchAndPersistLogSegment(FileCopyInfo fileCopyInfo, FileInfo logFileInfo, String partitionFilePath,
      PartitionFileStore fileStore) {
    int chunksInLogSegment = (int) Math.ceil((double) logFileInfo.getFileSize() / CHUNK_SIZE);
    FileCopyGetChunkResponse chunkResponse = null;

    for (int i = 0; i < chunksInLogSegment; i++) {
      long startOffset = (long) i * CHUNK_SIZE;
      long sizeInBytes = Math.min(CHUNK_SIZE, logFileInfo.getFileSize() - startOffset);
      try {
        fileCopyInfo.setChunkInfo(logFileInfo.getFileName(), startOffset, sizeInBytes, true);
        chunkResponse = OperationRetryHandler.executeWithRetry(
            () -> new GetChunkDataWorkflow(connectionPool, fileCopyInfo, clusterMap).execute(), "GetChunkDataOperation");
      } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    String filePath = partitionFilePath + File.separator + logFileInfo.getFileName();
    try {
      StoreFileChunk chunkToWrite = new StoreFileChunk(chunkResponse.getChunkStream(), chunkResponse.getChunkStream().available());
      fileStore.writeStoreFileChunkToDisk(filePath, chunkToWrite);
    } catch (IOException e) {
      //          throw new FileStoreException("Failed to write chunk to disk", e,
      //              FileStoreException.FileStoreErrorCode.FileStoreIOError);
    } catch (Exception e) {
      //      throw new FileCopyHandlerException("Failed to fetch metadata for file " + fileCopyInfo, e,
      //          FileCopyHandlerException.FileStoreErrorCode.UnknownError);
    }
  }

  private void fetchAndPersistIndexFile(FileCopyInfo fileCopyInfo, FileInfo indexFile,
      String partitionFilePath, PartitionFileStore fileStore) {
    FileCopyGetChunkResponse chunkResponse = null;

    try {
      fileCopyInfo.setChunkInfo(indexFile.getFileName(), 0, indexFile.getFileSize(), false);
      chunkResponse = OperationRetryHandler.executeWithRetry(
            () -> new GetChunkDataWorkflow(connectionPool, fileCopyInfo, clusterMap).execute(), "GetFileOperation");
    } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
      logger.error("Failed to fetch metadata for file {}", fileCopyInfo, e);
      //      throw new FileCopyHandlerException("Failed to fetch metadata for file " + fileCopyInfo, e,
      //          FileCopyHandlerException.FileStoreErrorCode.);
    } catch (Exception e) {
      //      throw new FileCopyHandlerException("Failed to fetch metadata for file " + fileCopyInfo, e,
      //          FileCopyHandlerException.FileStoreErrorCode.UnknownError);
    }
    String filePath = partitionFilePath + File.separator + indexFile.getFileName();
    try {
      StoreFileChunk chunkToWrite = new StoreFileChunk(chunkResponse.getChunkStream(), chunkResponse.getChunkStream().available());
      fileStore.writeStoreFileChunkToDisk(filePath, chunkToWrite);
    } catch (IOException e) {
      //          throw new FileStoreException("Failed to write chunk to disk", e,
      //              FileStoreException.FileStoreErrorCode.FileStoreIOError);
    } catch (Exception e) {
      //      throw new FileCopyHandlerException("Failed to fetch metadata for file " + fileCopyInfo, e,
      //          FileCopyHandlerException.FileStoreErrorCode.UnknownError);
    }
  }

  private void validateIfStoreFileCopyHandlerIsRunning() {
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", FileStoreException.FileStoreErrorCode.FileStoreRunningFailure);
    }
  }
}
