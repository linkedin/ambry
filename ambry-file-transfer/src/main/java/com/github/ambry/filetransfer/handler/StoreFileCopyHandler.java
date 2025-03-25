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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.filetransfer.FileChunkInfo;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.filetransfer.utils.OperationRetryHandler;
import com.github.ambry.filetransfer.workflow.GetChunkDataWorkflow;
import com.github.ambry.filetransfer.workflow.GetMetadataWorkflow;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileInfo;
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


/**
 * A class to handle copying of files from one store to another.
 */
public class StoreFileCopyHandler implements FileCopyHandler {
  /**
   * The connection pool to use for making requests.
   */
  private final ConnectionPool connectionPool;

  /**
   * The store manager to use for getting the {@link PartitionFileStore}.
   */
  private final StoreManager storeManager;

  /**
   * The configuration for the file copy handler.
   */
  private final FileCopyHandlerConfig config;

  /**
   * The cluster map to use for getting the {@link PartitionId}.
   */
  private final ClusterMap clusterMap;

  /**
   * The operation retry handler to use for retrying operations.
   */
  private OperationRetryHandler operationRetryHandler;

  /**
   * Flag to indicate if the handler is running.
   */
  private boolean isRunning = false;

  private static final Logger logger = LoggerFactory.getLogger(StoreFileCopyHandler.class);

  /**
   * Constructor to create StoreFileCopyHandler
   * @param connectionPool the {@link ConnectionPool} to use for making requests.
   * @param storeManager the {@link StoreManager} to use for getting the {@link PartitionFileStore}.
   * @param clusterMap the {@link ClusterMap} to use for getting the {@link PartitionId}.
   * @param config the configuration for the file copy handler.
   */
  public StoreFileCopyHandler(
      @Nonnull ConnectionPool connectionPool,
      @Nonnull StoreManager storeManager,
      @Nonnull ClusterMap clusterMap,
      @Nonnull FileCopyHandlerConfig config) {
    Objects.requireNonNull(connectionPool, "ConnectionPool cannot be null");
    Objects.requireNonNull(storeManager, "StoreManager cannot be null");
    Objects.requireNonNull(clusterMap, "ClusterMap cannot be null");
    Objects.requireNonNull(config, "FileCopyHandlerConfig cannot be null");

    this.connectionPool = connectionPool;
    this.storeManager = storeManager;
    this.clusterMap = clusterMap;
    this.config = config;
    this.operationRetryHandler = new OperationRetryHandler(config);
  }

  /**
   * Start the file copy handler.
   * @throws StoreException
   */
  void start() throws StoreException {
    isRunning = true;
  }

  /**
   * Stop the file copy handler.
   */
  void stop() {
    isRunning = false;
  }

  /**
   * Shutdown the file copy handler. Perform clean up steps in case of a graceful shutdown.
   */
  void shutdown() {
    connectionPool.shutdown();
    isRunning = false;
  }

  /**
   * Get the operation retry handler
   * @return the operation retry handler of type {@link OperationRetryHandler}
   */
  OperationRetryHandler getOperationRetryHandler() {
    return operationRetryHandler;
  }

  /**
   * Set the operation retry handler. Supposed to be used for testing.
   * @param operationRetryHandler the operation retry handler of type {@link OperationRetryHandler}
   */
  void setOperationRetryHandler(OperationRetryHandler operationRetryHandler) {
    this.operationRetryHandler = operationRetryHandler;
  }

  /**
   * Copy the file from the source replica to the destination replica.
   * @param fileCopyInfo the replica info of type {@link FileCopyInfo}
   * @throws IOException
   */
  @Override
  public void copy(@Nonnull FileCopyInfo fileCopyInfo) throws IOException {
    Objects.requireNonNull(fileCopyInfo, "fileCopyReplicaInfo param cannot be null");
    validateIfStoreFileCopyHandlerIsRunning();

    PartitionFileStore fileStore = storeManager.getFileStore(fileCopyInfo.getSourceReplicaId().getPartitionId());
    final String partitionToMountFilePath = fileCopyInfo.getSourceReplicaId().getMountPath() + File.separator +
        fileCopyInfo.getSourceReplicaId().getPartitionId().getId();

    final FileCopyGetMetaDataResponse metadataResponse = getFileCopyGetMetaDataResponse(fileCopyInfo);

    metadataResponse.getLogInfos().forEach(logInfo -> {
      logInfo.getIndexSegments().forEach(indexFile -> fetchAndPersistIndexFile(fileCopyInfo, indexFile,
          partitionToMountFilePath, fileStore));

      FileInfo logFileInfo = new StoreFileInfo(logInfo.getLogSegment().getFileName() + "_log",
          logInfo.getLogSegment().getFileSize());
      fetchAndPersistLogSegment(fileCopyInfo, logFileInfo, partitionToMountFilePath, fileStore);
    });
  }

  /**
   * Get the metadata for the file copy.
   * @param fileCopyInfo the file copy info of type {@link FileCopyInfo}
   * @return
   */
  FileCopyGetMetaDataResponse getFileCopyGetMetaDataResponse(FileCopyInfo fileCopyInfo) {
    String operationName = GetMetadataWorkflow.GET_METADATA_OPERATION_NAME + "[Partition=" +
        fileCopyInfo.getTargetReplicaId().getPartitionId().getId() + "]";
    FileCopyGetMetaDataResponse metadataResponse = null;
    try {
      metadataResponse = operationRetryHandler.executeWithRetry(
          () -> new GetMetadataWorkflow(connectionPool, fileCopyInfo, config).execute(), operationName);
    } catch (IOException e) {
      logMessageAndThrow(operationName, "IO error while fetching metadata file",
          e, FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError);
    } catch (ConnectionPoolTimeoutException e) {
      logMessageAndThrow(operationName, "Connection pool timeout while fetching metadata file",
          e, FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Preserve interrupt status

      logMessageAndThrow(operationName, "Thread interrupted while fetching metadata file",
          e, FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError);
    } catch (RuntimeException e) {
      logMessageAndThrow(operationName, "Unexpected runtime error while fetching metadata file",
          e, FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    } catch (Exception e) {
      logMessageAndThrow(operationName, "Exception while fetching metadata file",
          e, FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }

    validateResponseOrThrow(metadataResponse, operationName);
    logger.info(operationName + ": Fetched metadata");
    return metadataResponse;
  }

  /**
   * Fetch and persist the log segment.
   * @param fileCopyInfo the file copy info
   * @param logFileInfo the log file info
   * @param partitionToMountFilePath the partition file path
   * @param fileStore the file store
   */
  private void fetchAndPersistLogSegment(FileCopyInfo fileCopyInfo, FileInfo logFileInfo,
      String partitionToMountFilePath, PartitionFileStore fileStore) {
    int chunksInLogSegment = (int) Math.ceil((double) logFileInfo.getFileSize() / config.getFileCopyHandlerChunkSize);
    logger.info("Number of chunks in log segment: {} for filename {}", chunksInLogSegment, logFileInfo.getFileName());

    FileCopyGetChunkResponse chunkResponse;
    final long targetPartitionId = fileCopyInfo.getTargetReplicaId().getPartitionId().getId();

    for (int i = 0; i < chunksInLogSegment; i++) {
      long startOffset = (long) i * config.getFileCopyHandlerChunkSize;
      long sizeInBytes = Math.min(config.getFileCopyHandlerChunkSize, logFileInfo.getFileSize() - startOffset);
      FileChunkInfo fileChunkInfo = new FileChunkInfo(logFileInfo.getFileName(), startOffset,
          sizeInBytes, true);
      String operationName = GetChunkDataWorkflow.GET_CHUNK_OPERATION_NAME + "[Partition=" + targetPartitionId +
          ", FileName=" + logFileInfo.getFileName() + ", Chunk=" + (i+1) + "]";

      try {
        chunkResponse = operationRetryHandler.executeWithRetry(
            () -> new GetChunkDataWorkflow(connectionPool, fileCopyInfo, fileChunkInfo, clusterMap, config)
                .execute(), operationName);
        validateResponseOrThrow(chunkResponse, operationName);
        logger.info(operationName + ": Fetched chunk");

        String filePath = partitionToMountFilePath + File.separator + logFileInfo.getFileName();
        StoreFileChunk chunkToWrite = new StoreFileChunk(chunkResponse.getChunkStream(),
            chunkResponse.getChunkStream().available());

        fileStore.writeStoreFileChunkToDisk(filePath, chunkToWrite);
      } catch (IOException e) {
        logMessageAndThrow(operationName, "IO error while processing chunk", e,
            FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerWriteToDiskError);
      } catch (ConnectionPoolTimeoutException e) {
        logMessageAndThrow(operationName, "Connection pool timeout while processing chunk", e,
            FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetChunkDataApiError);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();  // Preserve interrupt status

        logMessageAndThrow(operationName, "Thread interrupted while processing chunk", e,
            FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetChunkDataApiError);
      } catch (RuntimeException e) {
        logMessageAndThrow(operationName, "Unexpected runtime error while processing chunk", e,
            FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
      } catch (Exception e) {
        logMessageAndThrow(operationName, "Exception while processing chunk", e,
            FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
      }
    }
  }

  /**
   * Fetch and persist the index file.
   * @param fileCopyInfo the file copy info
   * @param indexFile the index file
   * @param partitionToMountFilePath the partition file path
   * @param fileStore the file store
   */
  private void fetchAndPersistIndexFile(FileCopyInfo fileCopyInfo, FileInfo indexFile,
      String partitionToMountFilePath, PartitionFileStore fileStore) {
    String operationName = GetChunkDataWorkflow.GET_FILE_OPERATION_NAME;
    FileChunkInfo fileChunkInfo = new FileChunkInfo(indexFile.getFileName(), 0,
        indexFile.getFileSize(), false);

    try {
      FileCopyGetChunkResponse chunkResponse = operationRetryHandler.executeWithRetry(
          () -> new GetChunkDataWorkflow(connectionPool, fileCopyInfo, fileChunkInfo, clusterMap, config)
              .execute(), operationName);
      validateResponseOrThrow(chunkResponse, operationName);

      String filePath = partitionToMountFilePath + File.separator + indexFile.getFileName();
      StoreFileChunk chunkToWrite = new StoreFileChunk(chunkResponse.getChunkStream(),
          chunkResponse.getChunkStream().available());

      fileStore.writeStoreFileChunkToDisk(filePath, chunkToWrite);
    } catch (IOException e) {
      logMessageAndThrow(operationName, "IO error while processing index file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerWriteToDiskError);
    } catch (ConnectionPoolTimeoutException e) {
      logMessageAndThrow(operationName, "Connection pool timeout while processing index file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetChunkDataApiError);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Preserve interrupt status

      logMessageAndThrow(operationName, "Thread interrupted while processing index file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetChunkDataApiError);
    } catch (RuntimeException e) {
      logMessageAndThrow(operationName, "Unexpected runtime error while processing index file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    } catch (Exception e) {
      logMessageAndThrow(operationName, "Exception while processing index file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }
  }

  /**
   * Log the message and throw a FileCopyHandlerException.
   * @param operationName the operation name
   * @param message the message
   * @param e the exception
   * @param fileCopyHandlerErrorCode the error code
   */
  private void logMessageAndThrow(String operationName, String message, Exception e,
      FileCopyHandlerException.FileCopyHandlerErrorCode fileCopyHandlerErrorCode) {
    String s = operationName + ": " +  message;
    logger.error(s, e);
    throw new FileCopyHandlerException(s, e, fileCopyHandlerErrorCode);
  }

  /**
   * Validate if the file copy handler is running. Throw a FileCopyHandlerException if it is not running.
   */
  private void validateIfStoreFileCopyHandlerIsRunning() {
    if (!isRunning) {
      logger.error("FileCopyHandler is not running");
      throw new FileCopyHandlerException("FileCopyHandler is not running",
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerRunningFailure);
    }
  }

  /**
   * Validate the response or throw a FileCopyHandlerException.
   * @param response
   * @param operationName
   */
  private void validateResponseOrThrow(RequestOrResponse response, String operationName) {
    if (response == null) {
      logger.error(operationName + ": not expecting null response");
      throw new FileCopyHandlerException(operationName + ": not expecting null response",
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }
  }
}
