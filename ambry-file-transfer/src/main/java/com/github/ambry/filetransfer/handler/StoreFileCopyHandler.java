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
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.filetransfer.FileChunkInfo;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.filetransfer.FileCopyMetrics;
import com.github.ambry.filetransfer.utils.OperationRetryHandler;
import com.github.ambry.filetransfer.workflow.GetChunkDataWorkflow;
import com.github.ambry.filetransfer.workflow.GetMetadataWorkflow;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.protocol.Response;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.LogInfo;
import com.github.ambry.store.PartitionFileStore;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFileChunk;
import com.github.ambry.store.StoreFileInfo;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
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
  private final FileCopyBasedReplicationConfig config;

  private final StoreConfig storeConfig;

  /**
   * The cluster map to use for getting the {@link PartitionId}.
   */
  private final ClusterMap clusterMap;

  /**
   * The operation retry handler to use for retrying operations.
   */
  private OperationRetryHandler operationRetryHandler;

  private FileCopyMetrics fileCopyMetrics;

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
  public StoreFileCopyHandler(@Nonnull ConnectionPool connectionPool, @Nonnull StoreManager storeManager,
      @Nonnull ClusterMap clusterMap, @Nonnull FileCopyBasedReplicationConfig config, @Nonnull StoreConfig storeConfig,
      @Nonnull FileCopyMetrics fileCopyMetrics) {
    Objects.requireNonNull(connectionPool, "ConnectionPool cannot be null");
    Objects.requireNonNull(storeManager, "StoreManager cannot be null");
    Objects.requireNonNull(clusterMap, "ClusterMap cannot be null");
    Objects.requireNonNull(config, "FileCopyHandlerConfig cannot be null");
    Objects.requireNonNull(storeConfig, "StoreConfig cannot be null");
    Objects.requireNonNull(fileCopyMetrics, "FileCopyMetrics cannot be null");

    this.connectionPool = connectionPool;
    this.storeManager = storeManager;
    this.clusterMap = clusterMap;
    this.config = config;
    this.storeConfig = storeConfig;
    this.operationRetryHandler = new OperationRetryHandler(config);
    this.fileCopyMetrics = fileCopyMetrics;
  }

  /**
   * Start the file copy handler.
   * @throws StoreException
   */
  public void start() throws StoreException {
    isRunning = true;
  }

  /**
   * Stop the file copy handler.
   */
  void stop() {
    isRunning = false;
  }

  /**
   * Shutdown the file copy handler.
   */
  public void shutdown() {
    isRunning = false;
  }

  /**
   * Get the Handler's StoreManager
   * @return the store manager of type {@link StoreManager}
   */
  public StoreManager getStoreManager() {
    return storeManager;
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
  public void copy(@Nonnull FileCopyInfo fileCopyInfo) throws Exception {
    logger.info("File Copy handler is running for partition: {} on Mount Path: {} hydrating from DataNode: {}",
        fileCopyInfo.getSourceReplicaId().getPartitionId().getId(), fileCopyInfo.getSourceReplicaId().getMountPath(),
        fileCopyInfo.getTargetReplicaId().getDataNodeId());
    Objects.requireNonNull(fileCopyInfo, "fileCopyReplicaInfo param cannot be null");
    validateIfStoreFileCopyHandlerIsRunning();
    long startTimeInMs = System.currentTimeMillis();
    long totalSizeToDownloadInBytes = 0;
    AtomicLong totalSizeDownloadedInBytes = new AtomicLong();
    final PartitionFileStore fileStore = storeManager.getFileStore(fileCopyInfo.getSourceReplicaId().getPartitionId());
    final String partitionToMountFilePath =
        fileCopyInfo.getSourceReplicaId().getMountPath() + File.separator + fileCopyInfo.getSourceReplicaId()
            .getPartitionId()
            .getId();
    FileCopyGetMetaDataResponse metadataResponse = null;
    try {
      metadataResponse = getFileCopyGetMetaDataResponse(fileCopyInfo);

      for (LogInfo logInfo : metadataResponse.getLogInfos()) {
        totalSizeToDownloadInBytes = totalSizeToDownloadInBytes + logInfo.getLogSegment().getFileSize();
        for (FileInfo indexSegment : logInfo.getIndexSegments()) {
          totalSizeToDownloadInBytes = totalSizeToDownloadInBytes + indexSegment.getFileSize();
        }
      }
      fileCopyMetrics.updateFileCopyDataPerPartitionInBytes(totalSizeToDownloadInBytes);

      String snapshotId = metadataResponse.getSnapshotId();

      metadataResponse.getLogInfos().forEach(logInfo -> {
        // Process the respective files and copy it to the temporary path.
        final String partitionToMountTempFilePath =
            partitionToMountFilePath + File.separator + storeConfig.storeFileCopyTemporaryDirectoryName;
        logInfo.getIndexSegments().forEach(indexFile -> {
          processIndexFile(indexFile, partitionToMountTempFilePath, fileCopyInfo, snapshotId, fileStore);
          totalSizeDownloadedInBytes.addAndGet(indexFile.getFileSize());
        });
        // Process log segment
        long storeId = fileCopyInfo.getSourceReplicaId().getPartitionId().getId();
        // allocate the file
        FileInfo logFileInfo =
            new StoreFileInfo(logInfo.getLogSegment().getFileName() + "_log", logInfo.getLogSegment().getFileSize());
        String filePath = partitionToMountTempFilePath + File.separator + logFileInfo.getFileName();
        try {
          fileStore.allocateFile(filePath, Long.toString(storeId));
        } catch (IOException e) {
          logMessageAndThrow("ProcessLogSegment", "Failed Disk Space Allocation", e,
              FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerFailedDiskSpaceAllocation);
        }
        processLogSegment(logInfo, partitionToMountTempFilePath, fileCopyInfo, snapshotId, fileStore);
        totalSizeDownloadedInBytes.addAndGet(logInfo.getLogSegment().getFileSize());

        // Move all files to actual path.
        try {
          fileStore.moveAllRegularFiles(partitionToMountTempFilePath, partitionToMountFilePath);
        } catch (IOException e) {
          logMessageAndThrow("MoveFilesOperation", "Error moving files", e,
              FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerWriteToDiskError);
        }
      });
    } finally {
      if (metadataResponse != null) {
        metadataResponse.release();
      }
      int totalTimeInSec = (int) ((System.currentTimeMillis() - startTimeInMs) / 1000);
      if (totalTimeInSec != 0) {
        fileCopyMetrics.updateFileCopyAverageSpeedPerPartition(totalSizeDownloadedInBytes.get() / totalTimeInSec);
      }
    }
  }

  /**
   * Get the metadata for the file copy.
   * @param fileCopyInfo the file copy info of type {@link FileCopyInfo}
   * @return
   */
  FileCopyGetMetaDataResponse getFileCopyGetMetaDataResponse(FileCopyInfo fileCopyInfo) {
    validateIfStoreFileCopyHandlerIsRunning();
    String operationName =
        GetMetadataWorkflow.GET_METADATA_OPERATION_NAME + "[Partition=" + fileCopyInfo.getTargetReplicaId()
            .getPartitionId()
            .getId() + "]";
    FileCopyGetMetaDataResponse metadataResponse = null;

    try {
      metadataResponse = operationRetryHandler.executeWithRetry(
          () -> new GetMetadataWorkflow(connectionPool, fileCopyInfo, config).execute(), operationName);
    } catch (IOException e) {
      logMessageAndThrow(operationName, "IO error while fetching metadata file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError);
    } catch (ConnectionPoolTimeoutException e) {
      logMessageAndThrow(operationName, "Connection pool timeout while fetching metadata file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Preserve interrupt status

      logMessageAndThrow(operationName, "Thread interrupted while fetching metadata file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError);
    } catch (RuntimeException e) {
      logMessageAndThrow(operationName, "Unexpected runtime error while fetching metadata file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    } catch (Exception e) {
      logMessageAndThrow(operationName, "Exception while fetching metadata file", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }
    validateResponseOrThrow(metadataResponse, operationName);
    logger.info(operationName + ": Fetched metadata for partition: " + fileCopyInfo.getSourceReplicaId()
        .getPartitionId()
        .getId() + " - " + metadataResponse.toString());
    return metadataResponse;
  }

  /**
   * Get the chunk data for the file copy.
   * @param fileCopyInfo the file copy info of type {@link FileCopyInfo}
   * @param isChunked boolean to indicate if the file is chunked
   * @return the chunk response of type {@link FileCopyGetChunkResponse}
   */
  FileCopyGetChunkResponse getFileCopyGetChunkResponse(String operationName, FileCopyInfo fileCopyInfo,
      FileChunkInfo fileChunkInfo, String snapshotId, boolean isChunked) {
    validateIfStoreFileCopyHandlerIsRunning();

    FileCopyGetChunkResponse chunkResponse = null;
    String errorSuffix = " while processing the " + (isChunked ? "chunk" : "file");
    try {
      chunkResponse = operationRetryHandler.executeWithRetry(
          () -> new GetChunkDataWorkflow(connectionPool, fileCopyInfo, fileChunkInfo, snapshotId, clusterMap,
              config).execute(), operationName);
    } catch (IOException e) {
      logMessageAndThrow(operationName, "IO error" + errorSuffix, e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerWriteToDiskError);
    } catch (ConnectionPoolTimeoutException e) {
      logMessageAndThrow(operationName, "Connection pool timeout" + errorSuffix, e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetChunkDataApiError);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Preserve interrupt status

      logMessageAndThrow(operationName, "Thread interrupted" + errorSuffix, e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetChunkDataApiError);
    } catch (RuntimeException e) {
      logMessageAndThrow(operationName, "Unexpected runtime error" + errorSuffix, e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    } catch (Exception e) {
      logMessageAndThrow(operationName, "Exception" + errorSuffix, e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }
    validateResponseOrThrow(chunkResponse, operationName);
    return chunkResponse;
  }

  /**
   * Process the index file.
   * @param indexFile the index file to process
   * @param partitionToMountFilePath the partition file path
   * @param fileCopyInfo the file copy info
   * @param fileStore the file store
   */
  private void processIndexFile(FileInfo indexFile, String partitionToMountFilePath, FileCopyInfo fileCopyInfo,
      String snapshotId, PartitionFileStore fileStore) {
    validateIfStoreFileCopyHandlerIsRunning();
    final FileChunkInfo fileChunkInfo = new FileChunkInfo(indexFile.getFileName(), 0, indexFile.getFileSize(), false);
    FileCopyGetChunkResponse chunkResponse = null;
    try {
      chunkResponse =
          getFileCopyGetChunkResponse(GetChunkDataWorkflow.GET_FILE_OPERATION_NAME, fileCopyInfo, fileChunkInfo,
              snapshotId, false);
      String filePath = partitionToMountFilePath + File.separator + indexFile.getFileName();
      writeStoreFileChunkToDisk(chunkResponse, filePath, fileStore);
    } finally {
      if (chunkResponse != null) {
        chunkResponse.release();
      }
    }
  }

  /**
   * Process the log segment.
   * @param logInfo the log info to process
   * @param partitionToMountFilePath the partition file path
   * @param fileCopyInfo the file copy info
   * @param fileStore the file store
   */
  private void processLogSegment(LogInfo logInfo, String partitionToMountFilePath, FileCopyInfo fileCopyInfo,
      String snapshotId, PartitionFileStore fileStore) {
    if (logInfo.getLogSegment().getFileSize() > fileStore.getSegmentCapacity()) {
      throw new FileCopyHandlerException("Log segment file size is greater than the segment capacity",
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerInvalidLogFileSize);
    }
    FileInfo logFileInfo =
        new StoreFileInfo(logInfo.getLogSegment().getFileName() + "_log", logInfo.getLogSegment().getFileSize());
    int chunksInLogSegment = (int) Math.ceil((double) logFileInfo.getFileSize() / config.getFileCopyHandlerChunkSize);
    logger.info("Number of chunks in log segment: {} for filename {}", chunksInLogSegment, logFileInfo.getFileName());

    for (int i = 0; i < chunksInLogSegment; i++) {
      //Throw Exception and come out of the thread a shutdown is called.
      validateIfStoreFileCopyHandlerIsRunning();
      long startOffset = (long) i * config.getFileCopyHandlerChunkSize;
      long sizeInBytes = Math.min(config.getFileCopyHandlerChunkSize, logFileInfo.getFileSize() - startOffset);

      String operationName =
          GetChunkDataWorkflow.GET_CHUNK_OPERATION_NAME + "[Partition=" + fileCopyInfo.getTargetReplicaId()
              .getPartitionId()
              .getId() + ", FileName=" + logFileInfo.getFileName() + ", Chunk=" + (i + 1) + "]";
      FileChunkInfo fileChunkInfo = new FileChunkInfo(logFileInfo.getFileName(), startOffset, sizeInBytes, true);

      FileCopyGetChunkResponse chunkResponse = null;
      try {
        chunkResponse = getFileCopyGetChunkResponse(operationName, fileCopyInfo, fileChunkInfo, snapshotId, true);
        String filePath = partitionToMountFilePath + File.separator + logFileInfo.getFileName();
        writeStoreFileChunkToDisk(chunkResponse, filePath, fileStore);
      } finally {
        if (chunkResponse != null) {
          chunkResponse.release();
        }
      }
    }
  }

  /**
   * Write the store file chunk to disk.
   * @param chunkResponse the chunk response of type {@link FileCopyGetChunkResponse}
   * @param filePath the file path to write to
   * @param fileStore the file store to write to
   */
  private void writeStoreFileChunkToDisk(FileCopyGetChunkResponse chunkResponse, String filePath,
      PartitionFileStore fileStore) {
    validateIfStoreFileCopyHandlerIsRunning();
    StoreFileChunk chunkToWrite;
    try {
      chunkToWrite = new StoreFileChunk(chunkResponse.getChunkStream(), chunkResponse.getChunkStream().available());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      fileStore.writeStoreFileChunkToDisk(filePath, chunkToWrite);
    } catch (IOException e) {
      logMessageAndThrow("WriteChunkOperation", "Error writing file chunk to disk", e,
          FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerWriteToDiskError);
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
    String s = operationName + ": " + message;
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
   * @param response the response to validate
   * @param operationName the operation name
   */
  private void validateResponseOrThrow(Response response, String operationName) {
    if (response == null) {
      logger.error(operationName + ": not expecting null response");
      throw new FileCopyHandlerException(operationName + ": not expecting null response",
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }
    if (response.getError() != ServerErrorCode.NoError) {
      logger.error(operationName + ": not expecting error response");
      throw new FileCopyHandlerException(operationName + ": not expecting error response",
          FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError);
    }
  }
}
