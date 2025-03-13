package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.protocol.FileCopyGetChunkRequest;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataRequest;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.FileStoreException;
import com.github.ambry.store.PartitionFileStore;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFileChunk;
import com.github.ambry.store.StoreFileInfo;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestStoreFileCopyHandler implements FileCopyHandler {
  private final ConnectionPool connectionPool;
  private final StoreManager storeManager;
  private final ClusterMap clusterMap;
  private boolean isRunning = false;

  // TODO: From cfg2
  private static final int CHUNK_SIZE = 100 * 1024 * 1024; // 100MB

  private static final Logger logger = LoggerFactory.getLogger(TestStoreFileCopyHandler.class);

  public TestStoreFileCopyHandler(ConnectionPool connectionPool, StoreManager storeManager, ClusterMap clusterMap) {
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

  private void validateIfStoreFileCopyHandlerIsRunning() {
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", FileStoreException.FileStoreErrorCode.FileStoreRunningFailure);
    }
  }

  public void shutdown() {
    isRunning = false;
  }

  @Override
  public void copy(ReplicaId sourceReplicaId, ReplicaId targetReplicaId)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    validateIfStoreFileCopyHandlerIsRunning();

    PartitionFileStore fileStore = this.storeManager.getFileStore(sourceReplicaId.getPartitionId());

    FileCopyGetMetaDataRequest request = new FileCopyGetMetaDataRequest(
        FileCopyGetMetaDataRequest.FILE_METADATA_REQUEST_VERSION_V_1, 0, "", sourceReplicaId.getPartitionId(), "hostName");

    logger.info("Demo: Request: {}", request);
    long startTimeMs = System.currentTimeMillis();
    ConnectedChannel connectedChannel =
        connectionPool.checkOutConnection(targetReplicaId.getDataNodeId().getHostname(), targetReplicaId.getDataNodeId().getPortToConnectTo(), 40);
    ChannelOutput channelOutput = connectedChannel.sendAndReceive(request);
    FileCopyGetMetaDataResponse response = FileCopyGetMetaDataResponse.readFrom(channelOutput.getInputStream());
    logger.info("Demo: FileCopyGetMetaDataRequest Api took {} ms", System.currentTimeMillis() - startTimeMs);
    logger.info("Demo: Response: {}", response);

    fileStore.writeMetaDataFileToDisk(response.getLogInfos());
    String partitionFilePath = sourceReplicaId.getMountPath() + File.separator + sourceReplicaId.getPartitionId().getId();

    response.getLogInfos().forEach(logInfo -> {
      logInfo.getIndexSegments().forEach(indexFile -> {
        String filePath = partitionFilePath + "/fc_" + indexFile.getFileName();
        try {
          fetchAndPersistChunks(fileStore, sourceReplicaId.getPartitionId(), targetReplicaId, clusterMap, indexFile, filePath, Long.MAX_VALUE, 0, false);
        } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
//      logInfo.getBloomFilters().forEach(bloomFile -> {
//        String filePath = partitionFilePath + "/" + bloomFile.getFileName();
//        try {
//          fetchAndPersistChunks(partitionId, targetReplicaId, clusterMap, bloomFile, filePath, Long.MAX_VALUE, 0);
//        } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
//          throw new RuntimeException(e);
//        }
//      });
      String filePath = partitionFilePath + "/fc_" + logInfo.getLogSegment().getFileName() + "_log";
      FileInfo logFileInfo = new StoreFileInfo(logInfo.getLogSegment().getFileName() + "_log", logInfo.getLogSegment().getFileSize());

      int chunksInLogSegment = (int) Math.ceil((double) logFileInfo.getFileSize() / CHUNK_SIZE);
      logger.info("Demo: Total chunks in log segment: {}", chunksInLogSegment);

      for (int i = 0; i < chunksInLogSegment; i++) {
        long startOffset = (long) i * CHUNK_SIZE;
        long sizeInBytes = Math.min(CHUNK_SIZE, logFileInfo.getFileSize() - startOffset);
        logger.info("Demo: Fetching chunk {} for log segment: {} startOffset: {} sizeInBytes: {}", i+1, filePath, startOffset, sizeInBytes);
        try {
          fetchAndPersistChunks(fileStore, sourceReplicaId.getPartitionId(), targetReplicaId, clusterMap, logFileInfo, filePath, sizeInBytes, startOffset, true);
        } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void fetchAndPersistChunks(PartitionFileStore fileStore, PartitionId partitionId, ReplicaId replicaId, ClusterMap clusterMap,
      FileInfo fileInfo, String filePath, long sizeInBytes, long startOffset, boolean isChunked)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    FileCopyGetChunkRequest request = new FileCopyGetChunkRequest(
        FileCopyGetChunkRequest.FILE_CHUNK_REQUEST_VERSION_V_1, 0, "", partitionId,
        fileInfo.getFileName(), startOffset, sizeInBytes, isChunked);

    logger.info("Demo: Request: {}", request);
    long startTimeMs = System.currentTimeMillis();
    ConnectedChannel connectedChannel =
        connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPortToConnectTo(), 99999);
    ChannelOutput chunkChannelOutput = connectedChannel.sendAndReceive(request);
    FileCopyGetChunkResponse
        response = FileCopyGetChunkResponse.readFrom(chunkChannelOutput.getInputStream(), clusterMap);
    logger.info("Demo: FileCopyGetChunkRequest Api took {} ms", System.currentTimeMillis() - startTimeMs);
    logger.info("Demo: Response: {}", response);

//    putChunkToFile(filePath, response.getChunkStream(), response.getChunkSizeInBytes());
    putChunkToFile(fileStore, filePath, response.getChunkStream());
  }

  private void putChunkToFile(PartitionFileStore fileStore, String filePath, DataInputStream chunkStream)
      throws IOException {
    fileStore.writeStoreFileChunkToDisk(filePath, new StoreFileChunk(chunkStream, chunkStream.available()));
  }

  private void putChunkToFile(String filePath, DataInputStream stream, long chunkSizeInBytes) throws IOException {
    long startTimeMs = System.currentTimeMillis();
    if(!new File(filePath).exists()) {
      Files.createFile(new File(filePath).toPath());
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[(int) chunkSizeInBytes];
    int bytesRead;
    while ((bytesRead = stream.read(buffer)) != -1) {
      byteArrayOutputStream.write(buffer, 0, bytesRead);
    }
    Files.write(Paths.get(filePath), buffer, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    logger.info("Demo: putChunkToFile took {} ms", System.currentTimeMillis() - startTimeMs);
    logger.info("Demo: Write successful for chunk to file: " + filePath);

//    FileInputStream fileInputStream = new FileInputStream(String.valueOf(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));
//    fileStore.putChunkToFile(filePath, fileInputStream);
  }
}