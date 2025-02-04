package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.http2.Http2BlockingChannelPool;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.protocol.FileCopyGetChunkRequest;
import com.github.ambry.protocol.FileCopyGetChunkResponse;
import com.github.ambry.protocol.FileCopyGetMetaDataRequest;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.protocol.FileInfo;
import com.github.ambry.store.FileStore;
import com.github.ambry.store.LogInfo;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileCopyHandler {
  private final ConnectionPool connectionPool;
  private final FileStore fileStore;
  private final ClusterMap clusterMap;
  private final int CHUNK_SIZE = 20 * 1024 * 1024; // 20MB

  private static final Logger logger = LoggerFactory.getLogger(FileCopyHandler.class);

  public FileCopyHandler(ConnectionPool connectionPool, FileStore fileStore, ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
    this.fileStore = fileStore;
    this.connectionPool = connectionPool;
  }

  // TODO fix this ctor
  // Fails with :- java.lang.NoClassDefFoundError: com/github/ambry/server/FileCopyUtils
  public FileCopyHandler(VerifiableProperties properties, ClusterMap clusterMap) throws Exception {
    this.clusterMap = clusterMap;
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
    MetricRegistry registry = clusterMap.getMetricRegistry();
    SSLConfig sslConfig = new SSLConfig(properties);
    SSLFactory sslHttp2Factory = new NettySslHttp2Factory(sslConfig);

    if (clusterMapConfig.clusterMapEnableHttp2Replication) {
      Http2ClientMetrics http2ClientMetrics = new Http2ClientMetrics(registry);
      Http2ClientConfig http2ClientConfig = new Http2ClientConfig(properties);
      connectionPool = new Http2BlockingChannelPool(sslHttp2Factory, http2ClientConfig, http2ClientMetrics);
    } else {
      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
    }
    FileCopyConfig fileCopyConfig = new FileCopyConfig(properties);
    fileStore = new FileStore("dataDir", fileCopyConfig);
    fileStore.start();
  }

  public void copy(PartitionId partitionId, ReplicaId sourceReplicaId, ReplicaId targetReplicaId)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    FileCopyGetMetaDataRequest request = new FileCopyGetMetaDataRequest(
        FileCopyGetMetaDataRequest.File_Metadata_Request_Version_V1, 0, "", partitionId, "hostName");

    logger.info("Demo: Request: {}", request);
    long startTimeMs = System.currentTimeMillis();
    ConnectedChannel connectedChannel =
        connectionPool.checkOutConnection(targetReplicaId.getDataNodeId().getHostname(), targetReplicaId.getDataNodeId().getPortToConnectTo(), 40);
    ChannelOutput channelOutput = connectedChannel.sendAndReceive(request);
    FileCopyGetMetaDataResponse response = FileCopyGetMetaDataResponse.readFrom(channelOutput.getInputStream());
    logger.info("Demo: FileCopyGetMetaDataRequest Api took {} ms", System.currentTimeMillis() - startTimeMs);
    logger.info("Demo: Response: {}", response);

    List<LogInfo> logInfos = AmbryRequests.convertProtocolToStoreLogInfo(response.getLogInfos());

    String partitionFilePath = sourceReplicaId.getMountPath() + File.separator + partitionId.getId();
    fileStore.persistMetaDataToFile(partitionFilePath, logInfos);

    response.getLogInfos().forEach(logInfo -> {
      logInfo.getIndexFiles().forEach(indexFile -> {
        String filePath = partitionFilePath + "/" + indexFile.getFileName();
        try {
          fetchAndPersistChunks(partitionId, targetReplicaId, clusterMap, indexFile, filePath, Long.MAX_VALUE, 0);
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
      String filePath = partitionFilePath + "/" + logInfo.getFileName() + "_log";
      FileInfo logFileInfo = new FileInfo(logInfo.getFileName() + "_log", logInfo.getFileSizeInBytes());

      int chunksInLogSegment = (int) Math.ceil((double) logFileInfo.getFileSizeInBytes() / CHUNK_SIZE);
      logger.info("Demo: Total chunks in log segment: {}", chunksInLogSegment);

      for (int i = 0; i < chunksInLogSegment; i++) {
        long startOffset = (long) i * CHUNK_SIZE;
        long sizeInBytes = Math.min(CHUNK_SIZE, logFileInfo.getFileSizeInBytes() - startOffset);
        logger.info("Demo: Fetching chunk {} for log segment: {} startOffset: {} sizeInBytes: {}", i+1, filePath, startOffset, sizeInBytes);
        try {
          fetchAndPersistChunks(partitionId, targetReplicaId, clusterMap, logFileInfo, filePath, sizeInBytes, startOffset);
        } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void fetchAndPersistChunks(PartitionId partitionId, ReplicaId replicaId, ClusterMap clusterMap,
      FileInfo fileInfo, String filePath, long sizeInBytes, long startOffset)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    FileCopyGetChunkRequest request = new FileCopyGetChunkRequest(
        FileCopyGetChunkRequest.File_Chunk_Request_Version_V1, 0, "", partitionId,
        fileInfo.getFileName(), startOffset, sizeInBytes);

    logger.info("Demo: Request: {}", request);
    long startTimeMs = System.currentTimeMillis();
    ConnectedChannel connectedChannel =
        connectionPool.checkOutConnection(replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPortToConnectTo(), 99999);
    ChannelOutput chunkChannelOutput = connectedChannel.sendAndReceive(request);
    FileCopyGetChunkResponse response = FileCopyGetChunkResponse.readFrom(chunkChannelOutput.getInputStream(), clusterMap);
    logger.info("Demo: FileCopyGetChunkRequest Api took {} ms", System.currentTimeMillis() - startTimeMs);
    logger.info("Demo: Response: {}", response);

    putChunkToFile(filePath, response.getChunkStream(), response.getChunkSizeInBytes());
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