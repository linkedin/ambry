/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClient;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerPerformance implements Closeable {

  private static final int POLL_TIMEOUT_MS = 100000;
  private static final int OPERATION_TIMEOUT_MS = 500000;
  private static final String CLIENT_ID = "ServerReadPerformance";
  private final ClusterMap clusterMap;
  private final Http2NetworkClient networkClient;
  private final Time time = SystemTime.getInstance();

  private final ExecutorService executorService;
  private final AtomicInteger correlationId = new AtomicInteger(0);

  ServerReadPerformanceConfig serverReadPerformanceConfig;

  private static final Logger logger = LoggerFactory.getLogger(ServerPerformance.class);

  public static class ServerReadPerformanceConfig {
    /**
     * The path to the hardware layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
    final String partitionLayoutFilePath;

    final int maxParallelRequests;

    /**
     * The rate at which reads need to be performed
     */
    @Config("reads.per.second")
    @Default("1000")
    final int readsPerSecond;

    /**
     * The interval in second to report performance result
     */
    @Config("measurement.interval")
    @Default("300")
    final long measurementInterval;

    @Config("log.to.read")
    @Default("")
    final String logToRead;

    /**
     * The hostname of the target server as it appears in the partition layout.
     */
    @Config("hostname")
    @Default("localhost")
    final String hostname;

    /**
     * The port of the target server in the partition layout (need not be the actual port to connect to).
     */
    @Config("port")
    @Default("6667")
    final int port;

    ServerReadPerformanceConfig(VerifiableProperties verifiableProperties) {
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      readsPerSecond = verifiableProperties.getInt("reads.per.second", 1000);
      measurementInterval = verifiableProperties.getLong("measurement.interval", 300L);
      logToRead = verifiableProperties.getString("log.to.read", "");
      hostname = verifiableProperties.getString("hostname", "localhost");
      port = verifiableProperties.getInt("port", 6667);
      maxParallelRequests = verifiableProperties.getInt("max.parallel.requests", 1000);
    }
  }

  public ServerPerformance(ClusterMap clusterMap, SSLFactory sslFactory, VerifiableProperties verifiableProperties,
      ServerReadPerformanceConfig config) throws Exception {
    Http2ClientMetrics metrics = new Http2ClientMetrics(clusterMap.getMetricRegistry());
    Http2ClientConfig http2ClientConfig = new Http2ClientConfig(verifiableProperties);
    this.clusterMap = clusterMap;
    networkClient = new Http2NetworkClientFactory(metrics, http2ClientConfig, sslFactory, time).getNetworkClient();
    serverReadPerformanceConfig = config;
    executorService = Executors.newFixedThreadPool(10);
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    ServerReadPerformanceConfig config = new ServerReadPerformanceConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterMap clusterMap =
        ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap();
    SSLFactory sslFactory = new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    ServerPerformance serverReadPerformanceNew =
        new ServerPerformance(clusterMap, sslFactory, verifiableProperties, config);

    for (int i = 0; i < 10000; i++) {
      serverReadPerformanceNew.runPerfTest(config);
    }
    serverReadPerformanceNew.close();
    logger.info("fvoidfnkvkfn");
    System.exit(0);
  }

  void runPerfTest(ServerReadPerformanceConfig config) throws Exception {
    final BufferedReader br = new BufferedReader(new FileReader(config.logToRead));
    String line;
    Map<Integer, BlobId> correlationIdToBlob = new HashMap<>();
    while ((line = br.readLine()) != null) {
      String[] id = line.split("\n");
      logger.info("processing the blob id {}", id[0]);
      BlobId blobId = new BlobId(id[0], clusterMap);
      PartitionRequestInfo partitionRequestInfo =
          new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
      GetRequest getRequest = new GetRequest(correlationId.incrementAndGet(), CLIENT_ID, MessageFormatFlags.Blob,
          Collections.singletonList(partitionRequestInfo), GetOption.Include_All);
      correlationIdToBlob.put(getRequest.getCorrelationId(), blobId);
      List<ResponseInfo> responseInfos = sendAndPoll(getRequest);
      processResponses(correlationIdToBlob, responseInfos);

      while (((ThreadPoolExecutor) executorService).getQueue().size() >= 10) {
        Thread.sleep(5);
      }
    }
    br.close();
    clusterMap.close();

    while (!correlationIdToBlob.isEmpty()) {
      List<ResponseInfo> responses = networkClient.sendAndPoll(Collections.emptyList(), new HashSet<>(), 10);
      processResponses(correlationIdToBlob, responses);
    }
  }

  void processResponses(Map<Integer, BlobId> correlationIdToBlob, List<ResponseInfo> responseInfos) {
    responseInfos.forEach(responseInfo -> {
      executorService.submit(() -> {
        try {
          InputStream serverResponseStream = new NettyByteBufDataInputStream(responseInfo.content());
          GetResponse getResponse = GetResponse.readFrom(new DataInputStream(serverResponseStream), clusterMap);
          correlationIdToBlob.remove(getResponse.getCorrelationId());
          ServerErrorCode partitionErrorCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
          ServerErrorCode errorCode =
              partitionErrorCode == ServerErrorCode.No_Error ? getResponse.getError() : partitionErrorCode;
          InputStream stream = errorCode == ServerErrorCode.No_Error ? getResponse.getInputStream() : null;
          BlobData blobData = stream != null ? MessageFormatRecord.deserializeBlob(stream) : null;
          long sizeRead = blobData.getSize();
          byte[] outputBuffer = new byte[(int) blobData.getSize()];
          ByteBufferOutputStream streamOut = new ByteBufferOutputStream(ByteBuffer.wrap(outputBuffer));
          ByteBuf buffer = blobData.content();
          try {
            buffer.readBytes(streamOut, (int) blobData.getSize());
          } finally {
            buffer.release();
          }
          logger.info("blob property {}", sizeRead);
        } catch (Exception e) {
          logger.error("Error ", e);
        }
      });
    });
  }

  List<ResponseInfo> sendAndPoll(GetRequest getRequest) {
    DataNodeId dataNodeId =
        clusterMap.getDataNodeId(serverReadPerformanceConfig.hostname, serverReadPerformanceConfig.port);
    ReplicaId replicaId =
        getReplicaFromNode(dataNodeId, getRequest.getPartitionInfoList().get(0).getPartition(), clusterMap);
    String hostname = dataNodeId.getHostname();
    Port port = dataNodeId.getPortToConnectTo();
    String identifier = hostname + ":" + port.getPort();
    RequestInfo requestInfo = new RequestInfo(hostname, port, getRequest, replicaId, null);
    return networkClient.sendAndPoll(Collections.singletonList(requestInfo), new HashSet<>(), 10);
  }

  /**
   * Get replica of given {@link PartitionId} from given {@link DataNodeId}. If partitionId is null, it returns any
   * replica on the certain node.
   * @param dataNodeId the {@link DataNodeId} on which replica resides.
   * @param partitionId the {@link PartitionId} which replica belongs to.
   * @return {@link ReplicaId} from given node.
   */
  public static ReplicaId getReplicaFromNode(DataNodeId dataNodeId, PartitionId partitionId, ClusterMap clusterMap) {
    ReplicaId replicaToReturn = null;
    if (partitionId != null) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        if (replicaId.getDataNodeId().getHostname().equals(dataNodeId.getHostname())) {
          replicaToReturn = replicaId;
          break;
        }
      }
    } else {
      // pick any replica on this node
      replicaToReturn = clusterMap.getReplicaIds(dataNodeId).get(0);
    }
    return replicaToReturn;
  }

  @Override
  public void close() throws IOException {
    executorService.shutdown();
    try {
      executorService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    networkClient.close();
  }
}
