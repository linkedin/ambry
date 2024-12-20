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
package com.github.ambry.tools.perf.serverperf;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Supports running performance tests on the Ambry server.
 * Supports performance testing for GET request, where it creates one thread
 * which queues requests in {@link ServerPerfNetworkQueue} , it creates second
 * thread which continuous polls and processes the responses currently in {@link ServerPerfNetworkQueue}
 *
 */

public class ServerPerformance {
  private final ServerPerfNetworkQueue networkQueue;
  private final ServerPerformanceConfig config;
  private final ClusterMap clusterMap;
  private final AtomicInteger correlationId = new AtomicInteger();
  Http2ClientMetrics clientMetrics;

  private final CountDownLatch timedShutDownLatch;

  private final CountDownLatch shutDownLatch;

  private static final String CLIENT_ID = "ServerReadPerformance";
  private static final Logger logger = LoggerFactory.getLogger(ServerPerformance.class);

  public static class ServerPerformanceConfig {
    /**
     * The path to the hardware layout file. Needed if using
     * {@link StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
    final String partitionLayoutFilePath;

    /**
     * maximum parallel network requests at a point of time
     */
    @Config("max.parallel.requests")
    @Default("20")
    final int maxParallelRequests;

    /**
     * Total number of network clients
     */
    @Config("network.clients.count")
    @Default("2")
    final int networkClientsCount;

    /**
     * Time after which to drop a request
     */
    @Config("operations.time.out.sec")
    @Default("15")
    final int operationsTimeOutSec;

    /**
     * Path to file from which to read the blob ids
     */
    @Config("blob.id.file.path")
    @Default("")
    final String blobIdFilePath;

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

    /**
     * Total time after which to stop the performance test
     */
    @Config("time.out.seconds")
    @Default("30")
    final int timeOutSeconds;

    ServerPerformanceConfig(VerifiableProperties verifiableProperties) {
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      blobIdFilePath = verifiableProperties.getString("blob.id.file.path", "");
      hostname = verifiableProperties.getString("hostname", "localhost");
      port = verifiableProperties.getInt("port", 6667);
      maxParallelRequests = verifiableProperties.getInt("max.parallel.requests", 20);
      networkClientsCount = verifiableProperties.getInt("network.clients.count", 2);
      timeOutSeconds = verifiableProperties.getInt("time.out.seconds", 30);
      operationsTimeOutSec = verifiableProperties.getInt("operations.time.out.sec", 15);
    }
  }

  public ServerPerformance(VerifiableProperties verifiableProperties) throws Exception {
    config = new ServerPerformanceConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    clusterMap = ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
        config.hardwareLayoutFilePath, config.partitionLayoutFilePath)).getClusterMap();
    clientMetrics = new Http2ClientMetrics(new MetricRegistry());
    timedShutDownLatch = new CountDownLatch(1);
    shutDownLatch = new CountDownLatch(1);
    networkQueue =
        new ServerPerfNetworkQueue(verifiableProperties, clientMetrics, new SystemTime(), config.maxParallelRequests,
            config.networkClientsCount, config.operationsTimeOutSec);
    networkQueue.start();
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    ServerPerformance serverPerformance = new ServerPerformance(verifiableProperties);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        logger.info("Starting the shutdown");
        serverPerformance.forceShutDown();
        serverPerformance.printMetrics();
      } catch (Exception e) {
        logger.error("Caught error while shut down", e);
      }
    }));
    serverPerformance.startGetLoadTest();
    System.exit(0);
  }

  /**
   * starts a thread to produce get requests
   * starts a thread to consume responses
   * starts a thread to start the shutdown at given time
   * When all threads are done, {@link #shutDownLatch} is lowered
   * so any waiting thread can continue
   * @throws Exception
   */
  void startGetLoadTest() throws Exception {
    Thread getLoadProducer = getGetLoadProducerThread();
    Thread getLoadConsumer = getGetLoadConsumerThread();
    Thread shutDownThread = getTimedShutDownThread();
    shutDownThread.start();
    getLoadProducer.start();
    getLoadConsumer.start();
    getLoadProducer.join();
    getLoadConsumer.join();
    shutDownLatch.countDown();
  }

  void printMetrics() {
    logger.info("HTTP2 error count {}", clientMetrics.http2NetworkErrorCount.getCount());
    logger.info("HTTP2 dropped request count {}", clientMetrics.http2RequestsToDropCount.getCount());
    logger.info("HTTP2 send Mean rate {}", clientMetrics.http2ClientSendRate.getMeanRate());
    logger.info("HTTP2 stream median read time {}",
        clientMetrics.http2StreamFirstToLastFrameTime.getSnapshot().getMedian());
    logger.info("HTTP2 stream median acquire time, {}",
        clientMetrics.http2FirstStreamAcquireTime.getSnapshot().getMedian());
  }

  /**
   * Waits until the {@link ServerPerformanceConfig#timeOutSeconds} to elapse
   * or is forced out of wait and starts the shutdown
   * @return
   */
  Thread getTimedShutDownThread() {
    return new Thread(() -> {
      try {
        timedShutDownLatch.await(config.timeOutSeconds, TimeUnit.SECONDS);
        logger.info("Timed shutdown triggerred");
        shutDown();
      } catch (Exception e) {
        logger.error("Caught exception in shutdown thread", e);
      }
    });
  }

  /**
   * Creates a thread which will keep creating requests
   * and submit to {@link #networkQueue}
   * @return {@link Thread}
   */
  Thread getGetLoadProducerThread() {
    return new Thread(() -> {
      boolean isShutDown = false;
      while (!isShutDown) {
        try {
          isShutDown = loadProducerGETBlob();
        } catch (Exception e) {
          logger.error("encountered error in loadProducer", e);
        }
      }
      logger.info("Load producer thread is finished");
    });
  }

  /**
   * Iterates over {@link ServerPerformanceConfig#blobIdFilePath}
   * and creates a {@link RequestInfo} for get requests and submits
   * to {@link #networkQueue}
   * @throws Exception exception
   */
  boolean loadProducerGETBlob() throws Exception {
    final BufferedReader br = new BufferedReader(new FileReader(config.blobIdFilePath));
    String line;
    boolean isShutDown = false;
    while ((line = br.readLine()) != null) {
      String[] id = line.split("\n");
      BlobId blobId = new BlobId(id[0], clusterMap);

      PartitionRequestInfo partitionRequestInfo =
          new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
      GetRequest getRequest = new GetRequest(correlationId.incrementAndGet(), CLIENT_ID, MessageFormatFlags.Blob,
          Collections.singletonList(partitionRequestInfo), GetOption.Include_All);
      DataNodeId dataNodeId = clusterMap.getDataNodeId(config.hostname, config.port);
      ReplicaId replicaId =
          getReplicaFromNode(dataNodeId, getRequest.getPartitionInfoList().get(0).getPartition(), clusterMap);
      String hostname = dataNodeId.getHostname();
      Port port = dataNodeId.getPortToConnectTo();
      RequestInfo requestInfo = new RequestInfo(hostname, port, getRequest, replicaId, null);
      logger.info("submitting the blob id {} to network queue correlation id {}", blobId,
          requestInfo.getRequest().getCorrelationId());
      try {
        networkQueue.submit(requestInfo);
      } catch (ServerPerfNetworkQueue.ShutDownException e) {
        isShutDown = true;
        break;
      }
    }
    br.close();
    return isShutDown;
  }

  /**
   * Creates a thread which will continuously try to
   * always poll the {@link #networkQueue} and process response received
   */
  Thread getGetLoadConsumerThread() {
    return new Thread(() -> {
      while (true) {
        try {
          networkQueue.poll(this::processGetResponse);
        } catch (ServerPerfNetworkQueue.ShutDownException e) {
          logger.info("Network queue is shutdown. Exiting from thread");
          break;
        } catch (Exception e) {
          logger.error("error in load consumer thread", e);
        }
      }
      logger.info("Load consumer thread is finished");
    });
  }

  /**
   * Parses the response info into {@link GetResponse}
   * gets the blob data from the response and puts it
   * @param responseInfo response info to process
   */
  void processGetResponse(ResponseInfo responseInfo) {
    try {
      if (responseInfo.getError() == NetworkClientErrorCode.TimeoutError) {
        logger.info("Timeout error for correlation id {}",
            responseInfo.getRequestInfo().getRequest().getCorrelationId());
        return;
      }
      InputStream serverResponseStream = new NettyByteBufDataInputStream(responseInfo.content());
      GetResponse getResponse = GetResponse.readFrom(new DataInputStream(serverResponseStream), clusterMap);
      ServerErrorCode partitionErrorCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      ServerErrorCode errorCode =
          partitionErrorCode == ServerErrorCode.No_Error ? getResponse.getError() : partitionErrorCode;
      InputStream stream = errorCode == ServerErrorCode.No_Error ? getResponse.getInputStream() : null;
      BlobData blobData = stream != null ? MessageFormatRecord.deserializeBlob(stream) : null;
      long blobDataSize = blobData != null ? blobData.getSize() : 0;
      responseInfo.release();
      getResponse.release();
      logger.info("blob id {} blob size {}  correlation id {}",
          getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getStoreKey(), blobDataSize,
          responseInfo.getRequestInfo().getRequest().getCorrelationId());
    } catch (Exception e) {
      logger.error("error in processing get response", e);
    }
  }

  private ReplicaId getReplicaFromNode(DataNodeId dataNodeId, PartitionId partitionId, ClusterMap clusterMap) {
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

  /**
   * forces the {@link #getTimedShutDownThread()} to wake up and
   * start the shutdown, waits until the shutdown is complete
   */
  public void forceShutDown() {
    timedShutDownLatch.countDown();
    try {
      shutDownLatch.await();
    } catch (Exception e) {
      logger.error("Error while waiting gor shutdown latch", e);
    }
  }

  /**
   * Shuts down the network client,
   * which will cause load producer thread to stop and load consumer threads to
   * stop after consuming the pending requests
   * @throws Exception
   */
  public void shutDown() throws Exception {
    networkQueue.shutDown();
  }
}
