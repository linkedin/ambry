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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.tools.perf.serverperf.ServerPerformance.ServerPerformanceConfig;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains the logic for producing PUT requests by selecting one replica per disk {@link #replicaIdsSelected}
 * in the given datanode {@link #dataNodeId} and submitting to the {@link #networkQueue}, until shutdown is triggered in
 * {@link #networkQueue} or bytes sent reaches limit {@link ServerPerformanceConfig#serverPerformancePutTestDataLimitBytes}.
 * <p>
 * Contains the consuming logic which will decode the responses received and prints these to logs.
 */
public class PutLoadProducerConsumer implements LoadProducerConsumer {
  private final ServerPerfNetworkQueue networkQueue;
  private final ServerPerformanceConfig config;
  private final RouterConfig routerConfig;
  private final ClusterMap clusterMap;
  private final DataNodeId dataNodeId;

  private final List<ReplicaId> replicaIdsSelected;
  private final AtomicInteger correlationId;

  private long totalDataSentBytes;

  private static final String CLIENT_ID = "ServerPUTPerformance";

  private static final Logger logger = LoggerFactory.getLogger(PutLoadProducerConsumer.class);

  public PutLoadProducerConsumer(ServerPerfNetworkQueue networkQueue, ServerPerformanceConfig config,
      ClusterMap clusterMap, RouterConfig routerConfig) {
    this.networkQueue = networkQueue;
    this.config = config;
    this.clusterMap = clusterMap;
    this.routerConfig = routerConfig;
    replicaIdsSelected = new ArrayList<>();
    dataNodeId = clusterMap.getDataNodeId(config.serverPerformanceHostname, config.serverPerformancePort);
    selectReplica();
    correlationId = new AtomicInteger();
    totalDataSentBytes = 0;
  }

  /**
   * Selects {@link ServerPerformanceConfig#serverPerformancePutTestPartitionCountPerDisk} unsealed replicas randomly for
   * each disk in the {@link #dataNodeId} and stores these in {@link #replicaIdsSelected}
   */
  void selectReplica() {
    Random random = new Random();
    List<? extends ReplicaId> allReplicaIds = clusterMap.getReplicaIds(dataNodeId);

    Map<DiskId, List<ReplicaId>> diskIdToReplicaIds = new HashMap<>();
    allReplicaIds.forEach(replicaId -> {
      diskIdToReplicaIds.putIfAbsent(replicaId.getDiskId(), new ArrayList<>());
      if (!replicaId.isUnsealed()) {
        return;
      }
      diskIdToReplicaIds.get(replicaId.getDiskId()).add(replicaId);
    });

    diskIdToReplicaIds.keySet().forEach(diskId -> {
      List<ReplicaId> replicaIds = diskIdToReplicaIds.get(diskId);

      if (replicaIds.size() < config.serverPerformancePutTestPartitionCountPerDisk) {
        throw new IllegalArgumentException("disk does not have enough replicas " + diskId);
      }

      for (int i = 0; i < config.serverPerformancePutTestPartitionCountPerDisk; i++) {
        int randomIndex = random.nextInt(replicaIds.size());
        replicaIdsSelected.add(replicaIds.get(randomIndex));
        replicaIds.set(randomIndex, replicaIds.get(replicaIds.size() - 1));
        replicaIds.remove(replicaIds.size() - 1);
      }
    });
  }

  /**
   * Creates the PUT requests with blob size {@link ServerPerformanceConfig#serverPerformancePutTestBlobSizeBytes}
   * for rach replicas in {@link #replicaIdsSelected} in round-robin format and submits
   * requests to {@link #networkQueue}
   *
   * This will throw {@link ShutDownException} if total bytes sent exceeds
   * {@link ServerPerformanceConfig#serverPerformancePutTestBlobSizeBytes}
   * or it encounters {@link ShutDownException} from {@link #networkQueue}.
   *
   * @throws ShutDownException shutdown exception
   * @throws Exception exception
   */
  @Override
  public void produce() throws Exception {
    while (true) {
      for (ReplicaId replicaId : replicaIdsSelected) {
        int blobSize = config.serverPerformancePutTestBlobSizeBytes;
        totalDataSentBytes = totalDataSentBytes + blobSize;

        if (totalDataSentBytes > config.serverPerformancePutTestDataLimitBytes) {
          logger.info("Shutting down producer as total limit for bytes reached {}",
              config.serverPerformancePutTestDataLimitBytes);
          throw new ShutDownException();
        }

        byte[] blob = new byte[blobSize];
        byte[] userMetaData = new byte[new Random().nextInt(1024)];
        BlobProperties props =
            new BlobProperties(blobSize, CLIENT_ID, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false);
        props.setTimeToLiveInSeconds(config.serverPerformancePutTestBlobExpirySeconds);
        BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
            clusterMap.getLocalDatacenterId(), props.getAccountId(), props.getContainerId(), replicaId.getPartitionId(),
            false, BlobId.BlobDataType.DATACHUNK);

        PutRequest putRequest =
            new PutRequest(correlationId.incrementAndGet(), CLIENT_ID, blobId, props, ByteBuffer.wrap(userMetaData),
                Unpooled.wrappedBuffer(blob), props.getBlobSize(), BlobType.DataBlob, null);

        String hostname = dataNodeId.getHostname();
        Port port = dataNodeId.getPortToConnectTo();

        RequestInfo requestInfo = new RequestInfo(hostname, port, putRequest, replicaId, null);

        try {
          logger.info("Submitting put request correlationId {}, blob id {} , blob size {}",
              requestInfo.getRequest().getCorrelationId(), blobId, blobSize);
          networkQueue.submit(requestInfo);
        } catch (ShutDownException e) {
          logger.info("Shutting down producer as Shutdown exception received");
          throw e;
        } catch (Exception e) {
          logger.error("Error while sending request", e);
        }
      }
    }
  }

  /**
   * Polls {@link #networkQueue} and passes the function which decodes response
   * @throws ShutDownException if it encounters {@link ShutDownException}
   */
  @Override
  public void consume() throws Exception {
    try {
      networkQueue.poll(responseInfo -> {
        PutRequest putRequest = (PutRequest) responseInfo.getRequestInfo().getRequest();
        BlobId blobId = putRequest.getBlobId();
        int correlationId = putRequest.getCorrelationId();

        try {
          if (responseInfo.getError() != null) {
            logger.info("Error for correlation id {} blob id {} {} ", correlationId, blobId, responseInfo.getError());
            return;
          }
          InputStream serverResponseStream = new NettyByteBufDataInputStream(responseInfo.content());
          PutResponse putResponse = PutResponse.readFrom(new DataInputStream(serverResponseStream));
          responseInfo.release();
          putResponse.release();
          logger.info("received success response and decoded response successfully for correlation id {} {}",
              correlationId, blobId);
        } catch (Exception e) {
          logger.error("Error while processing response", e);
        }
      });
    } catch (ShutDownException e) {
      logger.info("Shutting down consumer as shutdown exception received");
      throw e;
    } catch (Exception e) {
      logger.error("Error while consuming", e);
    }
  }
}
