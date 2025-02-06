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


public class PutLoadProducerConsumer implements LoadProducerConsumer {
  private final ServerPerfNetworkQueue networkQueue;
  private final ServerPerformanceConfig config;
  private final RouterConfig routerConfig;
  private final ClusterMap clusterMap;
  private final DataNodeId dataNodeId;

  private final List<ReplicaId> replicaIdsSelected;
  private final AtomicInteger correlationId;

  private int totalDataSentBytes;

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

  void selectReplica() {
    Random random = new Random();
    List<? extends ReplicaId> allReplicaIds = clusterMap.getReplicaIds(dataNodeId);

    Map<DiskId, List<ReplicaId>> diskIdToReplicaIds = new HashMap<>();
    allReplicaIds.forEach(replicaId -> {
      if (!replicaId.isUnsealed()) {
        return;
      }
      diskIdToReplicaIds.putIfAbsent(replicaId.getDiskId(), new ArrayList<>());
      diskIdToReplicaIds.get(replicaId.getDiskId()).add(replicaId);
    });

    diskIdToReplicaIds.values().forEach(replicaIds -> {
      replicaIdsSelected.add(replicaIds.get(random.nextInt(replicaIds.size())));
    });
  }

  @Override
  public void produce() throws Exception {
    while (true) {
      for (ReplicaId replicaId : replicaIdsSelected) {
        int blobSize = config.serverPerformancePutTestBlobSizeBytes;
        totalDataSentBytes = totalDataSentBytes + blobSize;
        if (totalDataSentBytes > config.serverPerformancePutTestDataLimitBytes) {
          throw new ShutDownException("Shut down producer as size limit for bytes reached");
        }

        byte[] blob = new byte[blobSize];
        byte[] usermetadata = new byte[new Random().nextInt(1024)];
        BlobProperties props =
            new BlobProperties(blobSize, CLIENT_ID, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false);
        props.setTimeToLiveInSeconds(config.serverPerformancePutTestBlobExpirySeconds);
        BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
            clusterMap.getLocalDatacenterId(), props.getAccountId(), props.getContainerId(), replicaId.getPartitionId(),
            false, BlobId.BlobDataType.DATACHUNK);

        PutRequest putRequest =
            new PutRequest(correlationId.incrementAndGet(), CLIENT_ID, blobId, props, ByteBuffer.wrap(usermetadata),
                Unpooled.wrappedBuffer(blob), props.getBlobSize(), BlobType.DataBlob, null);

        String hostname = dataNodeId.getHostname();
        Port port = dataNodeId.getPortToConnectTo();

        RequestInfo requestInfo = new RequestInfo(hostname, port, putRequest, replicaId, null);

        try {
          logger.info("Submitting put request {} , blob size {}", requestInfo.getRequest().getCorrelationId(), 4);
          networkQueue.submit(requestInfo);
        } catch (ShutDownException e) {
          throw e;
        } catch (Exception e) {
          logger.error("Error while sending request", e);
        }
      }
    }
  }

  @Override
  public void consume() throws Exception {
    try {
      networkQueue.poll(responseInfo -> {
        try {
          InputStream serverResponseStream = new NettyByteBufDataInputStream(responseInfo.content());
          PutResponse putResponse = PutResponse.readFrom(new DataInputStream(serverResponseStream));
          logger.info("received success response for correlation id {}",
              responseInfo.getRequestInfo().getRequest().getCorrelationId());
        } catch (Exception e) {
          logger.error("Error while processing response", e);
        }
      });
    } catch (ShutDownException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Error while consuming", e);
    }
  }
}
