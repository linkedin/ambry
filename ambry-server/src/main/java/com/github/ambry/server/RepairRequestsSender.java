/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.server;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.network.LocalNetworkClient;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.ReplicateBlobRequest;
import com.github.ambry.protocol.ReplicateBlobResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.repair.RepairRequestRecord;
import com.github.ambry.repair.RepairRequestsDb;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RepairRequestsSender gets the records from the AmbryRepairRequests database.
 * And then generate ODR requests and send them to the request handlers.
 * If the requests are executed successfully, it removes the records from the database.
 */
class RepairRequestsSender implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(RepairRequestsSender.class);
  private static final String SERVICE_ID = "RepairRequestsSender";
  // ODR can take some time to execute due to remote call to the peer replica.
  private static int POLL_TIMEOUT_MS = 10000;
  private static int SLEEP_TIME_MS = 5000;

  // the channel to talk to the requests handler
  private final RequestResponseChannel requestChannel;
  // network client to send the requests and poll the responses
  private final LocalNetworkClient client;
  // cluster map
  private final ClusterMap clusterMap;
  // this data node
  private final DataNodeId nodeId;
  // LOCAL_CONSISTENCY_TODO. May listen to the Helix to get updated partition lists.
  private final ClusterParticipant clusterParticipant;
  // The database which stores the RepairRequestRecord
  private final RepairRequestsDb db;
  // partition id list this data node gets assigned
  private final List<Long> partitionIds;
  // Hashmap from partition id to the replicas
  private final Map<Long, ReplicaId> partition2Replicas;
  // generator to generate the correlation ids.
  private final AtomicInteger correlationIdGenerator = new AtomicInteger(0);
  // max result sets it can get from each query
  private final int maxResults;
  // shutdown flag
  private volatile boolean shutdown = false;

  /**
   * RepairRequestsSender constructor
   * @param requestChannel the channel to send requests and receive responses
   * @param factory the {@link LocalNetworkClientFactory} used to generate the {@link LocalNetworkClient}
   * @param clusterMap the cluster map
   * @param nodeId this {@link DataNodeId}
   * @param repairRequestsDb the database to get the RepairRequestRecord
   * @param clusterParticipant {@link ClusterParticipant}
   */
  public RepairRequestsSender(RequestResponseChannel requestChannel, LocalNetworkClientFactory factory,
      ClusterMap clusterMap, DataNodeId nodeId, RepairRequestsDb repairRequestsDb,
      ClusterParticipant clusterParticipant) {
    this.requestChannel = requestChannel;
    this.client = factory.getNetworkClient();
    this.clusterMap = clusterMap;
    this.nodeId = nodeId;
    this.clusterParticipant = clusterParticipant;

    this.db = repairRequestsDb;
    this.maxResults = db.getListMaxResults();

    // LOCAL_CONSISTENCY_TODO. listen to the clustermap change and make the modification accordingly.
    partitionIds = new ArrayList<>();
    partition2Replicas = new HashMap<>();
    for (ReplicaId replica : clusterMap.getReplicaIds(nodeId)) {
      long partition = replica.getPartitionId().getId();
      partitionIds.add(partition);
      partition2Replicas.put(partition, replica);
    }
  }

  public void run() {
    List<RequestInfo> requestInfos = null;

    while (!shutdown) {
      try {
        // sleep for some time
        Thread.sleep(SLEEP_TIME_MS);

        boolean hasMore = true;
        boolean hasError = false;
        // if some partitions have more requests, continue to handle them.
        while (hasMore && !hasError) {
          hasMore = false;
          hasError = false;
          // loop all the partitions. For each partition, we handle maxResults number of requests.
          for (long partitionId : partitionIds) {
            if (shutdown) {
              return;
            }
            // get the repair requests for this partition
            requestInfos = getAmbryRequest(partitionId);
            if (requestInfos.isEmpty()) {
              continue;
            }
            // this partition may have more requests.
            if (requestInfos.size() >= maxResults) {
              hasMore = true;
            }

            Set<Integer> requestsToDrop = new HashSet<>();
            // send the repair requests to the handler and wait for the responses.
            while (!requestInfos.isEmpty()) {
              RequestInfo reqInfo = requestInfos.get(requestInfos.size() - 1);
              List<ResponseInfo> responses =
                  client.sendAndPoll(Collections.singletonList(reqInfo), requestsToDrop, POLL_TIMEOUT_MS);
              if (responses == null || responses.size() == 0) {
                logger.error("RepairRequests Sender: timeout waiting for repairs {} {}", nodeId, reqInfo.getRequest());
                hasError = true;
                break;
              }
              ResponseInfo resInfo = responses.get(0);
              ReplicateBlobResponse res = (ReplicateBlobResponse) resInfo.getResponse();
              ReplicateBlobRequest req = (ReplicateBlobRequest) resInfo.getRequestInfo().getRequest();
              if (res.getError() == ServerErrorCode.No_Error) {
                RepairRequestRecord.OperationType type;
                if (req.getOperationType() == RequestOrResponseType.TtlUpdateRequest) {
                  type = RepairRequestRecord.OperationType.TtlUpdateRequest;
                } else {
                  type = RepairRequestRecord.OperationType.DeleteRequest;
                }
                db.removeRepairRequests(req.getBlobId().toString(), type);
                logger.info("RepairRequests Sender: Repaired {}", req);
              } else {
                // LOCAL_CONSISTENCY_TODO: metrics for all the errors happens in RepairRequestsSender.
                // Ideally we may need create alert if we cannot repair the requests in timely matter.
                logger.error("RepairRequests Sender: failed to repair {} response {}", req, res);
                hasError = true;
              }
              requestInfos.remove(requestInfos.size() - 1);
            }
            requestInfos.forEach(ri -> ri.getRequest().release());
            requestInfos = null;
          } // loop all the partitions
        }  // loop until all the partitions are clean
      } catch (Throwable e) {
        // LOCAL_CONSISTENCY_TODO add metric to track background threads
        logger.error("RepairRequests Sender: Exception when handling request", e);
      } finally {
        if (requestInfos != null) {
          requestInfos.forEach(ri -> ri.getRequest().release());
          requestInfos = null;
        }
      }
    }
  }

  /**
   * Get the {@link RepairRequestRecord} from the database and generate {@link ReplicateBlobRequest}
   * @param partitionId the partition id
   * @return list of {@link RequestInfo}
   */
  private List<RequestInfo> getAmbryRequest(long partitionId) throws Exception {
    List<RequestInfo> requestInfos = new ArrayList<>();
    // get the repair requests for this partition and this node is not the source replica.
    List<RepairRequestRecord> records =
        db.getRepairRequestsExcludingHost(partitionId, nodeId.getHostname(), nodeId.getPort());

    for (RepairRequestRecord record : records) {
      BlobId blobId;
      try {
        blobId = new BlobId(record.getBlobId(), clusterMap);
      } catch (IOException e) {
        // new BlobId may generate IOException
        // LOCAL_CONSISTENCY_TODO add metrics for RepairRequestsSender class
        logger.error("RepairRequests Sender: Failed to generate Blob ID {}", record, e);
        continue;
      }

      ReplicaId replicaId = partition2Replicas.get(partitionId);
      if (replicaId == null) {
        // LOCAL_CONSISTENCY_TODO add metrics for RepairRequestsSender class
        logger.error("RepairRequests Sender: Replica is null {} {} {}", partitionId, record, replicaId);
        continue;
      }

      RequestOrResponseType type;
      if (record.getOperationType() == RepairRequestRecord.OperationType.DeleteRequest) {
        type = RequestOrResponseType.DeleteRequest;
      } else if (record.getOperationType() == RepairRequestRecord.OperationType.TtlUpdateRequest) {
        type = RequestOrResponseType.TtlUpdateRequest;
      } else {
        logger.error("RepairRequests Sender: Un-supported repair operation type " + record);
        continue;
      }

      if (record.getSourceHostName().equals(nodeId.getHostname()) && record.getSourceHostPort() == nodeId.getPort()) {
        // shouldn't happen
        String errorMsg =
            "RepairRequests Sender: Shouldn't get the repair requests of which this node is the source replica, "
                + record;
        logger.error(errorMsg);
        throw new Exception(errorMsg);
      }

      int correlationId = correlationIdGenerator.incrementAndGet();
      ReplicateBlobRequest request =
          new ReplicateBlobRequest(correlationId, SERVICE_ID, blobId, record.getSourceHostName(),
              record.getSourceHostPort(), type, record.getOperationTimeMs(), record.getLifeVersion(),
              record.getExpirationTimeMs());
      RequestInfo requestInfo =
          new RequestInfo(nodeId.getHostname(), nodeId.getPortToConnectTo(), request, replicaId, null);
      requestInfos.add(requestInfo);
    }

    return requestInfos;
  }

  public void shutdown() {
    shutdown = true;
  }
}
