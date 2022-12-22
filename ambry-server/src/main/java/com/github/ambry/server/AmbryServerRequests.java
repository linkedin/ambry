/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkRequest;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.AdminResponseWithContent;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.protocol.BlobIndexAdminRequest;
import com.github.ambry.protocol.BlobStoreControlAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminResponse;
import com.github.ambry.protocol.ReplicationControlAdminRequest;
import com.github.ambry.protocol.RequestControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicationAPI;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.DiskHealthStatus;
import com.github.ambry.store.DiskManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreKeyJacksonConfig;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.server.ServerErrorCode.*;


/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */
public class AmbryServerRequests extends AmbryRequests {
  // POST requests are allowed on stores states: { LEADER, STANDBY }
  static final Set<ReplicaState> PUT_ALLOWED_STORE_STATES = EnumSet.of(ReplicaState.LEADER, ReplicaState.STANDBY);
  // UPDATE requests (including DELETE, TTLUpdate, UNDELETE) are allowed on stores states: { LEADER, STANDBY, INACTIVE, BOOTSTRAP }
  static final Set<ReplicaState> UPDATE_ALLOWED_STORE_STATES =
      EnumSet.of(ReplicaState.LEADER, ReplicaState.STANDBY, ReplicaState.INACTIVE, ReplicaState.BOOTSTRAP);
  static final Set<RequestOrResponseType> UPDATE_REQUEST_TYPES =
      EnumSet.of(RequestOrResponseType.DeleteRequest, RequestOrResponseType.TtlUpdateRequest,
          RequestOrResponseType.UndeleteRequest);
  private static final Logger logger = LoggerFactory.getLogger(AmbryServerRequests.class);
  private final DiskManagerConfig diskManagerConfig;
  private final StatsManager statsManager;
  private final ClusterParticipant clusterParticipant;
  private final ConcurrentHashMap<RequestOrResponseType, Set<PartitionId>> requestsDisableInfo =
      new ConcurrentHashMap<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  AmbryServerRequests(StoreManager storeManager, RequestResponseChannel requestResponseChannel, ClusterMap clusterMap,
      DataNodeId nodeId, MetricRegistry registry, ServerMetrics serverMetrics, FindTokenHelper findTokenHelper,
      NotificationSystem operationNotification, ReplicationAPI replicationEngine, StoreKeyFactory storeKeyFactory,
      ServerConfig serverConfig, DiskManagerConfig diskManagerConfig, StoreKeyConverterFactory storeKeyConverterFactory,
      StatsManager statsManager, ClusterParticipant clusterParticipant) {
    this(storeManager, requestResponseChannel, clusterMap, nodeId, registry, serverMetrics, findTokenHelper,
        operationNotification, replicationEngine, storeKeyFactory, serverConfig, diskManagerConfig,
        storeKeyConverterFactory, statsManager, clusterParticipant, null);
  }

  AmbryServerRequests(StoreManager storeManager, RequestResponseChannel requestResponseChannel, ClusterMap clusterMap,
      DataNodeId nodeId, MetricRegistry registry, ServerMetrics serverMetrics, FindTokenHelper findTokenHelper,
      NotificationSystem operationNotification, ReplicationAPI replicationEngine, StoreKeyFactory storeKeyFactory,
      ServerConfig serverConfig, DiskManagerConfig diskManagerConfig, StoreKeyConverterFactory storeKeyConverterFactory,
      StatsManager statsManager, ClusterParticipant clusterParticipant, ConnectionPool connectionPool) {
    super(storeManager, requestResponseChannel, clusterMap, nodeId, registry, serverMetrics, findTokenHelper,
        operationNotification, replicationEngine, storeKeyFactory, storeKeyConverterFactory, connectionPool,
        serverConfig);
    this.diskManagerConfig = diskManagerConfig;
    this.statsManager = statsManager;
    this.clusterParticipant = clusterParticipant;

    for (RequestOrResponseType requestType : EnumSet.of(RequestOrResponseType.PutRequest,
        RequestOrResponseType.GetRequest, RequestOrResponseType.DeleteRequest, RequestOrResponseType.UndeleteRequest,
        RequestOrResponseType.ReplicaMetadataRequest, RequestOrResponseType.TtlUpdateRequest)) {
      requestsDisableInfo.put(requestType, Collections.newSetFromMap(new ConcurrentHashMap<>()));
    }
    StoreKeyJacksonConfig.setupObjectMapper(objectMapper, new BlobIdFactory(clusterMap));
  }

  /**
   * @param requestType the {@link RequestOrResponseType} of the request.
   * @param id the partition id that the request is targeting.
   * @return {@code true} if the request is enabled. {@code false} otherwise.
   */
  private boolean isRequestEnabled(RequestOrResponseType requestType, PartitionId id) {
    if (requestType.equals(RequestOrResponseType.UndeleteRequest) && !serverConfig.serverHandleUndeleteRequestEnabled) {
      return false;
    }
    Set<PartitionId> requestDisableInfo = requestsDisableInfo.get(requestType);
    // 1. check if request is disabled by admin request
    if (requestDisableInfo != null && requestDisableInfo.contains(id)) {
      return false;
    }
    if (serverConfig.serverValidateRequestBasedOnStoreState) {
      // 2. check if request is disabled due to current state of store
      Store store = storeManager.getStore(id);
      if (requestType == RequestOrResponseType.PutRequest && !PUT_ALLOWED_STORE_STATES.contains(
          store.getCurrentState())) {
        logger.warn("{} is not allowed because current state of store {} is {}", requestType, id,
            store.getCurrentState());
        return false;
      }
      if (UPDATE_REQUEST_TYPES.contains(requestType) && !UPDATE_ALLOWED_STORE_STATES.contains(
          store.getCurrentState())) {
        logger.warn("{} is not allowed because current state of store {} is {}", requestType, id,
            store.getCurrentState());
        return false;
      }
    }
    return true;
  }

  /**
   * Enables/disables {@code requestOrResponseType} on the given {@code ids}.
   * @param requestTypes the {@link RequestOrResponseType} to enable/disable.
   * @param ids the {@link PartitionId}s to enable/disable it on.
   * @param enable whether to enable ({@code true}) or disable
   */
  private void controlRequestForPartitions(EnumSet<RequestOrResponseType> requestTypes, Collection<PartitionId> ids,
      boolean enable) {
    for (RequestOrResponseType requestType : requestTypes) {
      if (enable) {
        requestsDisableInfo.get(requestType).removeAll(ids);
      } else {
        requestsDisableInfo.get(requestType).addAll(ids);
      }
    }
  }

  /**
   * Handles an administration request. These requests can query for or change the internal state of the server.
   * @param request the request that needs to be handled.
   * @throws InterruptedException if response sending is interrupted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   */
  @Override
  public void handleAdminRequest(NetworkRequest request) throws InterruptedException, IOException {
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    long startTime = SystemTime.getInstance().milliseconds();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    AdminRequest adminRequest = AdminRequest.readFrom(requestStream, clusterMap);
    Histogram responseQueueTimeHistogram;
    Histogram responseSendTimeHistogram;
    Histogram requestTotalTimeHistogram;
    AdminResponse response = null;
    try {
      switch (adminRequest.getType()) {
        case TriggerCompaction:
          response = handleTriggerCompactionRequest(adminRequest);
          break;
        case RequestControl:
          response = handleRequestControlRequest(requestStream, adminRequest);
          break;
        case ReplicationControl:
          response = handleReplicationControlRequest(requestStream, adminRequest);
          break;
        case CatchupStatus:
          response = handleCatchupStatusRequest(requestStream, adminRequest);
          break;
        case BlobStoreControl:
          response = handleBlobStoreControlRequest(requestStream, adminRequest);
          break;
        case HealthCheck:
          response = handleHealthCheckRequest(requestStream, adminRequest);
          break;
        case BlobIndex:
          response = handleBlobIndexRequest(requestStream, adminRequest);
          break;
      }
    } catch (Exception e) {
      logger.error("Unknown exception for admin request {}", adminRequest, e);
      metrics.unExpectedAdminOperationError.inc();
      response =
          new AdminResponse(adminRequest.getCorrelationId(), adminRequest.getClientId(), ServerErrorCode.Unknown_Error);
      switch (adminRequest.getType()) {
        case CatchupStatus:
          response = new CatchupStatusAdminResponse(false, response);
          break;
        case HealthCheck:
        case BlobIndex:
          response = new AdminResponseWithContent(adminRequest.getCorrelationId(), adminRequest.getClientId(),
              ServerErrorCode.Unknown_Error);
          break;
      }
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", adminRequest, response, processingTime);
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater = new RequestMetricsUpdater(requestQueueTime, processingTime, 0, 0, false);
      adminRequest.accept(metricsUpdater);
      responseQueueTimeHistogram = metricsUpdater.getResponseQueueTimeHistogram();
      responseSendTimeHistogram = metricsUpdater.getResponseSendTimeHistogram();
      requestTotalTimeHistogram = metricsUpdater.getRequestTotalTimeHistogram();
    }
    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(responseQueueTimeHistogram, responseSendTimeHistogram,
            requestTotalTimeHistogram, null, null, totalTimeSpent));
  }

  /**
   * Handles {@link com.github.ambry.protocol.AdminRequestOrResponseType#TriggerCompaction}.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponse} to the request.
   */
  private AdminResponse handleTriggerCompactionRequest(AdminRequest adminRequest) {
    ServerErrorCode error = validateRequest(adminRequest.getPartitionId(), RequestOrResponseType.AdminRequest, false);
    if (error != ServerErrorCode.No_Error) {
      logger.error("Validating trigger compaction request failed with error {} for {}", error, adminRequest);
    } else if (!storeManager.scheduleNextForCompaction(adminRequest.getPartitionId())) {
      error = ServerErrorCode.Unknown_Error;
      logger.error("Triggering compaction failed for {}. Check if admin trigger is enabled for compaction",
          adminRequest);
    }
    return new AdminResponse(adminRequest.getCorrelationId(), adminRequest.getClientId(), error);
  }

  /**
   * Handles {@link com.github.ambry.protocol.AdminRequestOrResponseType#RequestControl}.
   * @param requestStream the serialized bytes of the request.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponse} to the request.
   * @throws IOException if there is any I/O error reading from the {@code requestStream}.
   */
  private AdminResponse handleRequestControlRequest(DataInputStream requestStream, AdminRequest adminRequest)
      throws IOException {
    RequestControlAdminRequest controlRequest = RequestControlAdminRequest.readFrom(requestStream, adminRequest);
    RequestOrResponseType toControl = controlRequest.getRequestTypeToControl();
    ServerErrorCode error;
    Collection<PartitionId> partitionIds;
    if (!requestsDisableInfo.containsKey(toControl)) {
      metrics.badRequestError.inc();
      error = ServerErrorCode.Bad_Request;
    } else {
      error = ServerErrorCode.No_Error;
      if (controlRequest.getPartitionId() != null) {
        error = validateRequest(controlRequest.getPartitionId(), RequestOrResponseType.AdminRequest, false);
        partitionIds = Collections.singletonList(controlRequest.getPartitionId());
      } else {
        partitionIds = storeManager.getLocalPartitions();
      }
      if (!error.equals(ServerErrorCode.Partition_Unknown)) {
        controlRequestForPartitions(EnumSet.of(toControl), partitionIds, controlRequest.shouldEnable());
        for (PartitionId partitionId : partitionIds) {
          logger.info("Enable state for {} on {} is {}", toControl, partitionId,
              isRequestEnabled(toControl, partitionId));
        }
      }
    }
    return new AdminResponse(adminRequest.getCorrelationId(), adminRequest.getClientId(), error);
  }

  /**
   * Handles {@link com.github.ambry.protocol.AdminRequestOrResponseType#ReplicationControl}.
   * @param requestStream the serialized bytes of the request.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponse} to the request.
   * @throws IOException if there is any I/O error reading from the {@code requestStream}.
   */
  private AdminResponse handleReplicationControlRequest(DataInputStream requestStream, AdminRequest adminRequest)
      throws IOException {
    Collection<PartitionId> partitionIds;
    ServerErrorCode error = ServerErrorCode.No_Error;
    ReplicationControlAdminRequest replControlRequest =
        ReplicationControlAdminRequest.readFrom(requestStream, adminRequest);
    if (replControlRequest.getPartitionId() != null) {
      error = validateRequest(replControlRequest.getPartitionId(), RequestOrResponseType.AdminRequest, false);
      partitionIds = Collections.singletonList(replControlRequest.getPartitionId());
    } else {
      partitionIds = storeManager.getLocalPartitions();
    }
    if (!error.equals(ServerErrorCode.Partition_Unknown)) {
      if (replicationEngine.controlReplicationForPartitions(partitionIds, replControlRequest.getOrigins(),
          replControlRequest.shouldEnable())) {
        error = ServerErrorCode.No_Error;
      } else {
        logger.error("Could not set enable status for replication of {} from {} to {}. Check partition validity and"
            + " origins list", partitionIds, replControlRequest.getOrigins(), replControlRequest.shouldEnable());
        error = ServerErrorCode.Bad_Request;
      }
    }
    return new AdminResponse(adminRequest.getCorrelationId(), adminRequest.getClientId(), error);
  }

  /**
   * Handles {@link com.github.ambry.protocol.AdminRequestOrResponseType#CatchupStatus}.
   * @param requestStream the serialized bytes of the request.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponse} to the request.
   * @throws IOException if there is any I/O error reading from the {@code requestStream}.
   */
  private AdminResponse handleCatchupStatusRequest(DataInputStream requestStream, AdminRequest adminRequest)
      throws IOException {
    Collection<PartitionId> partitionIds;
    ServerErrorCode error = ServerErrorCode.No_Error;
    boolean isCaughtUp = false;
    CatchupStatusAdminRequest catchupStatusRequest = CatchupStatusAdminRequest.readFrom(requestStream, adminRequest);
    if (catchupStatusRequest.getAcceptableLagInBytes() < 0) {
      error = ServerErrorCode.Bad_Request;
    } else if (catchupStatusRequest.getNumReplicasCaughtUpPerPartition() <= 0) {
      error = ServerErrorCode.Bad_Request;
    } else {
      if (catchupStatusRequest.getPartitionId() != null) {
        error = validateRequest(catchupStatusRequest.getPartitionId(), RequestOrResponseType.AdminRequest, false);
        partitionIds = Collections.singletonList(catchupStatusRequest.getPartitionId());
      } else {
        partitionIds = storeManager.getLocalPartitions();
      }
      if (!error.equals(ServerErrorCode.Partition_Unknown)) {
        error = ServerErrorCode.No_Error;
        isCaughtUp = isRemoteLagLesserOrEqual(partitionIds, catchupStatusRequest.getAcceptableLagInBytes(),
            catchupStatusRequest.getNumReplicasCaughtUpPerPartition());
      }
    }
    AdminResponse adminResponse = new AdminResponse(adminRequest.getCorrelationId(), adminRequest.getClientId(), error);
    return new CatchupStatusAdminResponse(isCaughtUp, adminResponse);
  }

  /**
   * Handles {@link com.github.ambry.protocol.AdminRequestOrResponseType#BlobStoreControl}.
   * @param requestStream the serialized bytes of the request.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponse} to the request.
   * @throws IOException if there is any I/O error reading from the {@code requestStream}.
   */
  private AdminResponse handleBlobStoreControlRequest(DataInputStream requestStream, AdminRequest adminRequest)
      throws StoreException, IOException {
    ServerErrorCode error;
    BlobStoreControlAdminRequest blobStoreControlAdminRequest =
        BlobStoreControlAdminRequest.readFrom(requestStream, adminRequest);
    PartitionId partitionId = blobStoreControlAdminRequest.getPartitionId();
    if (partitionId != null) {
      switch (blobStoreControlAdminRequest.getStoreControlAction()) {
        case StopStore:
          short numReplicasCaughtUpPerPartition = blobStoreControlAdminRequest.getNumReplicasCaughtUpPerPartition();
          logger.info(
              "Handling stop store request and {} replica(s) per partition should catch up before stopping store {}",
              numReplicasCaughtUpPerPartition, partitionId);
          error = handleStopStoreRequest(partitionId, numReplicasCaughtUpPerPartition);
          break;
        case StartStore:
          logger.info("Handling start store request on {}", partitionId);
          error = handleStartStoreRequest(partitionId);
          break;
        case AddStore:
          logger.info("Handling add store request for {}", partitionId);
          error = handleAddStoreRequest(partitionId);
          break;
        case RemoveStore:
          logger.info("Handling remove store request on {}", partitionId);
          error = handleRemoveStoreRequest(partitionId);
          break;
        default:
          throw new IllegalArgumentException(
              "NetworkRequest type not supported: " + blobStoreControlAdminRequest.getStoreControlAction());
      }
    } else {
      error = ServerErrorCode.Bad_Request;
      logger.debug("The partition Id should not be null.");
    }
    return new AdminResponse(adminRequest.getCorrelationId(), adminRequest.getClientId(), error);
  }

  /**
   * Handles {@link com.github.ambry.protocol.AdminRequestOrResponseType#HealthCheck}.
   * @param requestStream the serialized bytes of the request.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponse} to the request.
   * @throws IOException if there is any I/O error reading from the {@code requestStream}.
   */
  private AdminResponse handleHealthCheckRequest(DataInputStream requestStream, AdminRequest adminRequest) {
    ServerHealthStatus serverHealthStatus = this.storeManager instanceof StorageManager ? ServerHealthStatus.GOOD
        : ServerHealthStatus.BAD; //used to determine if the server is ever unhealthy
    JSONArray brokenDisks = new JSONArray();        // indicate which disks didn't pass the disk healthcheck
    JSONArray unstablePartitions = new JSONArray(); // indicate which partitions aren't in leader/standby state

    if (this.storeManager instanceof StorageManager) {
      StorageManager storageManager = (StorageManager) this.storeManager;

      //Finds all partitions on this host
      List<PartitionId> partitionsInThisHost = Collections.list(storageManager.getPartitionToDiskManager().keys());

      /*
       Checks if the Host's BlobStore exists,started and is Leader/Standby ReplicaState for all Partitions
       this Host is apart of
       */
      for (PartitionId partitionId : partitionsInThisHost) {
        BlobStore hostBlobStore = (BlobStore) storageManager.getStore(partitionId);

        //if any fail, this host isn't healthy
        if (hostBlobStore == null || !hostBlobStore.isStarted() || (
            (hostBlobStore.getCurrentState() != ReplicaState.STANDBY) && (hostBlobStore.getCurrentState()
                != ReplicaState.LEADER))) {

          JSONObject unstablePartition = new JSONObject();
          unstablePartition.put("partitionId", partitionId);
          unstablePartition.put("reason", hostBlobStore != null ? hostBlobStore.getCurrentState() : "Missing");
          unstablePartitions.put(unstablePartition);
        }
      }

      /*
       Checks if every disk that's assigned to a partition is also mounted and disk is healthy
       */
      if (diskManagerConfig.diskManagerDiskHealthCheckEnabled) {
        HashSet<DiskManager> partitionsDiskManagers =
            new HashSet<>(storageManager.getPartitionToDiskManager().values());
        for (Map.Entry<DiskId, DiskManager> entry : storageManager.getDiskToDiskManager().entrySet()) {
          DiskManager mountedDiskManager = entry.getValue();

          //Checks Disk healthchecking is enabled and
          // if a disk manager is assigned to a partition and isn't healthy then store it as part of the response
          if (partitionsDiskManagers.contains(mountedDiskManager)
              && mountedDiskManager.getDiskHealthStatus() != DiskHealthStatus.HEALTHY) {

            JSONObject brokenDisk = new JSONObject();
            brokenDisk.put("disk", entry.getKey().getMountPath());
            brokenDisk.put("reason", mountedDiskManager.getDiskHealthStatus());
            brokenDisks.put(brokenDisk);
          }
        }
      }
    }
    //if there are any issues on the partition or disk state then considered BAD
    serverHealthStatus =
        unstablePartitions.length() > 0 || brokenDisks.length() > 0 ? ServerHealthStatus.BAD : serverHealthStatus;

    //Initializing the content of the response as a json
    JSONObject contentJSON = new JSONObject();
    contentJSON.put("health", serverHealthStatus);
    contentJSON.put("brokenDisks", brokenDisks);
    contentJSON.put("unstablePartitions", unstablePartitions);

    byte[] content = contentJSON.toString().getBytes(StandardCharsets.UTF_8);
    ServerErrorCode error = ServerErrorCode.No_Error;
    return new AdminResponseWithContent(adminRequest.getCorrelationId(), adminRequest.getClientId(), error, content);
  }

  /**
   * Handles {@link BlobIndexAdminRequest}.
   * @param requestStream the serialized bytes of the request.
   * @param adminRequest the {@link AdminRequest} received.
   * @return the {@link AdminResponseWithContent} to the request.
   */
  private AdminResponse handleBlobIndexRequest(DataInputStream requestStream, AdminRequest adminRequest) {
    final int correlationId = adminRequest.getCorrelationId();
    final String clientId = adminRequest.getClientId();
    BlobIndexAdminRequest blobIndexAdminRequest = null;
    String errorMessage = null;
    try {
      blobIndexAdminRequest = BlobIndexAdminRequest.readFrom(requestStream, adminRequest, storeKeyFactory);
    } catch (Exception e) {
      logger.error("Failed to deserialize BlobIndexAdminRequest", e);
      return new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.Bad_Request, null);
    }

    BlobId blobId = (BlobId) blobIndexAdminRequest.getStoreKey();
    try {
      PartitionId partitionId = blobId.getPartition();
      Store store = storeManager.getStore(partitionId);
      if (store == null) {
        logger.error("BlobId {}'s partition[{}] doesn't exist or stopped in this host", blobId, partitionId.getId());
        return new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.Replica_Unavailable, null);
      }

      Map<String, MessageInfo> messages = store.findAllMessageInfosForKey(blobId);
      // Serialize with JSON
      String json = objectMapper.writeValueAsString(messages);
      return new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.No_Error, json.getBytes());
    } catch (StoreException e) {
      logger.error("Failed to find all message infos for given key {}", blobId, e);
      errorMessage = e.getMessage();
    } catch (IOException e) {
      logger.error("Failed to serialize to json string", e);
      errorMessage = e.getMessage();
    } catch (Exception e) {
      logger.error("Unexpected error when handleBlobIndexRequest", e);
      errorMessage = e.getMessage();
    }
    return new AdminResponseWithContent(correlationId, clientId, ServerErrorCode.Unknown_Error,
        errorMessage.getBytes());
  }

  /**
   * Handles admin request that starts BlobStore
   * @param partitionId the {@link PartitionId} associated with BlobStore
   * @return {@link ServerErrorCode} represents result of handling admin request.
   */
  private ServerErrorCode handleStartStoreRequest(PartitionId partitionId) {
    ServerErrorCode error = validateRequest(partitionId, RequestOrResponseType.AdminRequest, true);
    if (!error.equals(ServerErrorCode.No_Error)) {
      logger.debug("Validate request fails for {} with error code {} when trying to start store", partitionId, error);
      return error;
    }
    // 1. temporarily disable metadata request to add replica to ReplicationManager after starting the store
    Collection<PartitionId> partitionIds = Collections.singletonList(partitionId);
    controlRequestForPartitions(EnumSet.of(RequestOrResponseType.ReplicaMetadataRequest), partitionIds, false);
    if (!storeManager.startBlobStore(partitionId)) {
      logger.error("Starting BlobStore fails on {}", partitionId);
      return ServerErrorCode.Unknown_Error;
    }
    // 2. add replica to replication manager if needed
    if (replicationEngine instanceof ReplicationManager) {
      // attempt to add replica to ReplicationManager in case the store was not started during server startup
      // we don't check the result of adding replica as it returns false when replica is already present, which is the
      // most common case
      ((ReplicationManager) replicationEngine).addReplica(storeManager.getReplica(partitionId.toPathString()));
    }
    // 3. enable requests on this store
    controlRequestForPartitions(
        EnumSet.of(RequestOrResponseType.GetRequest, RequestOrResponseType.ReplicaMetadataRequest,
            RequestOrResponseType.PutRequest, RequestOrResponseType.DeleteRequest,
            RequestOrResponseType.TtlUpdateRequest), partitionIds, true);
    if (!replicationEngine.controlReplicationForPartitions(partitionIds, Collections.emptyList(), true)) {
      logger.error("Could not enable replication on {}", partitionIds);
      return ServerErrorCode.Unknown_Error;
    }
    // 4. reset partition state if it's in error state (will trigger state transition)
    boolean isLocalStoreInErrorState =
        partitionId.getReplicaIdsByState(ReplicaState.ERROR, currentNode.getDatacenterName())
            .stream()
            .anyMatch(r -> r.getDataNodeId().getHostname().equals(currentNode.getHostname()));
    if (isLocalStoreInErrorState && clusterParticipant != null) {
      if (!clusterParticipant.resetPartitionState(partitionId.toPathString())) {
        logger.error("Failed to reset partition {}", partitionId);
        return ServerErrorCode.Unknown_Error;
      }
    }
    // 5. remove replica from stopped list (if Helix is adopted)
    List<PartitionId> failToUpdateList =
        storeManager.setBlobStoreStoppedState(Collections.singletonList(partitionId), false);
    if (!failToUpdateList.isEmpty() && clusterParticipant != null) {
      logger.error("Fail to remove BlobStore(s) {} from stopped list after start operation completed",
          failToUpdateList.toArray());
      return ServerErrorCode.Unknown_Error;
    }
    // 6. enable compaction on this store
    if (!storeManager.controlCompactionForBlobStore(partitionId, true)) {
      logger.error("Enable compaction fails on given BlobStore {}", partitionId);
      return ServerErrorCode.Unknown_Error;
    }
    logger.info("Store is successfully started and functional for partition: {}", partitionId);
    return ServerErrorCode.No_Error;
  }

  /**
   * Handles admin request that stops BlobStore
   * @param partitionId the {@link PartitionId} associated with BlobStore
   * @param numReplicasCaughtUpPerPartition the minimum number of peer replicas per partition that should catch up with
   *                                        local store before stopping it.
   * @return {@link ServerErrorCode} represents result of handling admin request.
   */
  private ServerErrorCode handleStopStoreRequest(PartitionId partitionId, short numReplicasCaughtUpPerPartition) {
    ServerErrorCode error = validateRequest(partitionId, RequestOrResponseType.AdminRequest, false);
    if (!error.equals(ServerErrorCode.No_Error)) {
      logger.debug("Validate request fails for {} with error code {} when trying to stop store", partitionId, error);
      return error;
    }
    if (numReplicasCaughtUpPerPartition < 0) {
      logger.debug("The number of replicas to catch up should not be less than zero {}",
          numReplicasCaughtUpPerPartition);
      return ServerErrorCode.Bad_Request;
    }
    if (!storeManager.controlCompactionForBlobStore(partitionId, false)) {
      logger.error("Disable compaction fails on given BlobStore {}", partitionId);
      return ServerErrorCode.Unknown_Error;
    }
    Collection<PartitionId> partitionIds = Collections.singletonList(partitionId);
    controlRequestForPartitions(EnumSet.of(RequestOrResponseType.PutRequest, RequestOrResponseType.DeleteRequest,
        RequestOrResponseType.TtlUpdateRequest), partitionIds, false);
    if (!replicationEngine.controlReplicationForPartitions(partitionIds, Collections.<String>emptyList(), false)) {
      logger.error("Could not disable replication on {}", partitionIds);
      return ServerErrorCode.Unknown_Error;
    }
    if (!isRemoteLagLesserOrEqual(partitionIds, 0, numReplicasCaughtUpPerPartition)) {
      logger.debug("Catchup not done on {}", partitionIds);
      return Retry_After_Backoff;
    }
    controlRequestForPartitions(
        EnumSet.of(RequestOrResponseType.ReplicaMetadataRequest, RequestOrResponseType.GetRequest), partitionIds,
        false);
    // Shutdown the BlobStore completely
    if (storeManager.shutdownBlobStore(partitionId)) {
      logger.info("Store is successfully shutdown for partition: {}", partitionId);
      List<PartitionId> failToUpdateList =
          storeManager.setBlobStoreStoppedState(Collections.singletonList(partitionId), true);
      if (!failToUpdateList.isEmpty() && clusterParticipant != null) {
        logger.error("Fail to add BlobStore(s) {} to stopped list after stop operation completed",
            failToUpdateList.toArray());
        error = ServerErrorCode.Unknown_Error;
      }
      // After store is shut down and stopped state is updated, we also need to temporarily disable this replica if Helix
      // is adopted. The intention here is to force Helix to re-elect a new leader replica if necessary.
      if (storeManager instanceof StorageManager && clusterParticipant != null) {
        // Previous operation has guaranteed the store is not null
        Store store = ((StorageManager) storeManager).getStore(partitionId, true);
        // Setting disable state will trigger transition error at the very beginning of STANDBY -> INACTIVE transition.
        // Thus it doesn't go through decommission process.
        ((BlobStore) store).setDisableState(true);
        clusterParticipant.setReplicaDisabledState(storeManager.getReplica(partitionId.toPathString()), true);
      }
    } else {
      error = ServerErrorCode.Unknown_Error;
      logger.error("Shutting down BlobStore fails on {}", partitionId);
    }
    return error;
  }

  /**
   * Handles admin request that adds a BlobStore to current node
   * @param partitionId the {@link PartitionId} associated with BlobStore
   * @return {@link ServerErrorCode} represents result of handling admin request.
   */
  private ServerErrorCode handleAddStoreRequest(PartitionId partitionId) {
    ServerErrorCode errorCode = ServerErrorCode.No_Error;
    ReplicaId replicaToAdd = clusterMap.getBootstrapReplica(partitionId.toPathString(), currentNode);
    if (replicaToAdd == null) {
      logger.error("No new replica found for {} in cluster map", partitionId);
      return ServerErrorCode.Replica_Unavailable;
    }
    // Attempt to add store into storage manager. If store already exists, fail adding store request.
    if (!storeManager.addBlobStore(replicaToAdd)) {
      logger.error("Failed to add {} into storage manager", partitionId);
      return ServerErrorCode.Unknown_Error;
    }
    // Attempt to add replica into replication manager. If replica already exists, fail adding replica request
    if (!((ReplicationManager) replicationEngine).addReplica(replicaToAdd)) {
      logger.error("Failed to add {} into replication manager", partitionId);
      return ServerErrorCode.Unknown_Error;
    }
    // Attempt to add replica into stats manager. If replica already exists, fail adding replica request
    if (statsManager.addReplica(replicaToAdd)) {
      logger.info("{} is successfully added into storage manager, replication manager and stats manager.", partitionId);
    } else {
      errorCode = ServerErrorCode.Unknown_Error;
      logger.error("Failed to add {} into stats manager", partitionId);
    }
    return errorCode;
  }

  /**
   * Handles admin request that removes a BlobStore from current node
   * @param partitionId the {@link PartitionId} associated with BlobStore
   * @return {@link ServerErrorCode} represents result of handling admin request.
   */
  private ServerErrorCode handleRemoveStoreRequest(PartitionId partitionId) throws StoreException, IOException {
    ServerErrorCode errorCode = ServerErrorCode.No_Error;
    ReplicaId replicaId = storeManager.getReplica(partitionId.toPathString());
    if (replicaId == null) {
      logger.error("{} doesn't exist on current node", partitionId);
      return ServerErrorCode.Partition_Unknown;
    }
    // Attempt to remove replica from stats manager. If replica doesn't exist, log info but don't fail the request
    statsManager.removeReplica(replicaId);
    // Attempt to remove replica from replication manager. If replica doesn't exist, log info but don't fail the request
    ((ReplicationManager) replicationEngine).removeReplica(replicaId);
    Store store = ((StorageManager) storeManager).getStore(partitionId, true);
    // Attempt to remove store from storage manager.
    if (storeManager.removeBlobStore(partitionId) && store != null) {
      ((BlobStore) store).deleteStoreFiles();
      for (ReplicaStatusDelegate replicaStatusDelegate : ((BlobStore) store).getReplicaStatusDelegates()) {
        // Remove store from sealed and stopped list (if present)
        logger.info("Removing store from sealed and stopped list(if present)");
        replicaStatusDelegate.unseal(replicaId);
        replicaStatusDelegate.unmarkStopped(Collections.singletonList(replicaId));
      }
    } else {
      errorCode = ServerErrorCode.Unknown_Error;
    }
    return errorCode;
  }

  /**
   * Check that the provided partition is valid, on the disk, and can be written to.
   * @param partition the partition to validate.
   * @param requestType the {@link RequestOrResponseType} being validated.
   * @param skipPartitionAndDiskAvailableCheck whether to skip ({@code true}) conditions check for the availability of
   *                                           partition and disk.
   * @return {@link ServerErrorCode#No_Error} error if the partition can be written to, or the corresponding error code
   *         if it cannot.
   */
  @Override
  protected ServerErrorCode validateRequest(PartitionId partition, RequestOrResponseType requestType,
      boolean skipPartitionAndDiskAvailableCheck) {
    ServerErrorCode errorCode = super.validateRequest(partition, requestType, skipPartitionAndDiskAvailableCheck);
    if (errorCode != ServerErrorCode.No_Error) {
      return errorCode;
    }
    if (!skipPartitionAndDiskAvailableCheck) {
      // Ensure the disk for the partition/replica is available
      ReplicaId localReplica = storeManager.getReplica(partition.toPathString());
      if (localReplica != null && localReplica.getDiskId().getState() == HardwareState.UNAVAILABLE) {
        metrics.diskUnavailableError.inc();
        return ServerErrorCode.Disk_Unavailable;
      }
      // Check if partition exists on this node and that the store for this partition is available
      errorCode = storeManager.checkLocalPartitionStatus(partition, localReplica);
      switch (errorCode) {
        case Disk_Unavailable:
          metrics.diskUnavailableError.inc();
          localReplica.markDiskDown();
          return errorCode;
        case Replica_Unavailable:
          metrics.replicaUnavailableError.inc();
          return errorCode;
        case Partition_Unknown:
          metrics.partitionUnknownError.inc();
          return errorCode;
      }
    }
    // Ensure the partition is writable
    if (requestType.equals(RequestOrResponseType.PutRequest)
        && partition.getPartitionState() == PartitionState.READ_ONLY) {
      metrics.partitionReadOnlyError.inc();
      return ServerErrorCode.Partition_ReadOnly;
    }
    // Ensure the request is enabled.
    if (!isRequestEnabled(requestType, partition)) {
      metrics.temporarilyDisabledError.inc();
      return ServerErrorCode.Temporarily_Disabled;
    }
    return ServerErrorCode.No_Error;
  }

  /**
   * Provides catch up status of all the remote replicas of {@code partitionIds}.
   * @param partitionIds the {@link PartitionId}s for which lag has to be <= {@code acceptableLagInBytes}.
   * @param acceptableLagInBytes the maximum lag in bytes that is considered "acceptable".
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within {@code acceptableLagInBytes}
   *                                        (per partition). The min of this value or the total count of replicas - 1 is
   *                                        considered.
   * @return {@code true} if the lag of each of the remote replicas of each of the {@link PartitionId} in
   * {@code partitionIds} <= {@code acceptableLagInBytes}. {@code false} otherwise.
   */
  private boolean isRemoteLagLesserOrEqual(Collection<PartitionId> partitionIds, long acceptableLagInBytes,
      short numReplicasCaughtUpPerPartition) {
    boolean isAcceptable = true;
    for (PartitionId partitionId : partitionIds) {
      List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
      int caughtUpCount = 0;
      for (ReplicaId replicaId : replicaIds) {
        if (!replicaId.getDataNodeId().equals(currentNode)) {
          long lagInBytes = replicationEngine.getRemoteReplicaLagFromLocalInBytes(partitionId,
              replicaId.getDataNodeId().getHostname(), replicaId.getReplicaPath());
          logger.debug("Lag of {} is {}", replicaId, lagInBytes);
          if (lagInBytes <= acceptableLagInBytes) {
            caughtUpCount++;
          }
          if (caughtUpCount >= numReplicasCaughtUpPerPartition) {
            break;
          }
        }
      }
      // -1 because we shouldn't consider the replica hosted on this node.
      if (caughtUpCount < Math.min(replicaIds.size() - 1, numReplicasCaughtUpPerPartition)) {
        isAcceptable = false;
        break;
      }
    }
    return isAcceptable;
  }
}
