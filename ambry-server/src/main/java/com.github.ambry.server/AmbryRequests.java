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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatMetrics;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.CompositeSend;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.CatchupStatusAdminRequest;
import com.github.ambry.protocol.CatchupStatusAdminResponse;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.ReplicationControlAdminRequest;
import com.github.ambry.protocol.RequestControlAdminRequest;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */

public class AmbryRequests implements RequestAPI {

  private StorageManager storageManager;
  private final RequestResponseChannel requestResponseChannel;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private Logger publicAccessLogger = LoggerFactory.getLogger("PublicAccessLogger");
  private final ClusterMap clusterMap;
  private final DataNodeId currentNode;
  private final Collection<PartitionId> partitionsInCurrentNode;
  private final ServerMetrics metrics;
  private final MessageFormatMetrics messageFormatMetrics;
  private final FindTokenFactory findTokenFactory;
  private final NotificationSystem notification;
  private final ReplicationManager replicationManager;
  private final StoreKeyFactory storeKeyFactory;
  private final ConcurrentHashMap<RequestOrResponseType, Set<PartitionId>> requestsDisableInfo =
      new ConcurrentHashMap<>();

  public AmbryRequests(StorageManager storageManager, RequestResponseChannel requestResponseChannel,
      ClusterMap clusterMap, DataNodeId nodeId, MetricRegistry registry, FindTokenFactory findTokenFactory,
      NotificationSystem operationNotification, ReplicationManager replicationManager,
      StoreKeyFactory storeKeyFactory) {
    this.storageManager = storageManager;
    this.requestResponseChannel = requestResponseChannel;
    this.clusterMap = clusterMap;
    this.currentNode = nodeId;
    this.metrics = new ServerMetrics(registry);
    this.messageFormatMetrics = new MessageFormatMetrics(registry);
    this.findTokenFactory = findTokenFactory;
    this.notification = operationNotification;
    this.replicationManager = replicationManager;
    this.storeKeyFactory = storeKeyFactory;

    requestsDisableInfo.put(RequestOrResponseType.PutRequest, Collections.newSetFromMap(new ConcurrentHashMap<>()));
    requestsDisableInfo.put(RequestOrResponseType.GetRequest, Collections.newSetFromMap(new ConcurrentHashMap<>()));
    requestsDisableInfo.put(RequestOrResponseType.DeleteRequest, Collections.newSetFromMap(new ConcurrentHashMap<>()));
    requestsDisableInfo.put(RequestOrResponseType.ReplicaMetadataRequest,
        Collections.newSetFromMap(new ConcurrentHashMap<>()));

    Set<PartitionId> partitionIds = new HashSet<>();
    for (ReplicaId replicaId : clusterMap.getReplicaIds(currentNode)) {
      partitionIds.add(replicaId.getPartitionId());
    }
    partitionsInCurrentNode = Collections.unmodifiableSet(partitionIds);
  }

  public void handleRequests(Request request) throws InterruptedException {
    try {
      DataInputStream stream = new DataInputStream(request.getInputStream());
      RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
      switch (type) {
        case PutRequest:
          handlePutRequest(request);
          break;
        case GetRequest:
          handleGetRequest(request);
          break;
        case DeleteRequest:
          handleDeleteRequest(request);
          break;
        case ReplicaMetadataRequest:
          handleReplicaMetadataRequest(request);
          break;
        case AdminRequest:
          handleAdminRequest(request);
          break;
        default:
          throw new UnsupportedOperationException("Request type not supported");
      }
    } catch (Exception e) {
      logger.error("Error while handling request " + request + " closing connection", e);
      requestResponseChannel.closeConnection(request);
    }
  }

  public void handlePutRequest(Request request) throws IOException, InterruptedException {
    PutRequest.ReceivedPutRequest receivedRequest =
        PutRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.putBlobRequestQueueTimeInMs.update(requestQueueTime);
    metrics.putBlobRequestRate.mark();
    long startTime = SystemTime.getInstance().milliseconds();
    PutResponse response = null;
    try {
      ServerErrorCode error =
          validateRequest(receivedRequest.getBlobId().getPartition(), RequestOrResponseType.PutRequest);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating put request failed with error {} for request {}", error, receivedRequest);
        response = new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(), error);
      } else {
        MessageFormatInputStream stream =
            new PutMessageFormatInputStream(receivedRequest.getBlobId(), receivedRequest.getBlobEncryptionKey(),
                receivedRequest.getBlobProperties(), receivedRequest.getUsermetadata(), receivedRequest.getBlobStream(),
                receivedRequest.getBlobSize(), receivedRequest.getBlobType());
        MessageInfo info = new MessageInfo(receivedRequest.getBlobId(), stream.getSize(), false,
            Utils.addSecondsToEpochTime(receivedRequest.getBlobProperties().getCreationTimeInMs(),
                receivedRequest.getBlobProperties().getTimeToLiveInSeconds()), receivedRequest.getCrc(),
            receivedRequest.getBlobProperties().getAccountId(), receivedRequest.getBlobProperties().getContainerId(),
            receivedRequest.getBlobProperties().getCreationTimeInMs());
        ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
        infoList.add(info);
        MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList, false);
        Store storeToPut = storageManager.getStore(receivedRequest.getBlobId().getPartition());
        storeToPut.put(writeset);
        response = new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
            ServerErrorCode.No_Error);
        metrics.blobSizeInBytes.update(receivedRequest.getBlobSize());
        metrics.blobUserMetadataSizeInBytes.update(receivedRequest.getUsermetadata().limit());
        if (notification != null) {
          notification.onBlobReplicaCreated(currentNode.getHostname(), currentNode.getPort(),
              receivedRequest.getBlobId().getID(), BlobReplicaSourceType.PRIMARY);
        }
      }
    } catch (StoreException e) {
      logger.error("Store exception on a put with error code " + e.getErrorCode() + " for request " + receivedRequest,
          e);
      if (e.getErrorCode() == StoreErrorCodes.Already_Exist) {
        metrics.idAlreadyExistError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.IOError) {
        metrics.storeIOError.inc();
      } else {
        metrics.unExpectedStorePutError.inc();
      }
      response = new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
          ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    } catch (Exception e) {
      logger.error("Unknown exception on a put for request " + receivedRequest, e);
      response = new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
          ServerErrorCode.Unknown_Error);
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", receivedRequest, response, processingTime);
      metrics.putBlobProcessingTimeInMs.update(processingTime);
    }
    sendPutResponse(requestResponseChannel, response, request, metrics.putBlobResponseQueueTimeInMs,
        metrics.putBlobSendTimeInMs, metrics.putBlobTotalTimeInMs, totalTimeSpent, receivedRequest.getBlobSize(),
        metrics);
  }

  public void handleGetRequest(Request request) throws IOException, InterruptedException {
    GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    Histogram responseQueueTime = null;
    Histogram responseSendTime = null;
    Histogram responseTotalTime = null;
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
      metrics.getBlobRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobRequestRate.mark();
      responseQueueTime = metrics.getBlobResponseQueueTimeInMs;
      responseSendTime = metrics.getBlobSendTimeInMs;
      responseTotalTime = metrics.getBlobTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
      metrics.getBlobPropertiesRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobPropertiesRequestRate.mark();
      responseQueueTime = metrics.getBlobPropertiesResponseQueueTimeInMs;
      responseSendTime = metrics.getBlobPropertiesSendTimeInMs;
      responseTotalTime = metrics.getBlobPropertiesTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
      metrics.getBlobUserMetadataRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobUserMetadataRequestRate.mark();
      responseQueueTime = metrics.getBlobUserMetadataResponseQueueTimeInMs;
      responseSendTime = metrics.getBlobUserMetadataSendTimeInMs;
      responseTotalTime = metrics.getBlobUserMetadataTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobInfo) {
      metrics.getBlobInfoRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobInfoRequestRate.mark();
      responseQueueTime = metrics.getBlobInfoResponseQueueTimeInMs;
      responseSendTime = metrics.getBlobInfoSendTimeInMs;
      responseTotalTime = metrics.getBlobInfoTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.All) {
      metrics.getBlobAllRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobAllRequestRate.mark();
      responseQueueTime = metrics.getBlobAllResponseQueueTimeInMs;
      responseSendTime = metrics.getBlobAllSendTimeInMs;
      responseTotalTime = metrics.getBlobAllTotalTimeInMs;
    }
    long startTime = SystemTime.getInstance().milliseconds();
    GetResponse response = null;
    try {
      List<Send> messagesToSendList = new ArrayList<Send>(getRequest.getPartitionInfoList().size());
      List<PartitionResponseInfo> partitionResponseInfoList =
          new ArrayList<PartitionResponseInfo>(getRequest.getPartitionInfoList().size());
      for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
        ServerErrorCode error = validateRequest(partitionRequestInfo.getPartition(), RequestOrResponseType.GetRequest);
        if (error != ServerErrorCode.No_Error) {
          logger.error("Validating get request failed for partition {} with error {}",
              partitionRequestInfo.getPartition(), error);
          PartitionResponseInfo partitionResponseInfo =
              new PartitionResponseInfo(partitionRequestInfo.getPartition(), error);
          partitionResponseInfoList.add(partitionResponseInfo);
        } else {
          try {
            Store storeToGet = storageManager.getStore(partitionRequestInfo.getPartition());
            EnumSet<StoreGetOptions> storeGetOptions = EnumSet.noneOf(StoreGetOptions.class);
            // Currently only one option is supported.
            if (getRequest.getGetOption() == GetOption.Include_Expired_Blobs) {
              storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Expired);
            }
            if (getRequest.getGetOption() == GetOption.Include_Deleted_Blobs) {
              storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted);
            }
            if (getRequest.getGetOption() == GetOption.Include_All) {
              storeGetOptions =
                  EnumSet.of(StoreGetOptions.Store_Include_Deleted, StoreGetOptions.Store_Include_Expired);
            }
            StoreInfo info = storeToGet.get(partitionRequestInfo.getBlobIds(), storeGetOptions);
            MessageFormatSend blobsToSend =
                new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag(), messageFormatMetrics,
                    storeKeyFactory);
            PartitionResponseInfo partitionResponseInfo =
                new PartitionResponseInfo(partitionRequestInfo.getPartition(), info.getMessageReadSetInfo(),
                    blobsToSend.getMessageMetadataList());
            messagesToSendList.add(blobsToSend);
            partitionResponseInfoList.add(partitionResponseInfo);
          } catch (StoreException e) {
            boolean logInErrorLevel = false;
            if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found) {
              metrics.idNotFoundError.inc();
            } else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired) {
              metrics.ttlExpiredError.inc();
            } else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted) {
              metrics.idDeletedError.inc();
            } else if (e.getErrorCode() == StoreErrorCodes.Authorization_Failure) {
              metrics.getAuthorizationFailure.inc();
            } else {
              metrics.unExpectedStoreGetError.inc();
              logInErrorLevel = true;
            }
            if (logInErrorLevel) {
              logger.error("Store exception on a get with error code {} for partition {}", e.getErrorCode(),
                  partitionRequestInfo.getPartition(), e);
            } else {
              logger.trace("Store exception on a get with error code {} for partition {}", e.getErrorCode(),
                  partitionRequestInfo.getPartition(), e);
            }
            PartitionResponseInfo partitionResponseInfo = new PartitionResponseInfo(partitionRequestInfo.getPartition(),
                ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
            partitionResponseInfoList.add(partitionResponseInfo);
          } catch (MessageFormatException e) {
            logger.error(
                "Message format exception on a get with error code " + e.getErrorCode() + " for partitionRequestInfo "
                    + partitionRequestInfo, e);
            if (e.getErrorCode() == MessageFormatErrorCodes.Data_Corrupt) {
              metrics.dataCorruptError.inc();
            } else if (e.getErrorCode() == MessageFormatErrorCodes.Unknown_Format_Version) {
              metrics.unknownFormatError.inc();
            }
            PartitionResponseInfo partitionResponseInfo = new PartitionResponseInfo(partitionRequestInfo.getPartition(),
                ErrorMapping.getMessageFormatErrorMapping(e.getErrorCode()));
            partitionResponseInfoList.add(partitionResponseInfo);
          }
        }
      }
      CompositeSend compositeSend = new CompositeSend(messagesToSendList);
      response = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), partitionResponseInfoList,
          compositeSend, ServerErrorCode.No_Error);
    } catch (Exception e) {
      logger.error("Unknown exception for request " + getRequest, e);
      response =
          new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), ServerErrorCode.Unknown_Error);
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", getRequest, response, processingTime);
      if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
        metrics.getBlobProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
        metrics.getBlobPropertiesProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
        metrics.getBlobUserMetadataProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobInfo) {
        metrics.getBlobInfoProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.All) {
        metrics.getBlobAllProcessingTimeInMs.update(processingTime);
      }
    }
    sendGetResponse(requestResponseChannel, response, request, responseQueueTime, responseSendTime, responseTotalTime,
        totalTimeSpent, response.sizeInBytes(), getRequest.getMessageFormatFlag(), metrics);
  }

  public void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    DeleteRequest deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.deleteBlobRequestQueueTimeInMs.update(requestQueueTime);
    metrics.deleteBlobRequestRate.mark();
    long startTime = SystemTime.getInstance().milliseconds();
    DeleteResponse response = null;
    try {
      ServerErrorCode error =
          validateRequest(deleteRequest.getBlobId().getPartition(), RequestOrResponseType.DeleteRequest);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating delete request failed with error {} for request {}", error, deleteRequest);
        response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), error);
      } else {
        MessageFormatInputStream stream =
            new DeleteMessageFormatInputStream(deleteRequest.getBlobId(), deleteRequest.getAccountId(),
                deleteRequest.getContainerId(), deleteRequest.getDeletionTimeInMs());
        MessageInfo info = new MessageInfo(deleteRequest.getBlobId(), stream.getSize(), deleteRequest.getAccountId(),
            deleteRequest.getContainerId(), deleteRequest.getDeletionTimeInMs());
        ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
        infoList.add(info);
        MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList, false);
        Store storeToDelete = storageManager.getStore(deleteRequest.getBlobId().getPartition());
        storeToDelete.delete(writeset);
        response =
            new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), ServerErrorCode.No_Error);
        if (notification != null) {
          notification.onBlobReplicaDeleted(currentNode.getHostname(), currentNode.getPort(),
              deleteRequest.getBlobId().getID(), BlobReplicaSourceType.PRIMARY);
        }
      }
    } catch (StoreException e) {
      boolean logInErrorLevel = false;
      if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found) {
        metrics.idNotFoundError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired) {
        metrics.ttlExpiredError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted) {
        metrics.idDeletedError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.Authorization_Failure) {
        metrics.deleteAuthorizationFailure.inc();
      } else {
        logInErrorLevel = true;
        metrics.unExpectedStoreDeleteError.inc();
      }
      if (logInErrorLevel) {
        logger.error("Store exception on a delete with error code {} for request {}", e.getErrorCode(), deleteRequest,
            e);
      } else {
        logger.trace("Store exception on a delete with error code {} for request {}", e.getErrorCode(), deleteRequest,
            e);
      }
      response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(),
          ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    } catch (Exception e) {
      logger.error("Unknown exception for delete request " + deleteRequest, e);
      response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(),
          ServerErrorCode.Unknown_Error);
      metrics.unExpectedStoreDeleteError.inc();
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", deleteRequest, response, processingTime);
      metrics.deleteBlobProcessingTimeInMs.update(processingTime);
    }
    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.deleteBlobResponseQueueTimeInMs, metrics.deleteBlobSendTimeInMs,
            metrics.deleteBlobTotalTimeInMs, null, null, totalTimeSpent));
  }

  public void handleReplicaMetadataRequest(Request request) throws IOException, InterruptedException {
    ReplicaMetadataRequest replicaMetadataRequest =
        ReplicaMetadataRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap, findTokenFactory);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.replicaMetadataRequestQueueTimeInMs.update(requestQueueTime);
    metrics.replicaMetadataRequestRate.mark();

    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList =
        replicaMetadataRequest.getReplicaMetadataRequestInfoList();
    int partitionCnt = replicaMetadataRequestInfoList.size();
    long startTimeInMs = SystemTime.getInstance().milliseconds();
    ReplicaMetadataResponse response = null;
    try {
      List<ReplicaMetadataResponseInfo> replicaMetadataResponseList =
          new ArrayList<ReplicaMetadataResponseInfo>(partitionCnt);
      for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : replicaMetadataRequestInfoList) {
        long partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
        PartitionId partitionId = replicaMetadataRequestInfo.getPartitionId();
        ServerErrorCode error = validateRequest(partitionId, RequestOrResponseType.ReplicaMetadataRequest);
        logger.trace("{} Time used to validate metadata request: {}", partitionId,
            (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

        if (error != ServerErrorCode.No_Error) {
          logger.error("Validating replica metadata request failed with error {} for partition {}", error, partitionId);
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo = new ReplicaMetadataResponseInfo(partitionId, error);
          replicaMetadataResponseList.add(replicaMetadataResponseInfo);
        } else {
          try {
            FindToken findToken = replicaMetadataRequestInfo.getToken();
            String hostName = replicaMetadataRequestInfo.getHostName();
            String replicaPath = replicaMetadataRequestInfo.getReplicaPath();
            Store store = storageManager.getStore(partitionId);

            partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
            FindInfo findInfo =
                store.findEntriesSince(findToken, replicaMetadataRequest.getMaxTotalSizeOfEntriesInBytes());
            logger.trace("{} Time used to find entry since: {}", partitionId,
                (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

            partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
            replicationManager.updateTotalBytesReadByRemoteReplica(partitionId, hostName, replicaPath,
                findInfo.getFindToken().getBytesRead());
            logger.trace("{} Time used to update total bytes read: {}", partitionId,
                (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

            partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
            long remoteReplicaLagInBytes =
                replicationManager.getRemoteReplicaLagFromLocalInBytes(partitionId, hostName, replicaPath);
            logger.trace("{} Time used to get remote replica lag in bytes: {}", partitionId,
                (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

            ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
                new ReplicaMetadataResponseInfo(partitionId, findInfo.getFindToken(), findInfo.getMessageEntries(),
                    remoteReplicaLagInBytes);
            replicaMetadataResponseList.add(replicaMetadataResponseInfo);
          } catch (StoreException e) {
            logger.error(
                "Store exception on a replica metadata request with error code " + e.getErrorCode() + " for partition "
                    + partitionId, e);
            if (e.getErrorCode() == StoreErrorCodes.IOError) {
              metrics.storeIOError.inc();
            } else {
              metrics.unExpectedStoreFindEntriesError.inc();
            }
            ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
                new ReplicaMetadataResponseInfo(partitionId, ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
            replicaMetadataResponseList.add(replicaMetadataResponseInfo);
          }
        }
      }
      response =
          new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(), replicaMetadataRequest.getClientId(),
              ServerErrorCode.No_Error, replicaMetadataResponseList);
    } catch (Exception e) {
      logger.error("Unknown exception for request " + replicaMetadataRequest, e);
      response =
          new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(), replicaMetadataRequest.getClientId(),
              ServerErrorCode.Unknown_Error);
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTimeInMs;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", replicaMetadataRequest, response, processingTime);
      logger.trace("{} {} processingTime {}", replicaMetadataRequest, response, processingTime);
      metrics.replicaMetadataRequestProcessingTimeInMs.update(processingTime);
    }

    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.replicaMetadataResponseQueueTimeInMs,
            metrics.replicaMetadataSendTimeInMs, metrics.replicaMetadataTotalTimeInMs, null, null, totalTimeSpent));
  }

  /**
   * @param requestType the {@link RequestOrResponseType} of the request.
   * @param id the partition id that the request is targeting.
   * @return {@code true} if the request is enabled. {@code false} otherwise.
   */
  private boolean isRequestEnabled(RequestOrResponseType requestType, PartitionId id) {
    Set<PartitionId> requestDisableInfo = requestsDisableInfo.get(requestType);
    return requestDisableInfo == null || !requestDisableInfo.contains(id);
  }

  /**
   * Enables/disables {@code requestOrResponseType} on the given {@code ids}.
   * @param requestType the {@link RequestOrResponseType} to enable/disable.
   * @param ids the {@link PartitionId}s to enable/disable it on.
   * @param enable whether to enable ({@code true}) or disable
   */
  private void controlRequestForPartitions(RequestOrResponseType requestType, Collection<PartitionId> ids,
      boolean enable) {
    if (enable) {
      requestsDisableInfo.get(requestType).removeAll(ids);
    } else {
      requestsDisableInfo.get(requestType).addAll(ids);
    }
  }

  /**
   * Handles an administration request. These requests can query for or change the internal state of the server.
   * @param request the request that needs to be handled.
   * @throws InterruptedException if response sending is interrupted.
   * @throws IOException if there are I/O errors carrying our the required operation.
   */
  private void handleAdminRequest(Request request) throws InterruptedException, IOException {
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    long startTime = SystemTime.getInstance().milliseconds();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    AdminRequest adminRequest = AdminRequest.readFrom(requestStream, clusterMap);
    Histogram processingTimeHistogram = null;
    Histogram responseQueueTimeHistogram = null;
    Histogram responseSendTimeHistogram = null;
    Histogram requestTotalTimeHistogram = null;
    AdminResponse response = null;
    try {
      switch (adminRequest.getType()) {
        case TriggerCompaction:
          metrics.triggerCompactionRequestQueueTimeInMs.update(requestQueueTime);
          metrics.triggerCompactionRequestRate.mark();
          processingTimeHistogram = metrics.triggerCompactionResponseQueueTimeInMs;
          responseQueueTimeHistogram = metrics.triggerCompactionResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.triggerCompactionResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.triggerCompactionRequestTotalTimeInMs;
          response = handleTriggerCompactionRequest(adminRequest);
          break;
        case RequestControl:
          metrics.requestControlRequestQueueTimeInMs.update(requestQueueTime);
          metrics.requestControlRequestRate.mark();
          processingTimeHistogram = metrics.requestControlResponseQueueTimeInMs;
          responseQueueTimeHistogram = metrics.requestControlResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.requestControlResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.requestControlRequestTotalTimeInMs;
          response = handleRequestControlRequest(requestStream, adminRequest);
          break;
        case ReplicationControl:
          metrics.replicationControlRequestQueueTimeInMs.update(requestQueueTime);
          metrics.replicationControlRequestRate.mark();
          processingTimeHistogram = metrics.replicationControlResponseQueueTimeInMs;
          responseQueueTimeHistogram = metrics.replicationControlResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.replicationControlResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.replicationControlRequestTotalTimeInMs;
          response = handleReplicationControlRequest(requestStream, adminRequest);
          break;
        case CatchupStatus:
          metrics.catchupStatusRequestQueueTimeInMs.update(requestQueueTime);
          metrics.catchupStatusRequestRate.mark();
          processingTimeHistogram = metrics.catchupStatusResponseQueueTimeInMs;
          responseQueueTimeHistogram = metrics.catchupStatusResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.catchupStatusResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.catchupStatusRequestTotalTimeInMs;
          response = handleCatchupStatusRequest(requestStream, adminRequest);
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
      }
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", adminRequest, response, processingTime);
      processingTimeHistogram.update(processingTime);
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
    ServerErrorCode error = validateRequest(adminRequest.getPartitionId(), RequestOrResponseType.AdminRequest);
    if (error != ServerErrorCode.No_Error) {
      logger.error("Validating trigger compaction request failed with error {} for {}", error, adminRequest);
    } else if (!storageManager.scheduleNextForCompaction(adminRequest.getPartitionId())) {
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
        error = validateRequest(controlRequest.getPartitionId(), RequestOrResponseType.AdminRequest);
        partitionIds = Collections.singletonList(controlRequest.getPartitionId());
      } else {
        partitionIds = partitionsInCurrentNode;
      }
      if (!error.equals(ServerErrorCode.Partition_Unknown)) {
        controlRequestForPartitions(toControl, partitionIds, controlRequest.shouldEnable());
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
      error = validateRequest(replControlRequest.getPartitionId(), RequestOrResponseType.AdminRequest);
      partitionIds = Collections.singletonList(replControlRequest.getPartitionId());
    } else {
      partitionIds = partitionsInCurrentNode;
    }
    if (!error.equals(ServerErrorCode.Partition_Unknown)) {
      if (replicationManager.controlReplicationForPartitions(partitionIds, replControlRequest.getOrigins(),
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
        error = validateRequest(catchupStatusRequest.getPartitionId(), RequestOrResponseType.AdminRequest);
        partitionIds = Collections.singletonList(catchupStatusRequest.getPartitionId());
      } else {
        partitionIds = partitionsInCurrentNode;
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

  private void sendPutResponse(RequestResponseChannel requestResponseChannel, PutResponse response, Request request,
      Histogram responseQueueTime, Histogram responseSendTime, Histogram requestTotalTime, long totalTimeSpent,
      long blobSize, ServerMetrics metrics) throws InterruptedException {
    if (response.getError() == ServerErrorCode.No_Error) {
      metrics.markPutBlobRequestRateBySize(blobSize);
      if (blobSize <= ServerMetrics.smallBlob) {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                metrics.putSmallBlobProcessingTimeInMs, metrics.putSmallBlobTotalTimeInMs, totalTimeSpent));
      } else if (blobSize <= ServerMetrics.mediumBlob) {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                metrics.putMediumBlobProcessingTimeInMs, metrics.putMediumBlobTotalTimeInMs, totalTimeSpent));
      } else {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                metrics.putLargeBlobProcessingTimeInMs, metrics.putLargeBlobTotalTimeInMs, totalTimeSpent));
      }
    } else {
      requestResponseChannel.sendResponse(response, request,
          new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
              totalTimeSpent));
    }
  }

  private void sendGetResponse(RequestResponseChannel requestResponseChannel, GetResponse response, Request request,
      Histogram responseQueueTime, Histogram responseSendTime, Histogram requestTotalTime, long totalTimeSpent,
      long blobSize, MessageFormatFlags flags, ServerMetrics metrics) throws InterruptedException {

    if (blobSize <= ServerMetrics.smallBlob) {
      if (flags == MessageFormatFlags.Blob) {
        if (response.getError() == ServerErrorCode.No_Error) {
          metrics.markGetBlobRequestRateBySize(blobSize);
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                  metrics.getSmallBlobProcessingTimeInMs, metrics.getSmallBlobTotalTimeInMs, totalTimeSpent));
        } else {
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
                  totalTimeSpent));
        }
      } else {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
                totalTimeSpent));
      }
    } else if (blobSize <= ServerMetrics.mediumBlob) {
      if (flags == MessageFormatFlags.Blob) {
        if (response.getError() == ServerErrorCode.No_Error) {
          metrics.markGetBlobRequestRateBySize(blobSize);
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                  metrics.getMediumBlobProcessingTimeInMs, metrics.getMediumBlobTotalTimeInMs, totalTimeSpent));
        } else {
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
                  totalTimeSpent));
        }
      } else {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
                totalTimeSpent));
      }
    } else {
      if (flags == MessageFormatFlags.Blob) {
        if (response.getError() == ServerErrorCode.No_Error) {
          metrics.markGetBlobRequestRateBySize(blobSize);
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                  metrics.getLargeBlobProcessingTimeInMs, metrics.getLargeBlobTotalTimeInMs, totalTimeSpent));
        } else {
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
                  totalTimeSpent));
        }
      } else {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
                totalTimeSpent));
      }
    }
  }

  /**
   * Check that the provided partition is valid, on the disk, and can be written to.
   * @param partition the partition to validate.
   * @param requestType the {@link RequestOrResponseType} being validated.
   * @return {@link ServerErrorCode#No_Error} error if the partition can be written to, or the corresponding error code
   *         if it cannot.
   */
  private ServerErrorCode validateRequest(PartitionId partition, RequestOrResponseType requestType) {
    // 1. Check partition is null
    if (partition == null) {
      metrics.badRequestError.inc();
      return ServerErrorCode.Bad_Request;
    }
    // 2. check if partition exists on this node and that the store for this partition has been started
    if (storageManager.getStore(partition) == null) {
      if (partitionsInCurrentNode.contains(partition)) {
        metrics.diskUnavailableError.inc();
        return ServerErrorCode.Disk_Unavailable;
      } else {
        metrics.partitionUnknownError.inc();
        return ServerErrorCode.Partition_Unknown;
      }
    }
    // 3. ensure the disk for the partition/replica is available
    List<? extends ReplicaId> replicaIds = partition.getReplicaIds();
    for (ReplicaId replica : replicaIds) {
      if (replica.getDataNodeId().getHostname().equals(currentNode.getHostname())
          && replica.getDataNodeId().getPort() == currentNode.getPort()) {
        if (replica.getDiskId().getState() == HardwareState.UNAVAILABLE) {
          metrics.diskUnavailableError.inc();
          return ServerErrorCode.Disk_Unavailable;
        }
      }
    }
    // 4. ensure if the partition can be written to
    if (requestType.equals(RequestOrResponseType.PutRequest)
        && partition.getPartitionState() == PartitionState.READ_ONLY) {
      metrics.partitionReadOnlyError.inc();
      return ServerErrorCode.Partition_ReadOnly;
    }
    // 5. Ensure that the request is enabled.
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
          long lagInBytes = replicationManager.getRemoteReplicaLagFromLocalInBytes(partitionId,
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
