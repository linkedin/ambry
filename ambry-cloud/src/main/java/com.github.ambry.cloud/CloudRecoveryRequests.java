/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatMetrics;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.network.CompositeSend;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactoryFactory;
import com.github.ambry.server.ErrorMapping;
import com.github.ambry.server.RequestAPI;
import com.github.ambry.server.ServerMetrics;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudRecoveryRequests implements RequestAPI {

  private Logger logger = LoggerFactory.getLogger(getClass());
  private final CloudBackupManager cloudBackupManager;
  private final RequestResponseChannel requestResponseChannel;
  private final DataNodeId currentNode;
  private final ServerMetrics cloudRecoveryServerMetrics;
  private final MessageFormatMetrics messageFormatMetrics;
  private final FindTokenFactoryFactory findTokenFactoryFactory;
  private final NotificationSystem notification;
  private final StoreKeyFactory storeKeyFactory;
  private final StoreKeyConverterFactory storeKeyConverterFactory;
  private final boolean enableDataPrefetch;
  private final ClusterMap clusterMap;

  public CloudRecoveryRequests(CloudBackupManager cloudBackupManager, RequestResponseChannel requestResponseChannel,
      DataNodeId currentNode, MetricRegistry registry, FindTokenFactoryFactory findTokenFactoryFactory,
      NotificationSystem notification, StoreKeyFactory storageKeyFactory,
      StoreKeyConverterFactory storeKeyConverterFactory, ClusterMap clusterMap, boolean enableDataPrefetch) {
    this.cloudBackupManager = cloudBackupManager;
    this.requestResponseChannel = requestResponseChannel;
    this.currentNode = currentNode;
    this.cloudRecoveryServerMetrics = new ServerMetrics(registry, CloudRecoveryRequests.class, VcrServer.class);
    this.messageFormatMetrics = new MessageFormatMetrics(registry);
    this.findTokenFactoryFactory = findTokenFactoryFactory;
    this.notification = notification;
    this.storeKeyFactory = storageKeyFactory;
    this.enableDataPrefetch = enableDataPrefetch;
    this.storeKeyConverterFactory = storeKeyConverterFactory;
    this.clusterMap = clusterMap;
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
        case TtlUpdateRequest:
          handleTtlUpdateRequest(request);
          break;
        default:
          throw new UnsupportedOperationException("Request type not supported");
      }
    } catch (Exception e) {
      logger.error("Error while handling request " + request + " closing connection", e);
      requestResponseChannel.closeConnection(request);
    }
  }

  @Override
  public void handlePutRequest(Request request) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Request type not supported");
  }

  @Override
  public void handleGetRequest(Request request) throws IOException, InterruptedException {
    GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    Histogram responseQueueTime = null;
    Histogram responseSendTime = null;
    Histogram responseTotalTime = null;
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    boolean isReplicaRequest = getRequest.getClientId().startsWith(GetRequest.Replication_Client_Id_Prefix);
    if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
      cloudRecoveryServerMetrics.getBlobRequestQueueTimeInMs.update(requestQueueTime);
      cloudRecoveryServerMetrics.getBlobRequestRate.mark();
      responseQueueTime = cloudRecoveryServerMetrics.getBlobResponseQueueTimeInMs;
      responseSendTime = cloudRecoveryServerMetrics.getBlobSendTimeInMs;
      responseTotalTime = cloudRecoveryServerMetrics.getBlobTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
      cloudRecoveryServerMetrics.getBlobPropertiesRequestQueueTimeInMs.update(requestQueueTime);
      cloudRecoveryServerMetrics.getBlobPropertiesRequestRate.mark();
      responseQueueTime = cloudRecoveryServerMetrics.getBlobPropertiesResponseQueueTimeInMs;
      responseSendTime = cloudRecoveryServerMetrics.getBlobPropertiesSendTimeInMs;
      responseTotalTime = cloudRecoveryServerMetrics.getBlobPropertiesTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
      cloudRecoveryServerMetrics.getBlobUserMetadataRequestQueueTimeInMs.update(requestQueueTime);
      cloudRecoveryServerMetrics.getBlobUserMetadataRequestRate.mark();
      responseQueueTime = cloudRecoveryServerMetrics.getBlobUserMetadataResponseQueueTimeInMs;
      responseSendTime = cloudRecoveryServerMetrics.getBlobUserMetadataSendTimeInMs;
      responseTotalTime = cloudRecoveryServerMetrics.getBlobUserMetadataTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobInfo) {
      cloudRecoveryServerMetrics.getBlobInfoRequestQueueTimeInMs.update(requestQueueTime);
      cloudRecoveryServerMetrics.getBlobInfoRequestRate.mark();
      responseQueueTime = cloudRecoveryServerMetrics.getBlobInfoResponseQueueTimeInMs;
      responseSendTime = cloudRecoveryServerMetrics.getBlobInfoSendTimeInMs;
      responseTotalTime = cloudRecoveryServerMetrics.getBlobInfoTotalTimeInMs;
    } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.All) {
      if (isReplicaRequest) {
        cloudRecoveryServerMetrics.getBlobAllByReplicaRequestQueueTimeInMs.update(requestQueueTime);
        cloudRecoveryServerMetrics.getBlobAllByReplicaRequestRate.mark();
        responseQueueTime = cloudRecoveryServerMetrics.getBlobAllByReplicaResponseQueueTimeInMs;
        responseSendTime = cloudRecoveryServerMetrics.getBlobAllByReplicaSendTimeInMs;
        responseTotalTime = cloudRecoveryServerMetrics.getBlobAllByReplicaTotalTimeInMs;
      } else {
        cloudRecoveryServerMetrics.getBlobAllRequestQueueTimeInMs.update(requestQueueTime);
        cloudRecoveryServerMetrics.getBlobAllRequestRate.mark();
        responseQueueTime = cloudRecoveryServerMetrics.getBlobAllResponseQueueTimeInMs;
        responseSendTime = cloudRecoveryServerMetrics.getBlobAllSendTimeInMs;
        responseTotalTime = cloudRecoveryServerMetrics.getBlobAllTotalTimeInMs;
      }
    }

    long startTime = SystemTime.getInstance().milliseconds();
    GetResponse response = null;
    try {
      List<Send> messagesToSendList = new ArrayList<Send>(getRequest.getPartitionInfoList().size());
      List<PartitionResponseInfo> partitionResponseInfoList =
          new ArrayList<PartitionResponseInfo>(getRequest.getPartitionInfoList().size());
      for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
        //TODO validate request?
        try {
          EnumSet<StoreGetOptions> storeGetOptions = EnumSet.noneOf(StoreGetOptions.class);
          // Currently only one option is supported.
          if (getRequest.getGetOption() == GetOption.Include_Expired_Blobs) {
            storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Expired);
          }
          if (getRequest.getGetOption() == GetOption.Include_Deleted_Blobs) {
            storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted);
          }
          if (getRequest.getGetOption() == GetOption.Include_All) {
            storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted, StoreGetOptions.Store_Include_Expired);
          }
          List<StoreKey> convertedStoreKeys = getConvertedStoreKeys(partitionRequestInfo.getBlobIds());
          List<StoreKey> dedupedStoreKeys =
              convertedStoreKeys.size() > 1 ? convertedStoreKeys.stream().distinct().collect(Collectors.toList())
                  : convertedStoreKeys;
          Store cloudBlobStore = cloudBackupManager.getPartitionInfo(partitionRequestInfo.getPartition()).getStore();
          StoreInfo info = cloudBlobStore.get(dedupedStoreKeys, storeGetOptions);
          MessageFormatSend blobsToSend =
              new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag(), messageFormatMetrics,
                  storeKeyFactory, enableDataPrefetch);
          PartitionResponseInfo partitionResponseInfo =
              new PartitionResponseInfo(partitionRequestInfo.getPartition(), info.getMessageReadSetInfo(),
                  blobsToSend.getMessageMetadataList());
          messagesToSendList.add(blobsToSend);
          partitionResponseInfoList.add(partitionResponseInfo);
        } catch (StoreException e) {
          boolean logInErrorLevel = false;
          if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found) {
            cloudRecoveryServerMetrics.idNotFoundError.inc();
          } else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired) {
            cloudRecoveryServerMetrics.ttlExpiredError.inc();
          } else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted) {
            cloudRecoveryServerMetrics.idDeletedError.inc();
          } else if (e.getErrorCode() == StoreErrorCodes.Authorization_Failure) {
            cloudRecoveryServerMetrics.getAuthorizationFailure.inc();
          } else {
            cloudRecoveryServerMetrics.unExpectedStoreGetError.inc();
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
            cloudRecoveryServerMetrics.dataCorruptError.inc();
          } else if (e.getErrorCode() == MessageFormatErrorCodes.Unknown_Format_Version) {
            cloudRecoveryServerMetrics.unknownFormatError.inc();
          }
          PartitionResponseInfo partitionResponseInfo = new PartitionResponseInfo(partitionRequestInfo.getPartition(),
              ErrorMapping.getMessageFormatErrorMapping(e.getErrorCode()));
          partitionResponseInfoList.add(partitionResponseInfo);
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
      logger.info("{} {} processingTime {}", getRequest, response, processingTime);
      if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
        cloudRecoveryServerMetrics.getBlobProcessingTimeInMs.update(processingTime);
        cloudRecoveryServerMetrics.updateGetBlobProcessingTimeBySize(response.sizeInBytes(), processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
        cloudRecoveryServerMetrics.getBlobPropertiesProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
        cloudRecoveryServerMetrics.getBlobUserMetadataProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobInfo) {
        cloudRecoveryServerMetrics.getBlobInfoProcessingTimeInMs.update(processingTime);
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.All) {
        if (isReplicaRequest) {
          cloudRecoveryServerMetrics.getBlobAllByReplicaProcessingTimeInMs.update(processingTime);
        } else {
          cloudRecoveryServerMetrics.getBlobAllProcessingTimeInMs.update(processingTime);
          cloudRecoveryServerMetrics.updateGetBlobProcessingTimeBySize(response.sizeInBytes(), processingTime);
        }
      }
    }
    sendGetResponse(requestResponseChannel, response, request, responseQueueTime, responseSendTime, responseTotalTime,
        totalTimeSpent, response.sizeInBytes(), getRequest.getMessageFormatFlag(), cloudRecoveryServerMetrics);
  }

  @Override
  public void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Request type not supported");
  }

  @Override
  public void handleReplicaMetadataRequest(Request request) throws IOException, InterruptedException {
    ReplicaMetadataRequest replicaMetadataRequest;
    try {
      replicaMetadataRequest =
          ReplicaMetadataRequest.readFrom(new DataInputStream(request.getInputStream()), this.clusterMap,
              findTokenFactoryFactory);
    } catch (ReflectiveOperationException roe) {
      logger.error("Error on getting replica token factory", roe);
      throw new IOException("Error on getting replica token factory");
    }
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    cloudRecoveryServerMetrics.replicaMetadataRequestQueueTimeInMs.update(requestQueueTime);
    cloudRecoveryServerMetrics.replicaMetadataRequestRate.mark();

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
        logger.trace("{} Time used to validate metadata request: {}", partitionId,
            (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

        try {
          FindToken findToken = replicaMetadataRequestInfo.getToken();
          String hostName = replicaMetadataRequestInfo.getHostName();
          String replicaPath = replicaMetadataRequestInfo.getReplicaPath();

          partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
          Store cloudBlobStore =
              cloudBackupManager.getPartitionInfo(replicaMetadataRequestInfo.getPartitionId()).getStore();
          FindInfo findInfo =
              cloudBlobStore.findEntriesSince(findToken, replicaMetadataRequest.getMaxTotalSizeOfEntriesInBytes());
          logger.trace("{} Time used to find entry since: {}", partitionId,
              (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

          partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
          long totalBytesRead = findInfo.getFindToken().getBytesRead();
          //TODO fix this
          //replicationManager.updateTotalBytesReadByRemoteReplica(partitionId, hostName, replicaPath, totalBytesRead);
          logger.trace("{} Time used to update total bytes read: {}", partitionId,
              (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

          partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
          logger.trace("{} Time used to get remote replica lag in bytes: {}", partitionId,
              (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              new ReplicaMetadataResponseInfo(partitionId, findInfo.getFindToken(), findInfo.getMessageEntries(),
                  1024 * 1024);//TODO fix this
          if (replicaMetadataResponseInfo.getTotalSizeOfAllMessages()
              > 5 * replicaMetadataRequest.getMaxTotalSizeOfEntriesInBytes()) {
            logger.debug("{} generated a metadata response {} where the cumulative size of messages is {}",
                replicaMetadataRequest, replicaMetadataResponseInfo,
                replicaMetadataResponseInfo.getTotalSizeOfAllMessages());
            cloudRecoveryServerMetrics.replicationResponseMessageSizeTooHigh.inc();
          }
          replicaMetadataResponseList.add(replicaMetadataResponseInfo);
          cloudRecoveryServerMetrics.replicaMetadataTotalSizeOfMessages.update(
              replicaMetadataResponseInfo.getTotalSizeOfAllMessages());
        } catch (StoreException e) {
          logger.error(
              "Store exception on a replica metadata request with error code " + e.getErrorCode() + " for partition "
                  + partitionId, e);
          if (e.getErrorCode() == StoreErrorCodes.IOError) {
            cloudRecoveryServerMetrics.storeIOError.inc();
          } else {
            cloudRecoveryServerMetrics.unExpectedStoreFindEntriesError.inc();
          }
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              new ReplicaMetadataResponseInfo(partitionId, ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
          replicaMetadataResponseList.add(replicaMetadataResponseInfo);
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
      logger.info("{} {} processingTime {}", replicaMetadataRequest, response, processingTime);
      logger.trace("{} {} processingTime {}", replicaMetadataRequest, response, processingTime);
      cloudRecoveryServerMetrics.replicaMetadataRequestProcessingTimeInMs.update(processingTime);
    }

    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(cloudRecoveryServerMetrics.replicaMetadataResponseQueueTimeInMs,
            cloudRecoveryServerMetrics.replicaMetadataSendTimeInMs,
            cloudRecoveryServerMetrics.replicaMetadataTotalTimeInMs, null, null, totalTimeSpent));
  }

  @Override
  public void handleTtlUpdateRequest(Request request) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Request type not supported");
  }

  /**
   * Convert StoreKeys based on {@link StoreKeyConverter}
   * @param storeKeys A list of original storeKeys.
   * @return A list of converted storeKeys.
   */
  private List<StoreKey> getConvertedStoreKeys(List<? extends StoreKey> storeKeys) throws Exception {
    storeKeyConverterFactory.getStoreKeyConverter().dropCache();
    Map<StoreKey, StoreKey> conversionMap = storeKeyConverterFactory.getStoreKeyConverter().convert(storeKeys);
    List<StoreKey> convertedStoreKeys = new ArrayList<>();
    for (StoreKey key : storeKeys) {
      StoreKey convertedKey = conversionMap.get(key);
      convertedStoreKeys.add(convertedKey == null ? key : convertedKey);
    }
    return convertedStoreKeys;
  }

  private void sendGetResponse(RequestResponseChannel requestResponseChannel, GetResponse response, Request request,
      Histogram responseQueueTime, Histogram responseSendTime, Histogram requestTotalTime, long totalTimeSpent,
      long blobSize, MessageFormatFlags flags, ServerMetrics metrics) throws InterruptedException {

    if (blobSize <= ServerMetrics.smallBlob) {
      if (flags == MessageFormatFlags.Blob || flags == MessageFormatFlags.All) {
        if (response.getError() == ServerErrorCode.No_Error) {
          metrics.markGetBlobRequestRateBySize(blobSize);
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                  metrics.getSmallBlobSendTimeInMs, metrics.getSmallBlobTotalTimeInMs, totalTimeSpent));
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
      if (flags == MessageFormatFlags.Blob || flags == MessageFormatFlags.All) {
        if (response.getError() == ServerErrorCode.No_Error) {
          metrics.markGetBlobRequestRateBySize(blobSize);
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                  metrics.getMediumBlobSendTimeInMs, metrics.getMediumBlobTotalTimeInMs, totalTimeSpent));
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
      if (flags == MessageFormatFlags.Blob || flags == MessageFormatFlags.All) {
        if (response.getError() == ServerErrorCode.No_Error) {
          metrics.markGetBlobRequestRateBySize(blobSize);
          requestResponseChannel.sendResponse(response, request,
              new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                  metrics.getLargeBlobSendTimeInMs, metrics.getLargeBlobTotalTimeInMs, totalTimeSpent));
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
}
