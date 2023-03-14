/*
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
package com.github.ambry.protocol;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ErrorMapping;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatMetrics;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.LocalRequestResponseChannel.LocalChannelRequest;
import com.github.ambry.network.NetworkRequest;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.Send;
import com.github.ambry.network.ServerNetworkResponseMetrics;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicationAPI;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.IdUndeletedStoreException;
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreKeyJacksonConfig;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */
public class AmbryRequests implements RequestAPI {

  protected StoreManager storeManager;
  protected final ReplicationAPI replicationEngine;
  protected final RequestResponseChannel requestResponseChannel;
  protected final ClusterMap clusterMap;
  protected final DataNodeId currentNode;
  protected final ServerMetrics metrics;
  protected final MessageFormatMetrics messageFormatMetrics;
  protected final FindTokenHelper findTokenHelper;
  protected final NotificationSystem notification;
  protected final StoreKeyFactory storeKeyFactory;
  private final StoreKeyConverterFactory storeKeyConverterFactory;
  protected final ConnectionPool connectionPool;
  protected final MetricRegistry metricRegistry;
  protected final ServerConfig serverConfig;
  protected ThreadLocal<Transformer> transformer;
  protected static final Logger publicAccessLogger = LoggerFactory.getLogger("PublicAccessLogger");
  private static final Logger logger = LoggerFactory.getLogger(AmbryRequests.class);

  private static String ON_DEMAND_REPLICATION_CLIENTID_PREFIX = "replication-ondemand-fetch-";

  public AmbryRequests(StoreManager storeManager, RequestResponseChannel requestResponseChannel, ClusterMap clusterMap,
      DataNodeId nodeId, MetricRegistry registry, ServerMetrics serverMetrics, FindTokenHelper findTokenHelper,
      NotificationSystem operationNotification, ReplicationAPI replicationEngine, StoreKeyFactory storeKeyFactory,
      StoreKeyConverterFactory storeKeyConverterFactory) {
    this(storeManager, requestResponseChannel, clusterMap, nodeId, registry, serverMetrics, findTokenHelper,
        operationNotification, replicationEngine, storeKeyFactory, storeKeyConverterFactory, null, null);
  }

  public AmbryRequests(StoreManager storeManager, RequestResponseChannel requestResponseChannel, ClusterMap clusterMap,
      DataNodeId nodeId, MetricRegistry registry, ServerMetrics serverMetrics, FindTokenHelper findTokenHelper,
      NotificationSystem operationNotification, ReplicationAPI replicationEngine, StoreKeyFactory storeKeyFactory,
      StoreKeyConverterFactory storeKeyConverterFactory, ConnectionPool connectionPool, ServerConfig serverConfig) {
    this.storeManager = storeManager;
    this.requestResponseChannel = requestResponseChannel;
    this.clusterMap = clusterMap;
    this.currentNode = nodeId;
    this.metrics = serverMetrics;
    this.messageFormatMetrics = new MessageFormatMetrics(registry);
    this.findTokenHelper = findTokenHelper;
    this.replicationEngine = replicationEngine;
    this.notification = operationNotification;
    this.storeKeyFactory = storeKeyFactory;
    this.storeKeyConverterFactory = storeKeyConverterFactory;
    this.connectionPool = connectionPool;
    this.metricRegistry = registry;
    this.serverConfig = serverConfig;
    /* All the request handlers share one single AmbryRequests object.
     * But the StoreKeyConverter of the Transformer has a cache which shouldn't be shared among handlers.
     * So use ThreadLocal transformer. */
    this.transformer = ThreadLocal.withInitial(() -> {
      if (serverConfig != null) {
        try {
          StoreKeyConverter keyConverter = storeKeyConverterFactory.getStoreKeyConverter();
          return Utils.getObj(serverConfig.serverMessageTransformer, storeKeyFactory, keyConverter);
        } catch (Exception e) {
          logger.error("Failed to create transformer", e);
        }
      }
      return null;
    });
  }

  @Override
  public void handleRequests(NetworkRequest networkRequest) throws InterruptedException {
    try {
      RequestOrResponseType type;
      if (networkRequest instanceof LocalChannelRequest) {
        RequestOrResponse request =
            (RequestOrResponse) ((LocalChannelRequest) networkRequest).getRequestInfo().getRequest();
        type = request.type;
      } else {
        DataInputStream stream = new DataInputStream(networkRequest.getInputStream());
        type = RequestOrResponseType.values()[stream.readShort()];
      }
      switch (type) {
        case PutRequest:
          handlePutRequest(networkRequest);
          break;
        case GetRequest:
          handleGetRequest(networkRequest);
          break;
        case DeleteRequest:
          handleDeleteRequest(networkRequest);
          break;
        case TtlUpdateRequest:
          handleTtlUpdateRequest(networkRequest);
          break;
        case ReplicaMetadataRequest:
          handleReplicaMetadataRequest(networkRequest);
          break;
        case AdminRequest:
          handleAdminRequest(networkRequest);
          break;
        case UndeleteRequest:
          handleUndeleteRequest(networkRequest);
          break;
        case ReplicateBlobRequest:
          handleReplicateBlobRequest(networkRequest);
          break;
        default:
          throw new UnsupportedOperationException("Request type not supported");
      }
    } catch (Exception e) {
      logger.error("Error while handling request {} closing connection", networkRequest, e);
      requestResponseChannel.closeConnection(networkRequest);
    }
  }

  @Override
  public void dropRequest(NetworkRequest networkRequest) throws InterruptedException {
    try {
      InputStream is = networkRequest.getInputStream();
      DataInputStream dis = is instanceof DataInputStream ? (DataInputStream) is : new DataInputStream(is);
      RequestOrResponseType type = RequestOrResponseType.values()[dis.readShort()];
      RequestOrResponse request;
      Response response;
      long requestProcessingStartTime = SystemTime.getInstance().milliseconds();
      long requestQueueTime = requestProcessingStartTime - networkRequest.getStartTimeInMs();
      switch (type) {
        case PutRequest:
          request = PutRequest.readFrom(dis, clusterMap);
          response =
              new PutResponse(request.getCorrelationId(), request.getClientId(), ServerErrorCode.Retry_After_Backoff);
          break;
        case GetRequest:
          request = GetRequest.readFrom(dis, clusterMap);
          response =
              new GetResponse(request.getCorrelationId(), request.getClientId(), ServerErrorCode.Retry_After_Backoff);
          break;
        case DeleteRequest:
          request = DeleteRequest.readFrom(dis, clusterMap);
          response = new DeleteResponse(request.getCorrelationId(), request.getClientId(),
              ServerErrorCode.Retry_After_Backoff);
          break;
        case TtlUpdateRequest:
          request = TtlUpdateRequest.readFrom(dis, clusterMap);
          response = new TtlUpdateResponse(request.getCorrelationId(), request.getClientId(),
              ServerErrorCode.Retry_After_Backoff);
          break;
        case UndeleteRequest:
          request = UndeleteRequest.readFrom(dis, clusterMap);
          response = new UndeleteResponse(request.getCorrelationId(), request.getClientId(),
              ServerErrorCode.Retry_After_Backoff);
          break;
        case ReplicaMetadataRequest:
          request = ReplicaMetadataRequest.readFrom(dis, clusterMap, findTokenHelper);
          response = new ReplicaMetadataResponse(request.getCorrelationId(), request.getClientId(),
              ServerErrorCode.Retry_After_Backoff,
              ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId()));
          break;
        case AdminRequest:
          request = AdminRequest.readFrom(dis, clusterMap);
          response =
              new AdminResponse(request.getCorrelationId(), request.getClientId(), ServerErrorCode.Retry_After_Backoff);
          break;
        default:
          throw new UnsupportedOperationException("Request type not supported");
      }
      // Log the request and response in public access logs
      long requestProcessingTime = SystemTime.getInstance().milliseconds() - requestProcessingStartTime;
      publicAccessLogger.info("{} {} processingTime {}", request, response, requestProcessingTime);
      // Update common metrics for the request
      RequestMetricsUpdater metricsUpdater =
          new RequestMetricsUpdater(requestQueueTime, requestProcessingTime, 0, 0, true);
      request.accept(metricsUpdater);
      Histogram responseQueueTime = metricsUpdater.getResponseQueueTimeHistogram();
      Histogram responseSendTime = metricsUpdater.getResponseSendTimeHistogram();
      Histogram responseTotalTime = metricsUpdater.getRequestTotalTimeHistogram();
      // Send response
      requestResponseChannel.sendResponse(response, networkRequest,
          new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, responseTotalTime, null, null,
              requestQueueTime));
    } catch (Exception e) {
      logger.error("Error while handling networkRequest " + networkRequest + " closing connection", e);
      requestResponseChannel.closeConnection(networkRequest);
    }
  }

  @Override
  public void handlePutRequest(NetworkRequest request) throws IOException, InterruptedException {
    PutRequest receivedRequest;
    if (request instanceof LocalChannelRequest) {
      // This is a case where handlePutRequest is called when frontends are writing to Azure. In this case, this method
      // is called by request handler threads running within the frontend router itself. So, the request can be directly
      // referenced as java objects without any need for deserialization.
      PutRequest sentRequest = (PutRequest) ((LocalChannelRequest) request).getRequestInfo().getRequest();

      // However, we will create a new PutRequest object to represent the received Put request since the blob content
      // 'buffer' in PutRequest is accessed as 'stream' while writing to Store. Also, crc value for this request
      // would be null since it is only calculated (on the fly) when sending the request to network. It might be okay to
      // use null crc here since the scenario for which we are using crc (i.e. possibility of collisions due to fast
      // replication) as described in this PR https://github.com/linkedin/ambry/pull/549 might not be applicable when
      // frontends are talking to Azure.
      receivedRequest =
          new PutRequest(sentRequest.getCorrelationId(), sentRequest.getClientId(), sentRequest.getBlobId(),
              sentRequest.getBlobProperties(), sentRequest.getUsermetadata(), sentRequest.getBlobSize(),
              sentRequest.getBlobType(), sentRequest.getBlobEncryptionKey(),
              new ByteBufInputStream(sentRequest.getBlob()), null);
    } else {
      InputStream is = request.getInputStream();
      DataInputStream dis = is instanceof DataInputStream ? (DataInputStream) is : new DataInputStream(is);
      receivedRequest = PutRequest.readFrom(dis, clusterMap);
    }

    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    PutResponse response = null;
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      ServerErrorCode error =
          validateRequest(receivedRequest.getBlobId().getPartition(), RequestOrResponseType.PutRequest, false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating put request failed with error {} for request {}", error, receivedRequest);
        response = new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(), error);
      } else {
        MessageFormatWriteSet writeSet = getMessageFormatWriteSet(receivedRequest);
        Store storeToPut = storeManager.getStore(receivedRequest.getBlobId().getPartition());
        storeToPut.put(writeSet);
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
      logger.error("Store exception on a put with error code {} for request {}", e.getErrorCode(), receivedRequest, e);
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
      logger.error("Unknown exception on a put for request {}", receivedRequest, e);
      response = new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
          ServerErrorCode.Unknown_Error);
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", receivedRequest, response, processingTime);
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater =
          new RequestMetricsUpdater(requestQueueTime, processingTime, receivedRequest.getBlobSize(), 0, false);
      receivedRequest.accept(metricsUpdater);
    }
    sendPutResponse(requestResponseChannel, response, request, metrics.putBlobResponseQueueTimeInMs,
        metrics.putBlobSendTimeInMs, metrics.putBlobTotalTimeInMs, totalTimeSpent, receivedRequest.getBlobSize(),
        metrics);
  }

  @Override
  public void handleGetRequest(NetworkRequest request) throws IOException, InterruptedException {
    GetRequest getRequest;
    if (request instanceof LocalChannelRequest) {
      // This is a case where handleGetRequest is called when frontends are reading from Azure. In this case, this method
      // is called by request handler threads running within the frontend router itself. So, the request can be directly
      // referenced as java objects without any need for deserialization.
      getRequest = (GetRequest) ((LocalChannelRequest) request).getRequestInfo().getRequest();
    } else {
      getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    }
    Histogram responseQueueTime;
    Histogram responseSendTime;
    Histogram responseTotalTime;
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    GetResponse response = null;
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      List<Send> messagesToSendList = new ArrayList<>(getRequest.getPartitionInfoList().size());
      List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<>(getRequest.getPartitionInfoList().size());
      for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
        ServerErrorCode error =
            validateRequest(partitionRequestInfo.getPartition(), RequestOrResponseType.GetRequest, false);
        if (error != ServerErrorCode.No_Error) {
          logger.error("Validating get request failed for partition {} with error {}",
              partitionRequestInfo.getPartition(), error);
          PartitionResponseInfo partitionResponseInfo =
              new PartitionResponseInfo(partitionRequestInfo.getPartition(), error);
          partitionResponseInfoList.add(partitionResponseInfo);
        } else {
          try {
            Store storeToGet = storeManager.getStore(partitionRequestInfo.getPartition());
            EnumSet<StoreGetOptions> storeGetOptions = getStoreGetOptions(getRequest);
            List<StoreKey> convertedStoreKeys = getConvertedStoreKeys(partitionRequestInfo.getBlobIds());
            List<StoreKey> dedupedStoreKeys =
                convertedStoreKeys.size() > 1 ? convertedStoreKeys.stream().distinct().collect(Collectors.toList())
                    : convertedStoreKeys;
            StoreInfo info = storeToGet.get(dedupedStoreKeys, storeGetOptions);
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
            logger.error("Message format exception on a get with error code {} for partitionRequestInfo {}",
                e.getErrorCode(), partitionRequestInfo, e);
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
      logger.error("Unknown exception for request {}", getRequest, e);
      response =
          new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), ServerErrorCode.Unknown_Error);
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", getRequest, response, processingTime);
      long responseSize = response != null ? response.sizeInBytes() : 0;
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater =
          new RequestMetricsUpdater(requestQueueTime, processingTime, 0, responseSize, false);
      getRequest.accept(metricsUpdater);
      responseQueueTime = metricsUpdater.getResponseQueueTimeHistogram();
      responseSendTime = metricsUpdater.getResponseSendTimeHistogram();
      responseTotalTime = metricsUpdater.getRequestTotalTimeHistogram();
    }
    sendGetResponse(requestResponseChannel, response, request, responseQueueTime, responseSendTime, responseTotalTime,
        totalTimeSpent, response.sizeInBytes(), getRequest.getMessageFormatFlag(), metrics);
  }

  @Override
  public void handleDeleteRequest(NetworkRequest request) throws IOException, InterruptedException {
    DeleteRequest deleteRequest;
    if (request instanceof LocalChannelRequest) {
      // This is a case where handleDeleteRequest is called when frontends are talking to Azure. In this case, this method
      // is called by request handler threads running within the frontend router itself. So, the request can be directly
      // referenced as java objects without any need for deserialization.
      deleteRequest = (DeleteRequest) ((LocalChannelRequest) request).getRequestInfo().getRequest();
    } else {
      deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    }
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    long startTime = SystemTime.getInstance().milliseconds();
    DeleteResponse response = null;
    try {
      StoreKey convertedStoreKey = getConvertedStoreKeys(Collections.singletonList(deleteRequest.getBlobId())).get(0);
      ServerErrorCode error =
          validateRequest(deleteRequest.getBlobId().getPartition(), RequestOrResponseType.DeleteRequest, false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating delete request failed with error {} for request {}", error, deleteRequest);
        response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), error);
      } else {
        BlobId convertedBlobId = (BlobId) convertedStoreKey;
        MessageInfo info = new MessageInfo.Builder(convertedBlobId, -1, convertedBlobId.getAccountId(),
            convertedBlobId.getContainerId(), deleteRequest.getDeletionTimeInMs()).isDeleted(true)
            .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
            .build();
        Store storeToDelete = storeManager.getStore(deleteRequest.getBlobId().getPartition());
        storeToDelete.delete(Collections.singletonList(info));
        response =
            new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), ServerErrorCode.No_Error);
        if (notification != null) {
          notification.onBlobReplicaDeleted(currentNode.getHostname(), currentNode.getPort(), convertedStoreKey.getID(),
              BlobReplicaSourceType.PRIMARY);
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
      logger.error("Unknown exception for delete request {}", deleteRequest, e);
      response = new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(),
          ServerErrorCode.Unknown_Error);
      metrics.unExpectedStoreDeleteError.inc();
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", deleteRequest, response, processingTime);
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater = new RequestMetricsUpdater(requestQueueTime, processingTime, 0, 0, false);
      deleteRequest.accept(metricsUpdater);
    }
    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.deleteBlobResponseQueueTimeInMs, metrics.deleteBlobSendTimeInMs,
            metrics.deleteBlobTotalTimeInMs, null, null, totalTimeSpent));
  }

  @Override
  public void handleTtlUpdateRequest(NetworkRequest request) throws IOException, InterruptedException {
    TtlUpdateRequest updateRequest;
    if (request instanceof LocalChannelRequest) {
      // This is a case where handleTtlUpdateRequest is called when frontends are talking to Azure. In this case, this method
      // is called by request handler threads running within the frontend router itself. So, the request can be directly
      // referenced as java objects without any need for deserialization.
      updateRequest = (TtlUpdateRequest) ((LocalChannelRequest) request).getRequestInfo().getRequest();
    } else {
      updateRequest = TtlUpdateRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    }
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    long startTime = SystemTime.getInstance().milliseconds();
    TtlUpdateResponse response = null;
    try {
      ServerErrorCode error =
          validateRequest(updateRequest.getBlobId().getPartition(), RequestOrResponseType.TtlUpdateRequest, false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating TtlUpdateRequest failed with error {} for request {}", error, updateRequest);
        response = new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(), error);
      } else {
        BlobId convertedStoreKey =
            (BlobId) getConvertedStoreKeys(Collections.singletonList(updateRequest.getBlobId())).get(0);
        MessageInfo info = new MessageInfo.Builder(convertedStoreKey, -1, convertedStoreKey.getAccountId(),
            convertedStoreKey.getContainerId(), updateRequest.getOperationTimeInMs()).isTtlUpdated(true)
            .expirationTimeInMs(updateRequest.getExpiresAtMs())
            .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
            .build();
        Store store = storeManager.getStore(updateRequest.getBlobId().getPartition());
        store.updateTtl(Collections.singletonList(info));
        response = new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(),
            ServerErrorCode.No_Error);
        if (notification != null) {
          notification.onBlobReplicaUpdated(currentNode.getHostname(), currentNode.getPort(), convertedStoreKey.getID(),
              BlobReplicaSourceType.PRIMARY, UpdateType.TTL_UPDATE, info);
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
        metrics.ttlUpdateAuthorizationFailure.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.Already_Updated) {
        metrics.ttlAlreadyUpdatedError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.Update_Not_Allowed) {
        metrics.ttlUpdateRejectedError.inc();
      } else {
        logInErrorLevel = true;
        metrics.unExpectedStoreTtlUpdateError.inc();
      }
      if (logInErrorLevel) {
        logger.error("Store exception on a TTL update with error code {} for request {}", e.getErrorCode(),
            updateRequest, e);
      } else {
        logger.trace("Store exception on a TTL update with error code {} for request {}", e.getErrorCode(),
            updateRequest, e);
      }
      response = new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(),
          ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    } catch (Exception e) {
      logger.error("Unknown exception for TTL update request {}", updateRequest, e);
      response = new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(),
          ServerErrorCode.Unknown_Error);
      metrics.unExpectedStoreTtlUpdateError.inc();
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", updateRequest, response, processingTime);
      metrics.updateBlobTtlProcessingTimeInMs.update(processingTime);
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater = new RequestMetricsUpdater(requestQueueTime, processingTime, 0, 0, false);
      updateRequest.accept(metricsUpdater);
    }
    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.updateBlobTtlResponseQueueTimeInMs, metrics.updateBlobTtlSendTimeInMs,
            metrics.updateBlobTtlTotalTimeInMs, null, null, totalTimeSpent));
  }

  @Override
  public void handleReplicaMetadataRequest(NetworkRequest request) throws IOException, InterruptedException {
    if (replicationEngine == null) {
      throw new UnsupportedOperationException("Replication not supported on this node.");
    }
    ReplicaMetadataRequest replicaMetadataRequest =
        ReplicaMetadataRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap, findTokenHelper);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;

    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList =
        replicaMetadataRequest.getReplicaMetadataRequestInfoList();
    int partitionCnt = replicaMetadataRequestInfoList.size();
    long startTimeInMs = SystemTime.getInstance().milliseconds();
    ReplicaMetadataResponse response = null;
    try {
      List<ReplicaMetadataResponseInfo> replicaMetadataResponseList = new ArrayList<>(partitionCnt);
      for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : replicaMetadataRequestInfoList) {
        long partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
        PartitionId partitionId = replicaMetadataRequestInfo.getPartitionId();
        ReplicaType replicaType = replicaMetadataRequestInfo.getReplicaType();
        ServerErrorCode error = validateRequest(partitionId, RequestOrResponseType.ReplicaMetadataRequest, false);
        logger.trace("{} Time used to validate metadata request: {}", partitionId,
            (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

        if (error != ServerErrorCode.No_Error) {
          logger.error("Validating replica metadata request failed with error {} for partition {}", error, partitionId);
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              new ReplicaMetadataResponseInfo(partitionId, replicaType, error,
                  ReplicaMetadataResponse.getCompatibleResponseVersion(replicaMetadataRequest.getVersionId()));
          replicaMetadataResponseList.add(replicaMetadataResponseInfo);
        } else {
          try {
            FindToken findToken = replicaMetadataRequestInfo.getToken();
            String hostName = replicaMetadataRequestInfo.getHostName();
            String replicaPath = replicaMetadataRequestInfo.getReplicaPath();
            Store store = storeManager.getStore(partitionId);

            partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
            FindInfo findInfo =
                store.findEntriesSince(findToken, replicaMetadataRequest.getMaxTotalSizeOfEntriesInBytes(), hostName,
                    replicaPath);
            logger.trace("{} Time used to find entry since: {}", partitionId,
                (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

            partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
            long totalBytesRead = findInfo.getFindToken().getBytesRead();
            replicationEngine.updateTotalBytesReadByRemoteReplica(partitionId, hostName, replicaPath, totalBytesRead);
            logger.trace("{} Time used to update total bytes read: {}", partitionId,
                (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

            partitionStartTimeInMs = SystemTime.getInstance().milliseconds();
            logger.trace("{} Time used to get remote replica lag in bytes: {}", partitionId,
                (SystemTime.getInstance().milliseconds() - partitionStartTimeInMs));

            ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
                new ReplicaMetadataResponseInfo(partitionId, replicaType, findInfo.getFindToken(),
                    findInfo.getMessageEntries(), getRemoteReplicaLag(store, totalBytesRead),
                    ReplicaMetadataResponse.getCompatibleResponseVersion(replicaMetadataRequest.getVersionId()));
            if (replicaMetadataResponseInfo.getTotalSizeOfAllMessages()
                > 5 * replicaMetadataRequest.getMaxTotalSizeOfEntriesInBytes()) {
              logger.debug("{} generated a metadata response {} where the cumulative size of messages is {}",
                  replicaMetadataRequest, replicaMetadataResponseInfo,
                  replicaMetadataResponseInfo.getTotalSizeOfAllMessages());
              metrics.replicationResponseMessageSizeTooHigh.inc();
            }
            replicaMetadataResponseList.add(replicaMetadataResponseInfo);
            metrics.replicaMetadataTotalSizeOfMessages.update(replicaMetadataResponseInfo.getTotalSizeOfAllMessages());
          } catch (StoreException e) {
            logger.error("Store exception on a replica metadata request with error code {} for partition {}",
                e.getErrorCode(), partitionId, e);
            if (e.getErrorCode() == StoreErrorCodes.IOError) {
              metrics.storeIOError.inc();
            } else {
              metrics.unExpectedStoreFindEntriesError.inc();
            }
            ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
                new ReplicaMetadataResponseInfo(partitionId, replicaType,
                    ErrorMapping.getStoreErrorMapping(e.getErrorCode()),
                    ReplicaMetadataResponse.getCompatibleResponseVersion(replicaMetadataRequest.getVersionId()));
            replicaMetadataResponseList.add(replicaMetadataResponseInfo);
          }
        }
      }
      response =
          new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(), replicaMetadataRequest.getClientId(),
              ServerErrorCode.No_Error, replicaMetadataResponseList,
              ReplicaMetadataResponse.getCompatibleResponseVersion(replicaMetadataRequest.getVersionId()));
    } catch (Exception e) {
      logger.error("Unknown exception for request {}", replicaMetadataRequest, e);
      response =
          new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(), replicaMetadataRequest.getClientId(),
              ServerErrorCode.Unknown_Error,
              ReplicaMetadataResponse.getCompatibleResponseVersion(replicaMetadataRequest.getVersionId()));
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTimeInMs;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", replicaMetadataRequest, response, processingTime);
      logger.trace("{} {} processingTime {}", replicaMetadataRequest, response, processingTime);
      long responseSizeInBytes = response != null ? response.sizeInBytes() : 0L;
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater =
          new RequestMetricsUpdater(requestQueueTime, processingTime, 0, responseSizeInBytes, false);
      replicaMetadataRequest.accept(metricsUpdater);
    }

    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.replicaMetadataResponseQueueTimeInMs,
            metrics.replicaMetadataSendTimeInMs, metrics.replicaMetadataTotalTimeInMs, null, null, totalTimeSpent));
  }

  /**
   * If the replicateBlob is replicating from this node or the local store has the key already, return true.
   * @param replicateBlobRequest the {@link ReplicateBlobRequest}
   * @return true if replicates from this node or the local store has the key
   */
  private boolean localStoreHasTheKey(ReplicateBlobRequest replicateBlobRequest) throws Exception {
    BlobId blobId = replicateBlobRequest.getBlobId();
    final String remoteHostName = replicateBlobRequest.getSourceHostName();
    final int remoteHostPort = replicateBlobRequest.getSourceHostPort();
    final DataNodeId remoteDataNode = clusterMap.getDataNodeId(remoteHostName, remoteHostPort);
    // the source replica happens to be this node.
    if (remoteDataNode.equals(currentNode)) {
      return true;
    }

    // ReplicateBlob has two modes:
    // 1. write repair mode:
    //   Even the local store has the Blob, we still run the ReplicateBlob.
    //   Depending on the final state of the source and local replica, we may applyTtlUpdate or applyDelete to the local store.
    // 2. non write repair mode:
    //   If the local store has the Blob, do nothing.
    // ON_DEMAND_REPLICATION_TODO: add one configuration to switch between write repair mode and non-write repair mode.

    // Currently we don't enable the write repair. As long as the local store has the Blob, return success immediately.
    // check if local store has the key already
    StoreKey convertedKey = getConvertedStoreKeys(Collections.singletonList(blobId)).get(0);
    Store store = storeManager.getStore(((BlobId) convertedKey).getPartition());
    try {
      store.findKey(convertedKey);
      return true;
    } catch (StoreException e) {
      // it throws e.getErrorCode() == StoreErrorCodes.ID_Not_Found if it doesn't exist.
      return false;
    }
  }

  @Override
  public void handleReplicateBlobRequest(NetworkRequest request) throws IOException, InterruptedException {
    if (connectionPool == null || transformer == null || transformer.get() == null) {
      throw new UnsupportedOperationException("ReplicateBlobRequest is not supported on this node.");
    }

    ReplicateBlobRequest replicateBlobRequest;
    if (request instanceof LocalChannelRequest) {
      // This is a case where handleReplicateBlobRequest is called when frontends are talking to Azure. In this case, this method
      // is called by request handler threads running within the frontend router itself. So, the request can be directly
      // referenced as java objects without any need for deserialization.
      replicateBlobRequest = (ReplicateBlobRequest) ((LocalChannelRequest) request).getRequestInfo().getRequest();
    } else {
      replicateBlobRequest = ReplicateBlobRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    }
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    long startTime = SystemTime.getInstance().milliseconds();
    // Get the parameters from the replicateBlobRequest: the blobId, the remoteHostName and the remoteHostPort.
    BlobId blobId = replicateBlobRequest.getBlobId();
    String remoteHostName = replicateBlobRequest.getSourceHostName();
    int remoteHostPort = replicateBlobRequest.getSourceHostPort();
    GetResponse getResponse = null;
    ServerErrorCode errorCode;
    try {
      if (localStoreHasTheKey(replicateBlobRequest)) {
        logger.info("ReplicateBlobRequest replicated Blob {}, local Store has the Key already, do nothing", blobId);
        errorCode = ServerErrorCode.No_Error;
      } else {
        // get the Blob from the remote replica.
        Pair<ServerErrorCode, GetResponse> getResult = getBlobFromRemoteReplica(replicateBlobRequest);
        errorCode = getResult.getFirst();
        getResponse = getResult.getSecond();

        if (errorCode == ServerErrorCode.No_Error) {
          // getBlobFromRemoteReplicate has checked partitionResponseInfoList's size is 1 and it has one MessageInfo.
          PartitionResponseInfo partitionResponseInfo = getResponse.getPartitionResponseInfoList().get(0);
          List<MessageInfo> messageInfoList = partitionResponseInfo.getMessageInfoList();
          MessageInfo orgMsgInfo = messageInfoList.get(0);

          // Transfer the input stream with transformer
          Message output = MessageSievingInputStream.transferInputStream(Collections.singletonList(transformer.get()),
              getResponse.getInputStream(), orgMsgInfo);
          if (output == null) {
            logger.error("ReplicateBlobRequest transferInputStream {} returned null, {} {} {}", orgMsgInfo,
                remoteHostName, remoteHostPort, blobId);
            errorCode = ServerErrorCode.Unknown_Error;
          } else {
            // write the message to the local store
            MessageFormatWriteSet writeset =
                new MessageFormatWriteSet(output.getStream(), Collections.singletonList(output.getMessageInfo()),
                    false);
            Store store = storeManager.getStore(blobId.getPartition());
            store.put(writeset);

            // also applyTtlUpdate and applyDelete if needed.
            if (orgMsgInfo.isTtlUpdated()) {
              applyTtlUpdate(orgMsgInfo, replicateBlobRequest);
            }
            if (orgMsgInfo.isDeleted()) {
              applyDelete(orgMsgInfo, replicateBlobRequest);
            }
            logger.info("ReplicateBlobRequest replicated Blob {} from remote host {} {}", blobId, remoteHostName,
                remoteHostPort);
            errorCode = ServerErrorCode.No_Error;
          } // if (output == null)
        } else if (errorCode == ServerErrorCode.Blob_Deleted) {
          // If GetBlob with GetOption.Include_All returns Blob_Deleted.
          // it means the remote peer probably only have a delete tombstone for this blob.
          // get the tombstone index entry from the remote replica and force write the delete record.
          Pair<ServerErrorCode, MessageInfo> indexEntryResult =
              getTombStoneFromRemoteReplica(blobId, remoteHostName, remoteHostPort);
          errorCode = indexEntryResult.getFirst();
          MessageInfo indexInfo = indexEntryResult.getSecond();

          if (errorCode == ServerErrorCode.No_Error) {
            Store store = storeManager.getStore(blobId.getPartition());
            // if forceDelete fail due to local store has the key, it throws Already_Exist exception
            store.forceDelete(Collections.singletonList(indexInfo));
            logger.info("ReplicateBlobRequest replicated tombstone {} from remote host {} {} : {}", blobId,
                remoteHostName, remoteHostPort, indexInfo);
          }
        } // if (errorCode == XXX)
      } // if (remoteDataNode.equals(currentNode))
    } catch (StoreException e) { // catch the store write exception
      if (e.getErrorCode() == StoreErrorCodes.Already_Exist) {
        logger.info("ReplicateBlobRequest Blob {} already exists for {}", blobId, replicateBlobRequest);
        errorCode = ServerErrorCode.No_Error;
      } else {
        logger.error("ReplicateBlobRequest unknown exception to replicate {} of {}", blobId, replicateBlobRequest, e);
        errorCode = ServerErrorCode.Unknown_Error;
      }
    } catch (Exception e) {
      // localStoreHasTheKey calls getConvertedStoreKeys which may throw Exception
      logger.error("ReplicateBlobRequest unknown exception to replicate {} of {}", blobId, replicateBlobRequest, e);
      errorCode = ServerErrorCode.Unknown_Error;
    } finally {
      if (getResponse != null && getResponse.getInputStream() instanceof NettyByteBufDataInputStream) {
        // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
        ((NettyByteBufDataInputStream) (getResponse.getInputStream())).getBuffer().release();
      }
    }

    ReplicateBlobResponse response =
        new ReplicateBlobResponse(replicateBlobRequest.getCorrelationId(), replicateBlobRequest.getClientId(),
            errorCode);
    long processingTime = SystemTime.getInstance().milliseconds() - startTime;
    totalTimeSpent += processingTime;
    publicAccessLogger.info("{} {} processingTime {}", replicateBlobRequest, response, processingTime);
    // Update request metrics.
    RequestMetricsUpdater metricsUpdater = new RequestMetricsUpdater(requestQueueTime, processingTime, 0, 0, false);
    replicateBlobRequest.accept(metricsUpdater);
    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.replicateBlobResponseQueueTimeInMs, metrics.replicateBlobSendTimeInMs,
            metrics.replicateBlobTotalTimeInMs, null, null, totalTimeSpent));
  }

  /**
   * Get the Tombstone index entry from the remote replica
   * @param blobId the blob id which we want to get the tombstone record from
   * @param remoteHostName the host name of the remote replica
   * @param remoteHostPort the port of the remote replica
   * @return a pair of the {@link ServerErrorCode} and the tombstone {@link MessageInfo}.
   */
  private Pair<ServerErrorCode, MessageInfo> getTombStoneFromRemoteReplica(BlobId blobId, String remoteHostName,
      int remoteHostPort) {
    DataNodeId remoteDataNode = clusterMap.getDataNodeId(remoteHostName, remoteHostPort);
    if (remoteDataNode == null) {
      logger.error("ReplicateBlobRequest {} couldn't find the remote host {} {} in the clustermap.", blobId,
          remoteHostName, remoteHostPort);
      return new Pair(ServerErrorCode.Replica_Unavailable, null);
    }

    // ON_DEMAND_REPLICATION_TODO: Add configuration as replicationConfig.replicationConnectionPoolCheckoutTimeoutMs
    final int connectionPoolCheckoutTimeoutMs = 1000;
    final String clientId = ON_DEMAND_REPLICATION_CLIENTID_PREFIX + currentNode.getHostname();

    DataInputStream stream = null;
    try {
      // Get the index entries from the remote replica with AdminRequest
      int correlationId = 1;
      AdminRequest adminRequest = new AdminRequest(AdminRequestOrResponseType.BlobIndex, null, correlationId, clientId);
      BlobIndexAdminRequest blobIndexRequest = new BlobIndexAdminRequest(blobId, adminRequest);

      ConnectedChannel connectedChannel =
          connectionPool.checkOutConnection(remoteHostName, remoteDataNode.getPortToConnectTo(),
              connectionPoolCheckoutTimeoutMs);
      ChannelOutput channelOutput = connectedChannel.sendAndReceive(blobIndexRequest);
      stream = channelOutput.getInputStream();
      AdminResponseWithContent adminResponse = AdminResponseWithContent.readFrom(stream);
      if (adminResponse.getError() != ServerErrorCode.No_Error) {
        logger.error("ReplicateBlobRequest failed to get tombstone {} from the remote node {} {} {}", blobId,
            remoteHostName, remoteHostPort, adminResponse.getError());
        return new Pair(adminResponse.getError(), null);
      }

      byte[] jsonBytes = adminResponse.getContent();
      ObjectMapper objectMapper = new ObjectMapper();
      StoreKeyJacksonConfig.setupObjectMapper(objectMapper, new BlobIdFactory(clusterMap));
      Map<String, MessageInfo> messages =
          objectMapper.readValue(jsonBytes, new TypeReference<Map<String, MessageInfo>>() {
          });
      if (messages == null || messages.size() != 1) {
        logger.error("ReplicateBlobRequest adminRequest for {} from the remote node {} {} return {} entries", blobId,
            remoteHostName, remoteHostPort, messages == null ? 0 : messages.size());
        return new Pair<>(ServerErrorCode.Unknown_Error, null);
      }
      MessageInfo info = messages.values().stream().findFirst().get();
      if (info.isDeleted() != true) {
        logger.error("ReplicateBlobRequest adminRequest {} from {} {} returned unexpected entry {}", blobId,
            remoteHostName, remoteHostPort, info);
        return new Pair<>(ServerErrorCode.Unknown_Error, null);
      }

      return new Pair(ServerErrorCode.No_Error, info);
    } catch (Exception e) { // catch the getBlob exception
      logger.error("ReplicateBlobRequest getTombStoneFromRemoteReplica {} from the remote node {} hit exception ",
          blobId, remoteHostName, e);
      return new Pair(ServerErrorCode.Unknown_Error, null);
    } finally {
      if (stream != null && stream instanceof NettyByteBufDataInputStream) {
        // if the InputStream is NettyByteBufDataInputStream based, it's time to release its buffer.
        ((NettyByteBufDataInputStream) stream).getBuffer().release();
      }
    }
  }

  /**
   * Get the Blob from the remote replica
   * @param replicateBlobRequest the {@link ReplicateBlobRequest}
   * @return a pair of the {@link ServerErrorCode} and the {@link GetResponse}.
   */
  private Pair<ServerErrorCode, GetResponse> getBlobFromRemoteReplica(ReplicateBlobRequest replicateBlobRequest) {
    BlobId blobId = replicateBlobRequest.getBlobId();
    String remoteHostName = replicateBlobRequest.getSourceHostName();
    int remoteHostPort = replicateBlobRequest.getSourceHostPort();
    DataNodeId remoteDataNode = clusterMap.getDataNodeId(remoteHostName, remoteHostPort);
    if (remoteDataNode == null) {
      logger.error("ReplicateBlobRequest {} couldn't find the remote host {} {} in the clustermap.", blobId,
          remoteHostName, remoteHostPort);
      return new Pair(ServerErrorCode.Replica_Unavailable, null);
    }

    // ON_DEMAND_REPLICATION_TODO: Add configuration as replicationConfig.replicationConnectionPoolCheckoutTimeoutMs
    final int connectionPoolCheckoutTimeoutMs = 1000;
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    List<BlobId> blobIds = new ArrayList<>();
    blobIds.add(blobId);
    PartitionRequestInfo partitionInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    partitionRequestInfoList.add(partitionInfo);
    final String clientId = ON_DEMAND_REPLICATION_CLIENTID_PREFIX + currentNode.getHostname();

    try {
      // Get the Blob including expired and deleted.
      GetRequest getRequest = new GetRequest(replicateBlobRequest.getCorrelationId(), clientId, MessageFormatFlags.All,
          partitionRequestInfoList, GetOption.Include_All);

      // get the blob from the remote replica
      ConnectedChannel connectedChannel =
          connectionPool.checkOutConnection(remoteHostName, remoteDataNode.getPortToConnectTo(),
              connectionPoolCheckoutTimeoutMs);
      ChannelOutput channelOutput = connectedChannel.sendAndReceive(getRequest);
      GetResponse getResponse = GetResponse.readFrom(channelOutput.getInputStream(), clusterMap);
      if (getResponse.getError() != ServerErrorCode.No_Error) {
        logger.error("ReplicateBlobRequest failed to get blob {} from the remote node {} {} {}", blobId, remoteHostName,
            remoteHostPort, getResponse.getError());
        return new Pair(getResponse.getError(), getResponse);
      }
      if ((getResponse.getPartitionResponseInfoList() == null) || (getResponse.getPartitionResponseInfoList().size()
          == 0)) {
        logger.error("ReplicateBlobRequest {} returned empty list from the remote node {} {} {}", blobId,
            remoteHostName, remoteHostPort, getResponse.getError());
        return new Pair(ServerErrorCode.Unknown_Error, getResponse);
      }

      // only have one partition. And checked at least it has one entry above.
      PartitionResponseInfo partitionResponseInfo = getResponse.getPartitionResponseInfoList().get(0);
      if (partitionResponseInfo.getErrorCode() != ServerErrorCode.No_Error) {
        // the status can be Blob_Deleted or others
        // Since GetOption is Include_All, even it's deleted on the remote replica, we'll still get the PutBlob.
        // One exception is that because of compaction or other reasons, the PutRecord is gone and it returns Blob_Deleted.
        logger.error("ReplicateBlobRequest Blob {} of {} failed with error code {}", blobId, replicateBlobRequest,
            partitionResponseInfo.getErrorCode());
        return new Pair(partitionResponseInfo.getErrorCode(), getResponse);
      }
      List<MessageInfo> messageInfoList = partitionResponseInfo.getMessageInfoList();
      if (messageInfoList == null || messageInfoList.size() != 1) {
        logger.error(
            "ReplicateBlobRequest PartitionResponseInfo response from GetRequest {} {} {} {} {} returned null.",
            partitionResponseInfo, messageInfoList, remoteHostName, remoteHostPort, blobId);
        return new Pair(ServerErrorCode.Blob_Not_Found, getResponse);
      }

      return new Pair(ServerErrorCode.No_Error, getResponse);
    } catch (Exception e) { // catch the getBlob exception
      logger.error("ReplicateBlobRequest getBlob {} from the remote node {} hit exception ", blobId, remoteHostName, e);
      return new Pair(ServerErrorCode.Unknown_Error, null);
    }
  }

  @Override
  public void handleUndeleteRequest(NetworkRequest request) throws IOException, InterruptedException {
    UndeleteRequest undeleteRequest;
    if (request instanceof LocalChannelRequest) {
      // This is a case where handleUndeleteRequest is called when frontends are talking to Azure. In this case, this method
      // is called by request handler threads running within the frontend router itself. So, the request can be directly
      // referenced as java objects without any need for deserialization.
      undeleteRequest = (UndeleteRequest) ((LocalChannelRequest) request).getRequestInfo().getRequest();
    } else {
      undeleteRequest = UndeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    }
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    long startTime = SystemTime.getInstance().milliseconds();
    UndeleteResponse response = null;
    Store storeToUndelete;
    StoreKey convertedStoreKey;
    try {
      convertedStoreKey = getConvertedStoreKeys(Collections.singletonList(undeleteRequest.getBlobId())).get(0);
      ServerErrorCode error =
          validateRequest(undeleteRequest.getBlobId().getPartition(), RequestOrResponseType.UndeleteRequest, false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating undelete request failed with error {} for request {}", error, undeleteRequest);
        response = new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), error);
      } else {
        BlobId convertedBlobId = (BlobId) convertedStoreKey;
        MessageInfo info = new MessageInfo.Builder(convertedBlobId, -1, convertedBlobId.getAccountId(),
            convertedBlobId.getContainerId(), undeleteRequest.getOperationTimeMs()).isUndeleted(true)
            .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
            .build();
        storeToUndelete = storeManager.getStore(undeleteRequest.getBlobId().getPartition());
        short lifeVersion = storeToUndelete.undelete(info);
        response = new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), lifeVersion);
        if (notification != null) {
          notification.onBlobReplicaUndeleted(currentNode.getHostname(), currentNode.getPort(),
              convertedStoreKey.getID(), BlobReplicaSourceType.PRIMARY);
        }
      }
    } catch (StoreException e) {
      boolean logInErrorLevel = false;
      if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found) {
        metrics.idNotFoundError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired) {
        metrics.ttlExpiredError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted_Permanently) {
        metrics.idDeletedError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.Life_Version_Conflict) {
        metrics.lifeVersionConflictError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.ID_Not_Deleted) {
        metrics.idNotDeletedError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.ID_Undeleted) {
        metrics.idUndeletedError.inc();
      } else if (e.getErrorCode() == StoreErrorCodes.Authorization_Failure) {
        metrics.undeleteAuthorizationFailure.inc();
      } else {
        logInErrorLevel = true;
        metrics.unExpectedStoreUndeleteError.inc();
      }
      if (logInErrorLevel) {
        logger.error("Store exception on a undelete with error code {} for request {}", e.getErrorCode(),
            undeleteRequest, e);
      } else {
        logger.trace("Store exception on a undelete with error code {} for request {}", e.getErrorCode(),
            undeleteRequest, e);
      }
      if (e.getErrorCode() == StoreErrorCodes.ID_Undeleted) {
        if (e instanceof IdUndeletedStoreException) {
          response = new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
              ((IdUndeletedStoreException) e).getLifeVersion(), ServerErrorCode.Blob_Already_Undeleted);
        } else {
          response = new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
              MessageInfo.LIFE_VERSION_FROM_FRONTEND, ServerErrorCode.Blob_Already_Undeleted);
        }
      } else {
        response = new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
            ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      }
    } catch (Exception e) {
      logger.error("Unknown exception for undelete request {}", undeleteRequest, e);
      response = new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
          ServerErrorCode.Unknown_Error);
      metrics.unExpectedStoreUndeleteError.inc();
    } finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      publicAccessLogger.info("{} {} processingTime {}", undeleteRequest, response, processingTime);
      // Update request metrics.
      RequestMetricsUpdater metricsUpdater = new RequestMetricsUpdater(requestQueueTime, processingTime, 0, 0, false);
      undeleteRequest.accept(metricsUpdater);
    }
    requestResponseChannel.sendResponse(response, request,
        new ServerNetworkResponseMetrics(metrics.undeleteBlobResponseQueueTimeInMs, metrics.undeleteBlobSendTimeInMs,
            metrics.undeleteBlobTotalTimeInMs, null, null, totalTimeSpent));
  }

  /**
   * Get the formatted messages which needs to be written to Store.
   * @param receivedRequest received Put Request
   * @return {@link MessageFormatWriteSet} that contains the formatted messages which needs to be written to store.
   */
  protected MessageFormatWriteSet getMessageFormatWriteSet(PutRequest receivedRequest)
      throws MessageFormatException, IOException {
    MessageFormatInputStream stream =
        new PutMessageFormatInputStream(receivedRequest.getBlobId(), receivedRequest.getBlobEncryptionKey(),
            receivedRequest.getBlobProperties(), receivedRequest.getUsermetadata(), receivedRequest.getBlobStream(),
            receivedRequest.getBlobSize(), receivedRequest.getBlobType(), (short) 0, receivedRequest.isCompressed);
    BlobProperties properties = receivedRequest.getBlobProperties();
    long expirationTime = Utils.addSecondsToEpochTime(receivedRequest.getBlobProperties().getCreationTimeInMs(),
        properties.getTimeToLiveInSeconds());
    MessageInfo info = new MessageInfo.Builder(receivedRequest.getBlobId(), stream.getSize(), properties.getAccountId(),
        properties.getContainerId(), properties.getCreationTimeInMs()).expirationTimeInMs(expirationTime)
        .crc(receivedRequest.getCrc())
        .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
        .build();
    ArrayList<MessageInfo> infoList = new ArrayList<>();
    infoList.add(info);
    return new MessageFormatWriteSet(stream, infoList, false);
  }

  /**
   *
   * @param getRequest
   * @return
   */
  protected EnumSet<StoreGetOptions> getStoreGetOptions(GetRequest getRequest) {
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
    return storeGetOptions;
  }

  private void sendPutResponse(RequestResponseChannel requestResponseChannel, PutResponse response,
      NetworkRequest request, Histogram responseQueueTime, Histogram responseSendTime, Histogram requestTotalTime,
      long totalTimeSpent, long blobSize, ServerMetrics metrics) throws InterruptedException {
    if (response.getError() == ServerErrorCode.No_Error) {
      metrics.markPutBlobRequestRateBySize(blobSize);
      if (blobSize <= ServerMetrics.smallBlob) {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                metrics.putSmallBlobSendTimeInMs, metrics.putSmallBlobTotalTimeInMs, totalTimeSpent));
      } else if (blobSize <= ServerMetrics.mediumBlob) {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                metrics.putMediumBlobSendTimeInMs, metrics.putMediumBlobTotalTimeInMs, totalTimeSpent));
      } else {
        requestResponseChannel.sendResponse(response, request,
            new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime,
                metrics.putLargeBlobSendTimeInMs, metrics.putLargeBlobTotalTimeInMs, totalTimeSpent));
      }
    } else {
      requestResponseChannel.sendResponse(response, request,
          new ServerNetworkResponseMetrics(responseQueueTime, responseSendTime, requestTotalTime, null, null,
              totalTimeSpent));
    }
  }

  private void sendGetResponse(RequestResponseChannel requestResponseChannel, GetResponse response,
      NetworkRequest request, Histogram responseQueueTime, Histogram responseSendTime, Histogram requestTotalTime,
      long totalTimeSpent, long blobSize, MessageFormatFlags flags, ServerMetrics metrics) throws InterruptedException {

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

  /**
   * Check that the provided partition is valid and writable, and the disk (if any) is available.
   * @param partition the partition to validate.
   * @param requestType the {@link RequestOrResponseType} being validated.
   * @param skipPartitionAndDiskAvailableCheck whether to skip ({@code true}) conditions check for the availability of
   *                                           partition and disk.
   * @return {@link ServerErrorCode#No_Error} error if the partition can be written to, or the corresponding error code
   *         if it cannot.
   */
  protected ServerErrorCode validateRequest(PartitionId partition, RequestOrResponseType requestType,
      boolean skipPartitionAndDiskAvailableCheck) {
    // Check partition is not null
    if (partition == null) {
      metrics.badRequestError.inc();
      return ServerErrorCode.Bad_Request;
    }
    // Ensure if the partition can be written to
    if (requestType.equals(RequestOrResponseType.PutRequest)
        && partition.getPartitionState() == PartitionState.READ_ONLY) {
      metrics.partitionReadOnlyError.inc();
      return ServerErrorCode.Partition_ReadOnly;
    }
    return ServerErrorCode.No_Error;
  }

  /**
   * Convert StoreKeys based on {@link StoreKeyConverter}
   * @param storeKeys A list of original storeKeys.
   * @return A list of converted storeKeys.
   */
  protected List<StoreKey> getConvertedStoreKeys(List<? extends StoreKey> storeKeys) throws Exception {
    storeKeyConverterFactory.getStoreKeyConverter().dropCache();
    Map<StoreKey, StoreKey> conversionMap = storeKeyConverterFactory.getStoreKeyConverter().convert(storeKeys);
    List<StoreKey> convertedStoreKeys = new ArrayList<>();
    for (StoreKey key : storeKeys) {
      StoreKey convertedKey = conversionMap.get(key);
      convertedStoreKeys.add(convertedKey == null ? key : convertedKey);
    }
    return convertedStoreKeys;
  }

  protected long getRemoteReplicaLag(Store store, long totalBytesRead) {
    return store.getSizeInBytes() - totalBytesRead;
  }

  /**
   * Applies a TTL update to the blob described by {@code messageInfo}.
   * @param messageInfo the {@link MessageInfo} that will be transformed into a TTL update
   * @param replicateBlobRequest the {@link ReplicateBlobRequest}
   * @throws StoreException
   */
  private void applyTtlUpdate(MessageInfo messageInfo, ReplicateBlobRequest replicateBlobRequest)
      throws StoreException {
    BlobId blobId = replicateBlobRequest.getBlobId();
    Store store = storeManager.getStore(blobId.getPartition());
    try {
      messageInfo = new MessageInfo.Builder(messageInfo).isTtlUpdated(true).build();
      store.updateTtl(Collections.singletonList(messageInfo));
      logger.info("ReplicateBlobRequest applyTtlUpdate for {} of {} ", blobId, replicateBlobRequest);
    } catch (StoreException e) {
      // The blob may be deleted or updated which is alright
      if (e.getErrorCode() == StoreErrorCodes.ID_Deleted || e.getErrorCode() == StoreErrorCodes.Already_Updated) {
        logger.info("ReplicateBlobRequest applyTtlUpdate for {}, Key already updated: {}", blobId, e.getErrorCode());
      } else {
        logger.error("ReplicateBlobRequest applyTtlUpdate for {} failed with {}", blobId, e.getErrorCode());
        throw e;
      }
    }
  }

  /**
   * Applies a DELETE update to the blob described by {@code messageInfo}.
   * @param messageInfo the {@link MessageInfo} that will be transformed into a Delete update
   * @param replicateBlobRequest the {@link ReplicateBlobRequest}
   * @throws StoreException
   */
  private void applyDelete(MessageInfo messageInfo, ReplicateBlobRequest replicateBlobRequest) throws StoreException {
    BlobId blobId = replicateBlobRequest.getBlobId();
    Store store = storeManager.getStore(blobId.getPartition());
    try {
      messageInfo = new MessageInfo.Builder(messageInfo).isDeleted(true).isUndeleted(false).build();
      store.delete(Collections.singletonList(messageInfo));
      logger.info("ReplicateBlobRequest applyDelete for {} of {} ", blobId, replicateBlobRequest);
    } catch (StoreException e) {
      // The blob may be deleted or updated which is alright
      if (e.getErrorCode() == StoreErrorCodes.ID_Deleted || e.getErrorCode() == StoreErrorCodes.Life_Version_Conflict) {
        logger.info("ReplicateBlobRequest applyDelete for {}, Key already updated: {}", blobId, e.getErrorCode());
      } else {
        logger.error("ReplicateBlobRequest applyDelete for {} failed with {}", blobId, e.getErrorCode());
        throw e;
      }
    }
  }

  /**
   * Used to update common metrics for the incoming request.
   */
  public class RequestMetricsUpdater implements RequestVisitor {

    private final long requestQueueTime;
    private final long requestProcessingTime;
    private final long requestBlobSize;
    private final long responseBlobSize;
    private final boolean isRequestDropped;
    private Histogram responseQueueTimeHistogram;
    private Histogram responseSendTimeHistogram;
    private Histogram requestTotalTimeHistogram;

    public RequestMetricsUpdater(long requestQueueTime, long requestProcessingTime, long requestBlobSize,
        long responseBlobSize, boolean isRequestDropped) {
      this.requestQueueTime = requestQueueTime;
      this.requestProcessingTime = requestProcessingTime;
      this.requestBlobSize = requestBlobSize;
      this.responseBlobSize = responseBlobSize;
      this.isRequestDropped = isRequestDropped;
    }

    @Override
    public void visit(PutRequest putRequest) {
      metrics.putBlobRequestQueueTimeInMs.update(requestQueueTime);
      metrics.putBlobRequestRate.mark();
      metrics.putBlobProcessingTimeInMs.update(requestProcessingTime);
      metrics.updatePutBlobProcessingTimeBySize(requestBlobSize, requestProcessingTime);
      responseQueueTimeHistogram = metrics.putBlobResponseQueueTimeInMs;
      responseSendTimeHistogram = metrics.putBlobSendTimeInMs;
      requestTotalTimeHistogram = metrics.putBlobTotalTimeInMs;
      if (isRequestDropped) {
        metrics.putBlobDroppedRate.mark();
        metrics.totalRequestDroppedRate.mark();
      }
    }

    @Override
    public void visit(GetRequest getRequest) {
      boolean isReplicaRequest = getRequest.getClientId().startsWith(GetRequest.Replication_Client_Id_Prefix);
      if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
        metrics.getBlobRequestQueueTimeInMs.update(requestQueueTime);
        metrics.getBlobRequestRate.mark();
        metrics.getBlobProcessingTimeInMs.update(requestProcessingTime);
        metrics.updateGetBlobProcessingTimeBySize(responseBlobSize, requestProcessingTime);
        responseQueueTimeHistogram = metrics.getBlobResponseQueueTimeInMs;
        responseSendTimeHistogram = metrics.getBlobSendTimeInMs;
        requestTotalTimeHistogram = metrics.getBlobTotalTimeInMs;
        if (isRequestDropped) {
          metrics.getBlobDroppedRate.mark();
          metrics.totalRequestDroppedRate.mark();
        }
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
        metrics.getBlobPropertiesRequestQueueTimeInMs.update(requestQueueTime);
        metrics.getBlobPropertiesRequestRate.mark();
        metrics.getBlobPropertiesProcessingTimeInMs.update(requestProcessingTime);
        responseQueueTimeHistogram = metrics.getBlobPropertiesResponseQueueTimeInMs;
        responseSendTimeHistogram = metrics.getBlobPropertiesSendTimeInMs;
        requestTotalTimeHistogram = metrics.getBlobPropertiesTotalTimeInMs;
        if (isRequestDropped) {
          metrics.getBlobPropertiesDroppedRate.mark();
          metrics.totalRequestDroppedRate.mark();
        }
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
        metrics.getBlobUserMetadataRequestQueueTimeInMs.update(requestQueueTime);
        metrics.getBlobUserMetadataRequestRate.mark();
        metrics.getBlobUserMetadataProcessingTimeInMs.update(requestProcessingTime);
        responseQueueTimeHistogram = metrics.getBlobUserMetadataResponseQueueTimeInMs;
        responseSendTimeHistogram = metrics.getBlobUserMetadataSendTimeInMs;
        requestTotalTimeHistogram = metrics.getBlobUserMetadataTotalTimeInMs;
        if (isRequestDropped) {
          metrics.getBlobUserMetadataDroppedRate.mark();
          metrics.totalRequestDroppedRate.mark();
        }
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobInfo) {
        metrics.getBlobInfoRequestQueueTimeInMs.update(requestQueueTime);
        metrics.getBlobInfoRequestRate.mark();
        metrics.getBlobInfoProcessingTimeInMs.update(requestProcessingTime);
        responseQueueTimeHistogram = metrics.getBlobInfoResponseQueueTimeInMs;
        responseSendTimeHistogram = metrics.getBlobInfoSendTimeInMs;
        requestTotalTimeHistogram = metrics.getBlobInfoTotalTimeInMs;
        if (isRequestDropped) {
          metrics.getBlobInfoDroppedRate.mark();
          metrics.totalRequestDroppedRate.mark();
        }
      } else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.All) {
        if (isReplicaRequest) {
          metrics.getBlobAllByReplicaRequestQueueTimeInMs.update(requestQueueTime);
          metrics.getBlobAllByReplicaRequestRate.mark();
          metrics.getBlobAllByReplicaProcessingTimeInMs.update(requestProcessingTime);
          // client id now has dc name at the end, for example: ClientId=replication-fetch-abc.example.com[dc1]
          String[] clientStrs = getRequest.getClientId().split("\\[");
          if (clientStrs.length > 1) {
            String clientDc = clientStrs[1].substring(0, clientStrs[1].length() - 1);
            if (!currentNode.getDatacenterName().equals(clientDc)) {
              metrics.updateCrossColoFetchBytesRate(clientDc, responseBlobSize);
            }
          }
          responseQueueTimeHistogram = metrics.getBlobAllByReplicaResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.getBlobAllByReplicaSendTimeInMs;
          requestTotalTimeHistogram = metrics.getBlobAllByReplicaTotalTimeInMs;
          if (isRequestDropped) {
            metrics.getBlobAllByReplicaDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
        } else {
          metrics.getBlobAllRequestQueueTimeInMs.update(requestQueueTime);
          metrics.getBlobAllRequestRate.mark();
          metrics.getBlobAllProcessingTimeInMs.update(requestProcessingTime);
          metrics.updateGetBlobProcessingTimeBySize(responseBlobSize, requestProcessingTime);
          responseQueueTimeHistogram = metrics.getBlobAllResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.getBlobAllSendTimeInMs;
          requestTotalTimeHistogram = metrics.getBlobAllTotalTimeInMs;
          if (isRequestDropped) {
            metrics.getBlobAllDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
        }
      }
    }

    @Override
    public void visit(TtlUpdateRequest ttlUpdateRequest) {
      metrics.updateBlobTtlRequestQueueTimeInMs.update(requestQueueTime);
      metrics.updateBlobTtlRequestRate.mark();
      metrics.updateBlobTtlProcessingTimeInMs.update(requestProcessingTime);
      responseQueueTimeHistogram = metrics.updateBlobTtlResponseQueueTimeInMs;
      responseSendTimeHistogram = metrics.updateBlobTtlSendTimeInMs;
      requestTotalTimeHistogram = metrics.updateBlobTtlTotalTimeInMs;
      if (isRequestDropped) {
        metrics.updateBlobTtlDroppedRate.mark();
        metrics.totalRequestDroppedRate.mark();
      }
    }

    @Override
    public void visit(DeleteRequest deleteRequest) {
      metrics.deleteBlobRequestQueueTimeInMs.update(requestQueueTime);
      metrics.deleteBlobRequestRate.mark();
      metrics.deleteBlobProcessingTimeInMs.update(requestProcessingTime);
      responseQueueTimeHistogram = metrics.deleteBlobResponseQueueTimeInMs;
      responseSendTimeHistogram = metrics.deleteBlobSendTimeInMs;
      requestTotalTimeHistogram = metrics.deleteBlobTotalTimeInMs;
      if (isRequestDropped) {
        metrics.deleteBlobDroppedRate.mark();
        metrics.totalRequestDroppedRate.mark();
      }
    }

    @Override
    public void visit(UndeleteRequest undeleteRequest) {
      metrics.undeleteBlobRequestQueueTimeInMs.update(requestQueueTime);
      metrics.undeleteBlobRequestRate.mark();
      metrics.undeleteBlobProcessingTimeInMs.update(requestProcessingTime);
      responseQueueTimeHistogram = metrics.undeleteBlobResponseQueueTimeInMs;
      responseSendTimeHistogram = metrics.undeleteBlobSendTimeInMs;
      requestTotalTimeHistogram = metrics.undeleteBlobTotalTimeInMs;
      if (isRequestDropped) {
        metrics.undeleteBlobDroppedRate.mark();
        metrics.totalRequestDroppedRate.mark();
      }
    }

    @Override
    public void visit(ReplicaMetadataRequest replicaMetadataRequest) {
      metrics.replicaMetadataRequestQueueTimeInMs.update(requestQueueTime);
      metrics.replicaMetadataRequestRate.mark();
      metrics.replicaMetadataRequestProcessingTimeInMs.update(requestProcessingTime);
      // client id now has dc name at the end, for example: ClientId=replication-metadata-abc.example.com[dc1]
      String[] clientStrs = replicaMetadataRequest.getClientId().split("\\[");
      if (clientStrs.length > 1) {
        String clientDc = clientStrs[1].substring(0, clientStrs[1].length() - 1);
        if (!currentNode.getDatacenterName().equals(clientDc)) {
          metrics.updateCrossColoMetadataExchangeBytesRate(clientDc, responseBlobSize);
        }
      }
      responseQueueTimeHistogram = metrics.replicaMetadataResponseQueueTimeInMs;
      responseSendTimeHistogram = metrics.replicaMetadataSendTimeInMs;
      requestTotalTimeHistogram = metrics.replicaMetadataTotalTimeInMs;
      if (isRequestDropped) {
        metrics.replicaMetadataDroppedRate.mark();
        metrics.totalRequestDroppedRate.mark();
      }
    }

    @Override
    public void visit(ReplicateBlobRequest replicateBlobRequest) {
      metrics.replicateBlobRequestQueueTimeInMs.update(requestQueueTime);
      metrics.replicateBlobRequestRate.mark();
      metrics.replicateBlobProcessingTimeInMs.update(requestProcessingTime);
      responseQueueTimeHistogram = metrics.replicateBlobResponseQueueTimeInMs;
      responseSendTimeHistogram = metrics.replicateBlobSendTimeInMs;
      requestTotalTimeHistogram = metrics.replicateBlobTotalTimeInMs;
      if (isRequestDropped) {
        metrics.replicateBlobDroppedRate.mark();
        metrics.totalRequestDroppedRate.mark();
      }
    }

    @Override
    public void visit(AdminRequest adminRequest) {
      switch (adminRequest.getType()) {
        case TriggerCompaction:
          metrics.triggerCompactionRequestQueueTimeInMs.update(requestQueueTime);
          metrics.triggerCompactionRequestRate.mark();
          metrics.triggerCompactionRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.triggerCompactionResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.triggerCompactionResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.triggerCompactionRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.triggerCompactionDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case RequestControl:
          metrics.requestControlRequestQueueTimeInMs.update(requestQueueTime);
          metrics.requestControlRequestRate.mark();
          metrics.requestControlRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.requestControlResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.requestControlResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.requestControlRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.requestControlDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case ReplicationControl:
          metrics.replicationControlRequestQueueTimeInMs.update(requestQueueTime);
          metrics.replicationControlRequestRate.mark();
          metrics.replicationControlRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.replicationControlResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.replicationControlResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.replicationControlRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.replicationControlDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case CatchupStatus:
          metrics.catchupStatusRequestQueueTimeInMs.update(requestQueueTime);
          metrics.catchupStatusRequestRate.mark();
          metrics.catchupStatusRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.catchupStatusResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.catchupStatusResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.catchupStatusRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.catchupStatusDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case BlobStoreControl:
          metrics.blobStoreControlRequestQueueTimeInMs.update(requestQueueTime);
          metrics.blobStoreControlRequestRate.mark();
          metrics.blobStoreControlRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.blobStoreControlResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.blobStoreControlResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.blobStoreControlRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.blobStoreControlDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case HealthCheck:
          metrics.healthCheckRequestQueueTimeInMs.update(requestQueueTime);
          metrics.healthCheckRequestRate.mark();
          metrics.healthCheckRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.healthCheckResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.healthCheckResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.healthCheckRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.healthCheckDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case BlobIndex:
          metrics.blobIndexRequestQueueTimeInMs.update(requestQueueTime);
          metrics.blobIndexRequestRate.mark();
          metrics.blobIndexRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.blobIndexResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.blobIndexResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.blobIndexRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.blobIndexDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
        case ForceDelete:
          metrics.forceDeleteRequestQueueTimeInMs.update(requestQueueTime);
          metrics.forceDeleteRequestRate.mark();
          metrics.forceDeleteRequestProcessingTimeInMs.update(requestProcessingTime);
          responseQueueTimeHistogram = metrics.forceDeleteResponseQueueTimeInMs;
          responseSendTimeHistogram = metrics.forceDeleteResponseSendTimeInMs;
          requestTotalTimeHistogram = metrics.forceDeleteRequestTotalTimeInMs;
          if (isRequestDropped) {
            metrics.forceDeleteDroppedRate.mark();
            metrics.totalRequestDroppedRate.mark();
          }
          break;
      }
    }

    /**
     * Get the histogram object used for tracking response queue time. This should be called only after corresponding
     * visit(Request request) method is invoked.
     * @return {@link Histogram} associated with tracking the response queue times of the request.
     */
    public Histogram getResponseQueueTimeHistogram() {
      return responseQueueTimeHistogram;
    }

    /**
     * Get the histogram object used for tracking response send times. This should be called only after corresponding
     * visit(Request request) method is invoked.
     * @return {@link Histogram} associated with tracking the response send times of the request.
     */
    public Histogram getResponseSendTimeHistogram() {
      return responseSendTimeHistogram;
    }

    /**
     * Get the histogram object used for tracking request total time. This should be called only after corresponding
     * visit(Request request) method is invoked.
     * @return {@link Histogram} associated with tracking the request total times of the request.
     */
    public Histogram getRequestTotalTimeHistogram() {
      return requestTotalTimeHistogram;
    }
  }
}
