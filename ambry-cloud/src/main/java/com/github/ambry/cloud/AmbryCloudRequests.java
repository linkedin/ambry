/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ErrorMapping;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkRequest;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.IdUndeletedStoreException;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBufInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The request implementation class for handling requests to Cloud store. All requests from cloud router will be handled
 * by this class.
 */
public class AmbryCloudRequests extends AmbryRequests {

  private static final Logger logger = LoggerFactory.getLogger(AmbryCloudRequests.class);

  /**
   * @param storeManager {@link StoreManager} object to get stores for replicas
   * @param requestResponseChannel the {@link RequestResponseChannel} to receive requests and send responses.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param nodeId the data node Id.
   * @param registry the {@link MetricRegistry}
   */
  public AmbryCloudRequests(StoreManager storeManager, RequestResponseChannel requestResponseChannel,
      ClusterMap clusterMap, DataNodeId nodeId, MetricRegistry registry) {
    super(storeManager, requestResponseChannel, clusterMap, nodeId, registry, null, null, null, null, null, null);
  }

  @Override
  public void handlePutRequest(NetworkRequest request) throws InterruptedException {

    if (!(request instanceof LocalRequestResponseChannel.LocalChannelRequest)) {
      throw new IllegalArgumentException("The request must be of LocalChannelRequest type");
    }

    PutRequest receivedRequest;

    // This is a case where handlePutRequest is called when frontends are writing to Azure. In this case, this method
    // is called by request handler threads running within the frontend router itself. So, the request can be directly
    // referenced as java objects without any need for deserialization.
    PutRequest sentRequest =
        (PutRequest) ((LocalRequestResponseChannel.LocalChannelRequest) request).getRequestInfo().getRequest();

    // However, we will create a new PutRequest object to represent the received Put request since the blob content
    // 'buffer' in PutRequest is accessed as 'stream' while writing to Store. Also, crc value for this request
    // would be null since it is only calculated (on the fly) when sending the request to network. It might be okay to
    // use null crc here since the scenario for which we are using crc (i.e. possibility of collisions due to fast
    // replication) as described in this PR https://github.com/linkedin/ambry/pull/549 might not be applicable when
    // frontends are talking to Azure.
    receivedRequest = new PutRequest(sentRequest.getCorrelationId(), sentRequest.getClientId(), sentRequest.getBlobId(),
        sentRequest.getBlobProperties(), sentRequest.getUsermetadata(), sentRequest.getBlobSize(),
        sentRequest.getBlobType(), sentRequest.getBlobEncryptionKey(), new ByteBufInputStream(sentRequest.getBlob()),
        null);

    AtomicReference<PutResponse> response = new AtomicReference<>();
    try {
      ServerErrorCode error =
          validateRequest(receivedRequest.getBlobId().getPartition(), RequestOrResponseType.PutRequest, false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating put request failed with error {} for request {}", error, receivedRequest);
        requestResponseChannel.sendResponse(
            new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(), error), request, null);
      } else {
        MessageFormatWriteSet writeSet = getMessageFormatWriteSet(receivedRequest);
        CloudBlobStore cloudBlobStore =
            (CloudBlobStore) storeManager.getStore(receivedRequest.getBlobId().getPartition());
        //TODO: Pass executor to have async completion stages run on configured thread pool instead of letting them
        // run on default ForkJoinPool
        cloudBlobStore.putAsync(writeSet).whenCompleteAsync((unused, throwable) -> {
          if (throwable != null) {
            Exception ex = Utils.extractFutureExceptionCause(throwable);
            if (ex instanceof StoreException) {
              StoreErrorCodes storeErrorCode = ((StoreException) ex).getErrorCode();
              logger.error("Store exception on a put with error code {} for request {}", storeErrorCode,
                  receivedRequest, ex);
              response.set(new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
                  ErrorMapping.getStoreErrorMapping(storeErrorCode)));
            } else {
              logger.error("Unknown exception on a put for request {}", receivedRequest, ex);
              response.set(new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
                  ServerErrorCode.Unknown_Error));
            }
          } else {
            response.set(new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
                ServerErrorCode.No_Error));
          }

          try {
            requestResponseChannel.sendResponse(response.get(), request, null);
          } catch (InterruptedException ie) {
            logger.warn("Interrupted while enqueuing the response", ie);
          }
        });
      }
    } catch (MessageFormatException | IOException e) {
      logger.error("Unknown exception on a put for request {}", receivedRequest, e);
      requestResponseChannel.sendResponse(
          new PutResponse(receivedRequest.getCorrelationId(), receivedRequest.getClientId(),
              ServerErrorCode.Unknown_Error), request, null);
    }
  }

  @Override
  public void handleGetRequest(NetworkRequest request) throws InterruptedException {

    if (!(request instanceof LocalRequestResponseChannel.LocalChannelRequest)) {
      throw new IllegalArgumentException("The request must be of LocalChannelRequest type");
    }

    GetRequest getRequest;
    // This is a case where handleGetRequest is called when frontends are reading from Azure. In this case, this method
    // is called by request handler threads running within the frontend router itself. So, the request can be directly
    // referenced as java objects without any need for deserialization.
    getRequest = (GetRequest) ((LocalRequestResponseChannel.LocalChannelRequest) request).getRequestInfo().getRequest();

    AtomicReference<GetResponse> response = new AtomicReference<>();
    if (getRequest.getPartitionInfoList().size() > 1) {
      logger.error("Invalid number of messages in GET request received from Frontend {}", getRequest);
      response.set(
          new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), ServerErrorCode.Bad_Request));
    }
    PartitionRequestInfo partitionRequestInfo = getRequest.getPartitionInfoList().iterator().next();
    ServerErrorCode error =
        validateRequest(partitionRequestInfo.getPartition(), RequestOrResponseType.GetRequest, false);
    if (error != ServerErrorCode.No_Error) {
      logger.error("Validating get request failed for partition {} with error {}", partitionRequestInfo.getPartition(),
          error);
      // Send Response
      requestResponseChannel.sendResponse(
          new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), error), request, null);
    } else {
      CloudBlobStore cloudBlobStore = (CloudBlobStore) storeManager.getStore(partitionRequestInfo.getPartition());
      EnumSet<StoreGetOptions> storeGetOptions = getStoreGetOptions(getRequest);
      cloudBlobStore.getAsync(partitionRequestInfo.getBlobIds(), storeGetOptions)
          .whenCompleteAsync((info, throwable) -> {
            if (throwable != null) {
              Exception ex = Utils.extractFutureExceptionCause(throwable);
              if (ex instanceof StoreException) {
                logger.error("Store exception on a get with error code {} for partition {}",
                    ((StoreException) ex).getErrorCode(), partitionRequestInfo.getPartition(), ex);
                response.set(new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
                    ErrorMapping.getStoreErrorMapping(((StoreException) ex).getErrorCode())));
              } else if (ex instanceof MessageFormatException) {
                logger.error("Message format exception on a get with error code {} for partitionRequestInfo {}",
                    ((MessageFormatException) ex).getErrorCode(), partitionRequestInfo, ex);
                response.set(new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
                    ErrorMapping.getMessageFormatErrorMapping(((MessageFormatException) ex).getErrorCode())));
              } else {
                logger.error("Unknown exception for request {}", getRequest, ex);
                response.set(new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
                    ServerErrorCode.Unknown_Error));
              }
            } else {
              MessageFormatSend blobsToSend;
              try {
                blobsToSend = new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag(),
                    messageFormatMetrics, storeKeyFactory);
                PartitionResponseInfo partitionResponseInfo =
                    new PartitionResponseInfo(partitionRequestInfo.getPartition(), info.getMessageReadSetInfo(),
                        blobsToSend.getMessageMetadataList());
                response.set(new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
                    Collections.singletonList(partitionResponseInfo), blobsToSend, ServerErrorCode.No_Error));
              } catch (IOException e) {
                response.set(new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
                    ServerErrorCode.Unknown_Error));
              } catch (MessageFormatException ex) {
                response.set(new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
                    ErrorMapping.getMessageFormatErrorMapping(ex.getErrorCode())));
              }
            }
            try {
              // Send Response
              requestResponseChannel.sendResponse(response.get(), request, null);
            } catch (InterruptedException ie) {
              logger.warn("Interrupted while enqueuing the response", ie);
            }
          });
    }
  }

  @Override
  public void handleDeleteRequest(NetworkRequest request) throws InterruptedException {

    if (!(request instanceof LocalRequestResponseChannel.LocalChannelRequest)) {
      throw new IllegalArgumentException("The request must be of LocalChannelRequest type");
    }

    // This is a case where handleDeleteRequest is called when frontends are talking to Azure. In this case, this method
    // is called by request handler threads running within the frontend router itself. So, the request can be directly
    // referenced as java objects without any need for deserialization.
    DeleteRequest deleteRequest =
        (DeleteRequest) ((LocalRequestResponseChannel.LocalChannelRequest) request).getRequestInfo().getRequest();

    AtomicReference<DeleteResponse> response = new AtomicReference<>();

    ServerErrorCode error =
        validateRequest(deleteRequest.getBlobId().getPartition(), RequestOrResponseType.DeleteRequest, false);
    if (error != ServerErrorCode.No_Error) {
      logger.error("Validating delete request failed with error {} for request {}", error, deleteRequest);
      // Send Response
      requestResponseChannel.sendResponse(
          new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), error), request, null);
    } else {
      BlobId blobId = deleteRequest.getBlobId();
      MessageInfo info =
          new MessageInfo.Builder(deleteRequest.getBlobId(), -1, blobId.getAccountId(), blobId.getContainerId(),
              deleteRequest.getDeletionTimeInMs()).isDeleted(true)
              .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
              .build();
      CloudBlobStore cloudBlobStore = (CloudBlobStore) storeManager.getStore(deleteRequest.getBlobId().getPartition());
      cloudBlobStore.deleteAsync(Collections.singletonList(info)).whenCompleteAsync((unused, throwable) -> {
        if (throwable != null) {
          Exception ex = Utils.extractFutureExceptionCause(throwable);
          if (ex instanceof StoreException) {
            logger.error("Store exception on a delete with error code {} for request {}",
                ((StoreException) ex).getErrorCode(), deleteRequest, ex);
            response.set(new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(),
                ErrorMapping.getStoreErrorMapping(((StoreException) ex).getErrorCode())));
          } else {
            logger.error("Unknown exception for delete request {}", deleteRequest, ex);
            response.set(new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(),
                ServerErrorCode.Unknown_Error));
          }
        } else {
          response.set(new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(),
              ServerErrorCode.No_Error));
        }
        try {
          requestResponseChannel.sendResponse(response.get(), request, null);
        } catch (InterruptedException ie) {
          logger.warn("Interrupted while enqueuing the response", ie);
        }
      });
    }
  }

  @Override
  public void handleTtlUpdateRequest(NetworkRequest request) throws InterruptedException {

    if (!(request instanceof LocalRequestResponseChannel.LocalChannelRequest)) {
      throw new IllegalArgumentException("The request must be of LocalChannelRequest type");
    }

    TtlUpdateRequest updateRequest;

    // This is a case where handleTtlUpdateRequest is called when frontends are talking to Azure. In this case, this method
    // is called by request handler threads running within the frontend router itself. So, the request can be directly
    // referenced as java objects without any need for deserialization.
    updateRequest =
        (TtlUpdateRequest) ((LocalRequestResponseChannel.LocalChannelRequest) request).getRequestInfo().getRequest();

    AtomicReference<TtlUpdateResponse> response = new AtomicReference<>();

    ServerErrorCode error =
        validateRequest(updateRequest.getBlobId().getPartition(), RequestOrResponseType.TtlUpdateRequest, false);
    if (error != ServerErrorCode.No_Error) {
      logger.error("Validating TtlUpdateRequest failed with error {} for request {}", error, updateRequest);
      // Send Response
      requestResponseChannel.sendResponse(
          new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(), error), request, null);
    } else {
      BlobId blobId = updateRequest.getBlobId();
      MessageInfo info = new MessageInfo.Builder(blobId, -1, blobId.getAccountId(), blobId.getContainerId(),
          updateRequest.getOperationTimeInMs()).isTtlUpdated(true)
          .expirationTimeInMs(updateRequest.getExpiresAtMs())
          .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
          .build();
      CloudBlobStore cloudBlobStore = (CloudBlobStore) storeManager.getStore(updateRequest.getBlobId().getPartition());
      cloudBlobStore.updateTtlAsync(Collections.singletonList(info)).whenCompleteAsync((unused, throwable) -> {
        if (throwable != null) {
          Exception ex = Utils.extractFutureExceptionCause(throwable);
          if (ex instanceof StoreException) {
            logger.error("Store exception on a TTL update with error code {} for request {}",
                ((StoreException) ex).getErrorCode(), updateRequest, ex);
            response.set(new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(),
                ErrorMapping.getStoreErrorMapping(((StoreException) ex).getErrorCode())));
          } else {
            logger.error("Unknown exception for TTL update request{}", updateRequest, ex);
            response.set(new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(),
                ServerErrorCode.Unknown_Error));
          }
        } else {
          response.set(new TtlUpdateResponse(updateRequest.getCorrelationId(), updateRequest.getClientId(),
              ServerErrorCode.No_Error));
        }
        try {
          requestResponseChannel.sendResponse(response.get(), request, null);
        } catch (InterruptedException ie) {
          logger.warn("Interrupted while enqueuing the response", ie);
        }
      });
    }
  }

  @Override
  public void handleUndeleteRequest(NetworkRequest request) {

    if (!(request instanceof LocalRequestResponseChannel.LocalChannelRequest)) {
      throw new IllegalArgumentException("The request must be of LocalChannelRequest type");
    }

    UndeleteRequest undeleteRequest;

    // This is a case where handleUndeleteRequest is called when frontends are talking to Azure. In this case, this method
    // is called by request handler threads running within the frontend router itself. So, the request can be directly
    // referenced as java objects without any need for deserialization.
    undeleteRequest =
        (UndeleteRequest) ((LocalRequestResponseChannel.LocalChannelRequest) request).getRequestInfo().getRequest();

    AtomicReference<UndeleteResponse> response = new AtomicReference<>();

    ServerErrorCode error =
        validateRequest(undeleteRequest.getBlobId().getPartition(), RequestOrResponseType.UndeleteRequest, false);
    if (error != ServerErrorCode.No_Error) {
      logger.error("Validating undelete request failed with error {} for request {}", error, undeleteRequest);
      response.set(new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), error));
    } else {
      BlobId blobId = undeleteRequest.getBlobId();
      MessageInfo info = new MessageInfo.Builder(blobId, -1, blobId.getAccountId(), blobId.getContainerId(),
          undeleteRequest.getOperationTimeMs()).isUndeleted(true)
          .lifeVersion(MessageInfo.LIFE_VERSION_FROM_FRONTEND)
          .build();
      CloudBlobStore cloudBlobStore =
          (CloudBlobStore) storeManager.getStore(undeleteRequest.getBlobId().getPartition());
      cloudBlobStore.undeleteAsync(info).whenCompleteAsync((lifeVersion, throwable) -> {
        if (throwable != null) {
          Exception ex = Utils.extractFutureExceptionCause(throwable);
          if (ex instanceof StoreException) {
            logger.error("Store exception on a undelete with error code {} for request {}",
                ((StoreException) ex).getErrorCode(), undeleteRequest, ex);
            StoreException storeException = (StoreException) ex;
            if (storeException.getErrorCode() == StoreErrorCodes.ID_Undeleted) {
              if (ex instanceof IdUndeletedStoreException) {
                response.set(new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
                    ((IdUndeletedStoreException) ex).getLifeVersion(), ServerErrorCode.Blob_Already_Undeleted));
              } else {
                response.set(new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
                    MessageInfo.LIFE_VERSION_FROM_FRONTEND, ServerErrorCode.Blob_Already_Undeleted));
              }
            } else {
              logger.error("Unknown exception for undelete request{}", undeleteRequest, ex);
              response.set(new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
                  ErrorMapping.getStoreErrorMapping(storeException.getErrorCode())));
            }
          } else {
            logger.error("Unknown exception for undelete request{}", undeleteRequest, ex);
            response.set(new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(),
                ServerErrorCode.Unknown_Error));
          }
        } else {
          response.set(
              new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), lifeVersion));
        }
        try {
          requestResponseChannel.sendResponse(response.get(), request, null);
        } catch (InterruptedException ie) {
          logger.warn("Interrupted while enqueuing the response", ie);
        }
      });
    }
  }
}
