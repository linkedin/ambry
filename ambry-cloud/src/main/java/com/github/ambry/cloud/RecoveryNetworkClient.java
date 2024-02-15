/**
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
 */
package com.github.ambry.cloud;

import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.CompositeSend;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link NetworkClient} that get the response for each request from Azure cloud.
 * TODO:
 * recovery token writer - store as json files
 * end-of-partition signal - stop repl
 * fetch data
 * helix add replica - cloud-to-store mgr
 * replica thread transformer header mismatch
 */
public class RecoveryNetworkClient implements NetworkClient {
  private final static Logger logger = LoggerFactory.getLogger(RecoveryNetworkClient.class);
  private final ClusterMap clustermap;
  private final StoreManager storeManager;
  private final ConcurrentHashMap<StoreKey, MessageInfo> messageInfoCache;
  private final AzureBlobLayoutStrategy azureBlobLayoutStrategy;
  private final ClusterMapConfig clusterMapConfig;
  private final AzureCloudConfig azureCloudConfig;
  private final AzureCloudDestinationSync azureSyncClient;
  private final RecoveryNetworkClientCallback clientCallback;
  private final RecoveryMetrics recoveryMetrics;

  class EmptyCallback implements RecoveryNetworkClientCallback {
    public void onListBlobs(ReplicaMetadataRequestInfo request) {}
  }

  public RecoveryNetworkClient(VerifiableProperties properties, MetricRegistry registry, ClusterMap clusterMap,
      StoreManager storeManager, AccountService accountService, RecoveryNetworkClientCallback clientCallback) {
    this.clientCallback = clientCallback == null ? new EmptyCallback() : clientCallback;
    this.clustermap = clusterMap;
    this.storeManager = storeManager;
    this.clusterMapConfig = new ClusterMapConfig(properties);
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.recoveryMetrics = new RecoveryMetrics(registry);
    this.messageInfoCache = new ConcurrentHashMap<>();
    this.azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, azureCloudConfig);
    try {
      this.azureSyncClient = new AzureCloudDestinationSync(properties, registry, clusterMap, accountService);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public RecoveryMetrics getRecoveryMetrics() {
    return recoveryMetrics;
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    List<ResponseInfo> responseInfos = new ArrayList<>();
    for (RequestInfo requestInfo : requestsToSend) {
      // Don't have to serialize the request, we can just use it
      RequestOrResponse request = (RequestOrResponse) requestInfo.getRequest();
      RequestOrResponseType type = request.getRequestType();
      Send send = null;
      try {
        // RecoveryNetworkClient only cares about the ReplicaMetadataRequest and GetRequest
        switch (type) {
          case ReplicaMetadataRequest:
            send = handleReplicaMetadataRequest((ReplicaMetadataRequest) request);
            break;
          case GetRequest:
            send = handleGetRequest((GetRequest) request);
            break;
          default:
            throw new IllegalArgumentException("RecoveryNetworkClient doesn't support request: " + type);
        }
      } catch (Exception exception) {
        recoveryMetrics.recoveryRequestError.inc();
        logger.error("Failed to handle request: type {}", type, exception);
      }
      ResponseInfo responseInfo;
      if (send != null) {
        ByteBuf byteBuf = send.content();
        byteBuf.readLong(); // skip the size of the response.
        responseInfo = new ResponseInfo(requestInfo, null, byteBuf, requestInfo.getReplicaId().getDataNodeId(), false);
      } else {
        responseInfo = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError,
            requestInfo.getReplicaId().getDataNodeId(), null);
      }
      responseInfos.add(responseInfo);
    }
    return responseInfos;
  }

  /**
   * Handle ReplicaMetataRequest and return a response.
   * @param request The {@link ReplicaMetadataRequest} to handle
   * @return A {@link ReplicaMetadataResponse}.
   */
  private ReplicaMetadataResponse handleReplicaMetadataRequest(ReplicaMetadataRequest request) {
    List<ReplicaMetadataResponseInfo> responseList =
        new ArrayList<>(request.getReplicaMetadataRequestInfoList().size());

    // For each partition
    for (ReplicaMetadataRequestInfo rinfo : request.getReplicaMetadataRequestInfoList()) {
      // Get previous azure continuation token
      RecoveryToken prevToken = (RecoveryToken) rinfo.getToken();
      String containerName = azureBlobLayoutStrategy.getClusterAwareAzureContainerName(
          rinfo.getPartitionId().toPathString());
      logger.trace("For container {}, previous RecoveryToken = {}", containerName, prevToken.toString());
      if (prevToken.getAmbryPartitionId() > -1 && prevToken.getAmbryPartitionId() != rinfo.getPartitionId().getId()) {
        logger.error("For partition {}, expected RecoveryToken.ambryPartitionId to be {} but found {}",
            rinfo.getPartitionId().getId(), rinfo.getPartitionId().getId(), prevToken.getAmbryPartitionId());
        recoveryMetrics.recoveryTokenError.inc();
        continue;
      }
      if (prevToken.getAzureStorageContainerId() != null &&
          !prevToken.getAzureStorageContainerId().equals(containerName)) {
        logger.error("For partition {}, expected RecoveryToken.azureContainerId to be {} but found {}",
            rinfo.getPartitionId().getId(), containerName, prevToken.getAzureStorageContainerId());
        recoveryMetrics.recoveryTokenError.inc();
        continue;
      }

      List<MessageInfo> messageInfoList = new ArrayList<>();
      if (prevToken.isEndOfPartition()) {
        // End of partition, nothing more to recover assuming nothing was written during recovery
        logger.trace("Recovery reached end-of-partition for {}", rinfo.getPartitionId());
        responseList.add(
            new ReplicaMetadataResponseInfo(rinfo.getPartitionId(), rinfo.getReplicaType(),
                prevToken, messageInfoList, 0,
                ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId())));
        continue;
      }

      // List N blobs with metadata from Azure storage from prev token position
      ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
          .setDetails(new BlobListDetails().setRetrieveMetadata(true))
          .setMaxResultsPerPage(azureCloudConfig.azureBlobStorageMaxResultsPerPage);
      PagedResponse<BlobItem> response;
      try {
        response = azureSyncClient.createOrGetBlobStore(containerName)
            .listBlobs(listBlobsOptions, null)
            .iterableByPage(prevToken.getToken())
            .iterator()
            .next();
        recoveryMetrics.listBlobsSuccessRate.mark();
        clientCallback.onListBlobs(rinfo);
      } catch (Throwable t) {
        responseList.add(
            new ReplicaMetadataResponseInfo(rinfo.getPartitionId(), rinfo.getReplicaType(),
                ServerErrorCode.IO_Error,
                ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId())));
        logger.error("Failed to list blobs for Ambry partition {} in Azure Storage container {} due to {}",
            rinfo.getPartitionId().getId(), containerName, t);
        recoveryMetrics.listBlobsError.inc();
        continue;
      }

      // Extract ambry metadata
      logger.trace("For container {}, number of blobItems from Azure = {}", containerName, response.getValue().size());
      long bytesRead = 0, blobsRead = 0;
      for (BlobItem blobItem: response.getValue()) {
        MessageInfo messageInfo = getMessageInfo(blobItem);
        if (messageInfo != null) {
          messageInfoList.add(messageInfo);
          messageInfoCache.put(messageInfo.getStoreKey(), messageInfo);
          bytesRead += messageInfo.getSize();
          blobsRead += 1;
        }
      }

      // Save metadata objects
      logger.trace("For container {}, number of messageInfo created = {}", containerName, messageInfoList.size());
      responseList.add(
          new ReplicaMetadataResponseInfo(rinfo.getPartitionId(), rinfo.getReplicaType(),
              new RecoveryToken(prevToken, rinfo.getPartitionId().getId(), containerName,
                  response.getContinuationToken(), blobsRead, bytesRead),
              messageInfoList,
              // Lag metric is useless here as we can't find out size of a container using Azure APIs
              0,
              ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId())));
    }

    // return metadata response
    return new ReplicaMetadataResponse(request.getCorrelationId(), request.getClientId(), ServerErrorCode.No_Error,
        responseList, ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId()));
  }

  /**
   * Create {@link MessageInfo} object from {@link BlobItem} object.
   * @param blobItem {@link BlobItem} object.
   * @return {@link MessageInfo} object.
   */
  private MessageInfo getMessageInfo(BlobItem blobItem) {
    Map<String, String> metadata = blobItem.getMetadata();
    try {
      /**
       * Azure blob metadata contains a field expiryTimeMs.
       * Presence of this field indicates the blob is temporary. ttlUpdated as false in this case.
       * Absence of this field means the blob was either permanent to start with or,
       * transitioned from temporary to permanent through a TTL-UPDATE.
       * There is no way to know which case happened, so just put ttlUpdated as false.
       * expiryTimeMs is included however and replication-code can decide necessary course of action.
       * Same goes for undeleted field and lifeVersion is included for replication-code to decide.
       *
       * We don't upload CRC during backups. This is probably an issue.
       */
      return new MessageInfo(new BlobId(blobItem.getName(), clustermap),
          Long.parseLong(metadata.get(CloudBlobMetadata.FIELD_SIZE)),
          metadata.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME),
          false,
          false,
          Long.parseLong(metadata.containsKey(CloudBlobMetadata.FIELD_EXPIRATION_TIME) ?
              metadata.get(CloudBlobMetadata.FIELD_EXPIRATION_TIME) : String.valueOf(Utils.Infinite_Time)),
          null,
          Short.parseShort(metadata.get(CloudBlobMetadata.FIELD_ACCOUNT_ID)),
          Short.parseShort(metadata.get(CloudBlobMetadata.FIELD_CONTAINER_ID)),
          Long.parseLong(metadata.containsKey(CloudBlobMetadata.FIELD_CREATION_TIME) ?
              metadata.get(CloudBlobMetadata.FIELD_CREATION_TIME) : String.valueOf(System.currentTimeMillis())),
          Short.parseShort(metadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION)));
    } catch (Exception e) {
      recoveryMetrics.metadataError.inc();
      logger.error("Failed to create MessageInfo for blob-id {} from Azure blob metadata due to {}",
          blobItem.getName(), e);
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Downloads blobs from Azure Storage
   * @param request
   * @param downloadToStream
   * @return
   */
  private PartitionResponseInfo downloadBlobs(PartitionRequestInfo request, ByteArrayOutputStream downloadToStream) {
    List<MessageInfo> messageInfoList = new ArrayList<>();
    for (StoreKey storeKey : request.getBlobIds()) {
      // MessageInfo for the given store key is put in the cache in ReplicaMetadataRequest.
      // All store keys in GetRequest should already have MessageInfos in the cache.
      // If replication thread retries, it would send the ReplicaMetadataRequest again before the GetRequest.
      MessageInfo info = messageInfoCache.remove(storeKey);
      if (info == null) {
        // TODO: Log and metric
        logger.error("Failed to find MessageInfo for store key {} at partition {}", storeKey, request.getPartition());
        return new PartitionResponseInfo(request.getPartition(), ServerErrorCode.Blob_Not_Found);
      }

      try {
        azureSyncClient.downloadBlob((BlobId) storeKey, downloadToStream);
        messageInfoList.add(info);
      } catch (CloudStorageException e) {
        logger.error("Failed to download blob {} due to {}", storeKey.getID(), e.getMessage());
        e.printStackTrace();
        return new PartitionResponseInfo(request.getPartition(), ServerErrorCode.IO_Error);
      }
    }
    List<MessageMetadata> messageMetadataList = new ArrayList<>();
    messageInfoList.forEach(info -> messageMetadataList.add(null)); // unused
    return new PartitionResponseInfo(request.getPartition(), messageInfoList, messageMetadataList);
  }

  /**
   * Handle GetRequest downloads blobs from Azure Storage
   * @param request The {@link GetRequest} to handle
   * @return A {@link GetResponse}.
   */
  private GetResponse handleGetRequest(GetRequest request) {
    if (request.getMessageFormatFlag() != MessageFormatFlags.All) {
      throw new IllegalArgumentException("GetRequest should have MessageFormatFlags being ALL");
    }
    List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<>();
    ByteArrayOutputStream downloadToStream = new ByteArrayOutputStream();
    request.getPartitionInfoList().forEach(partitionRequestInfo ->
        partitionResponseInfoList.add(downloadBlobs(partitionRequestInfo, downloadToStream)));
    List<Send> blobsToSend = Collections.singletonList(
        new AllSend(downloadToStream.size(), downloadToStream.toByteArray()));
    return new GetResponse(request.getCorrelationId(), request.getClientId(), partitionResponseInfoList,
        new CompositeSend(blobsToSend), ServerErrorCode.No_Error);
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    logger.info("Warmup is called");
    return 0;
  }

  @Override
  public void wakeup() {
    logger.info("Wakeup is called");
  }

  @Override
  public void close() {
    logger.info("Close is called");
  }

  /**
   * A helper implementation of {@link Send} to return all zeros.
   */
  public static class AllSend extends AbstractByteBufHolder<AllSend> implements Send {
    private final long size;
    private final ByteBuf content;

    public AllSend(long size, byte[] bytes) {
      this.size = size;
      this.content = Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public long writeTo(WritableByteChannel channel) throws IOException {
      return 0;
    }

    @Override
    public boolean isSendComplete() {
      return false;
    }

    @Override
    public long sizeInBytes() {
      return size;
    }

    @Override
    public ByteBuf content() {
      return content;
    }

    @Override
    public AllSend replace(ByteBuf content) {
      return null;
    }
  }
}
