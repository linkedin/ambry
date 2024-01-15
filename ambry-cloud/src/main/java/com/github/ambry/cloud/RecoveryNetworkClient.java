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
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link NetworkClient} that get the response for each request from Azure cloud.
 */
public class RecoveryNetworkClient implements NetworkClient {
  private final static Logger logger = LoggerFactory.getLogger(RecoveryNetworkClient.class);
  private final ClusterMap clustermap;
  private final StoreManager storeManager;
  private final AzureBlobLayoutStrategy azureBlobLayoutStrategy;
  private final ClusterMapConfig clusterMapConfig;
  private final AzureCloudConfig azureCloudConfig;
  private final AzureCloudDestinationSync azureSyncClient;

  public RecoveryNetworkClient(VerifiableProperties properties, MetricRegistry registry, ClusterMap clusterMap,
      StoreManager storeManager, AccountService accountService) {
    this.clustermap = clusterMap;
    this.storeManager = storeManager;
    this.clusterMapConfig = new ClusterMapConfig(properties);
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, azureCloudConfig);
    try {
      this.azureSyncClient = new AzureCloudDestinationSync(properties, registry, clusterMap, accountService);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
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
        // TODO: emit metric, don't halt replication
        continue;
      }
      if (prevToken.getAzureStorageContainerId() != null && !prevToken.getAzureStorageContainerId().equals(containerName)) {
        logger.error("For partition {}, expected RecoveryToken.azureContainerId to be {} but found {}",
            rinfo.getPartitionId().getId(), containerName, prevToken.getAzureStorageContainerId());
        // TODO: emit metric, don't halt replication
        continue;
      }

      // List N blobs with metadata from Azure storage from prev token position
      ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
          .setDetails(new BlobListDetails().setRetrieveMetadata(true))
          .setMaxResultsPerPage(azureCloudConfig.azureBlobStorageMaxResultsPerPage);
      PagedResponse<BlobItem> response = azureSyncClient.createOrGetBlobStore(containerName)
          .listBlobs(listBlobsOptions, null)
          .iterableByPage(prevToken.getToken())
          .iterator()
          .next();

      // Extract ambry metadata
      logger.trace("For container {}, number of blobItems from Azure = {}", containerName, response.getValue().size());
      List<MessageInfo> messageInfoList = new ArrayList<>();
      long bytesRead = 0, blobsRead = 0;
      for (BlobItem blobItem: response.getValue()) {
        MessageInfo messageInfo = getMessageInfo(blobItem);
        if (messageInfo != null) {
          messageInfoList.add(messageInfo);
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
      // TODO: Add comments for booleans
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
          Long.parseLong(metadata.get(CloudBlobMetadata.FIELD_CREATION_TIME)),
          Short.parseShort(metadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION)));
    } catch (Exception e) {
      // TODO: Emit metric
      logger.error("Failed to create MessageInfo from azure blob metadata for blob-id {} due to {}",
          blobItem.getName(), e.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * Handle GetRequest but return fake blob content with all zero value bytes
   * @param request The {@link GetRequest} to handle
   * @return A {@link GetResponse}.
   * @throws IOException
   */
  private GetResponse handleGetRequest(GetRequest request) throws IOException {
    if (request.getMessageFormatFlag() != MessageFormatFlags.All) {
      throw new IllegalArgumentException("GetRequest should have MessageFormatFlags being ALL");
    }
    // TODO: Implement in next PR
    return null;
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
  public static class AllZeroSend extends AbstractByteBufHolder<AllZeroSend> implements Send {
    private final long size;
    private final ByteBuf content;

    public AllZeroSend(long size) {
      this.size = size;
      this.content = Unpooled.wrappedBuffer(new byte[(int) size]);
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
    public AllZeroSend replace(ByteBuf content) {
      return null;
    }
  }
}
