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

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
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
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link NetworkClient} that get the response for each request from Azure cloud.
 */
public class RecoveryNetworkClient implements NetworkClient {
  private final static Logger logger = LoggerFactory.getLogger(RecoveryNetworkClient.class);
  private final ClusterMap clustermap;
  private final FindTokenHelper findTokenHelper;
  private final StoreManager storeManager;
  private final ConcurrentHashMap<StoreKey, MessageInfo> messageInfoCache = new ConcurrentHashMap<>();
  protected final CosmosContainer cosmosContainer;

  public RecoveryNetworkClient(ClusterMap clustermap, FindTokenHelper findTokenHelper, StoreManager storeManager,
      CosmosContainer cosmosContainer) {
    this.clustermap = clustermap;
    this.findTokenHelper = findTokenHelper;
    this.storeManager = storeManager;
    this.cosmosContainer = cosmosContainer;
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    List<ResponseInfo> responseInfos = new ArrayList<>();
    for (RequestInfo requestInfo : requestsToSend) {
      ByteBuf content = requestInfo.getRequest().content();
      RequestOrResponseType type = null;
      Send send = null;
      try {
        content.readLong(); // Skip the size of the requests
        type = RequestOrResponseType.values()[content.readShort()];
        // RecoveryNetworkClient only cares about the ReplicaMetadataRequest and GetRequest
        switch (type) {
          case ReplicaMetadataRequest:
            send = handleReplicaMetadataRequest(content);
            break;
          case GetRequest:
            send = handleGetRequest(content);
            break;
          default:
            throw new IllegalArgumentException("RecoveryNetworkClient doesn't support request: " + type);
        }
      } catch (Exception exception) {
        logger.error("Failed to handle request: type {}", type, exception);
      } finally {
        content.release();
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

  private ReplicaMetadataResponse handleReplicaMetadataRequest(ByteBuf content) throws IOException {
    ReplicaMetadataRequest request =
        ReplicaMetadataRequest.readFrom(new NettyByteBufDataInputStream(content), clustermap, findTokenHelper);
    final String COSMOS_QUERY = "select * from c where c.partitionId = \"%s\"";
    List<ReplicaMetadataResponseInfo> replicaMetadataResponseList =
        new ArrayList<>(request.getReplicaMetadataRequestInfoList().size());
    short replicaMetadataRequestVersion = ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId());
    for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : request.getReplicaMetadataRequestInfoList()) {
      PartitionId partitionId = replicaMetadataRequestInfo.getPartitionId();
      ReplicaType replicaType = replicaMetadataRequestInfo.getReplicaType();
      String partitionPath = String.valueOf(partitionId.getId());
      Store store = storeManager.getStore(partitionId);

      RecoveryToken currRecoveryToken = (RecoveryToken) replicaMetadataRequestInfo.getToken();
      RecoveryToken nextRecoveryToken = new RecoveryToken();
      List<MessageInfo> messageEntries = new ArrayList<>();
      String cosmosQuery = String.format(COSMOS_QUERY, partitionPath);
      CosmosQueryRequestOptions cosmosQueryRequestOptions = new CosmosQueryRequestOptions();
      cosmosQueryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
      // eventual consistency is cheapest
      cosmosQueryRequestOptions.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
      long lastQueryTime = System.currentTimeMillis();
      String queryName = String.join("_", "recovery_query", partitionPath, String.valueOf(lastQueryTime));
      cosmosQueryRequestOptions.setQueryName(queryName);
      logger.trace("QueryName = {} | Sending cosmos query '{}'", queryName, cosmosQuery);
      try {
        long startTime = System.currentTimeMillis();
        int numPages = 0, numItems = 0;
        double requestCharge = 0;
        Iterable<FeedResponse<CloudBlobMetadata>> cloudBlobMetadataIter =
            cosmosContainer.queryItems(cosmosQuery, cosmosQueryRequestOptions, CloudBlobMetadata.class)
                .iterableByPage(currRecoveryToken.getCosmosContinuationToken());

        String firstBlobId = currRecoveryToken.getEarliestBlob(), lastBlobId = currRecoveryToken.getLatestBlob();
        long totalBlobBytesRead = 0, backupStartTime = currRecoveryToken.getBackupStartTimeMs(), backupEndTime =
            currRecoveryToken.getBackupEndTimeMs();

        for (FeedResponse<CloudBlobMetadata> page : cloudBlobMetadataIter) {
          requestCharge += page.getRequestCharge();
          for (CloudBlobMetadata cloudBlobMetadata : page.getResults()) {
            MessageInfo messageInfo = getMessageInfoFromMetadata(cloudBlobMetadata);
            // Adding this MessageInfo in the cache, we can later use it in the GetRequest.
            messageInfoCache.put(messageInfo.getStoreKey(), messageInfo);
            messageEntries.add(messageInfo);
            totalBlobBytesRead += cloudBlobMetadata.getSize();
            if (backupStartTime == -1 || (cloudBlobMetadata.getCreationTime() < backupStartTime)) {
              backupStartTime = cloudBlobMetadata.getCreationTime();
              firstBlobId = cloudBlobMetadata.getId();
            }
            if (backupEndTime == -1 || (backupEndTime < cloudBlobMetadata.getLastUpdateTime() * 1000)) {
              backupEndTime = cloudBlobMetadata.getLastUpdateTime() * 1000;
              lastBlobId = cloudBlobMetadata.getId();
            }
            numItems += !(messageInfo.isDeleted() || messageInfo.isExpired()) ? 1 : 0;
          }
          //if (numItems != page.getResults().size()) {
          //  logger.error("Item count mismatch numItems = {}, page.size = {}, prev_token = {}", numItems,
          //      page.getResults().size(), currRecoveryToken.getCosmosContinuationToken());
          //}
          String nextCosmosContinuationToken = getCosmosContinuationToken(page.getContinuationToken());
          nextRecoveryToken = new RecoveryToken(queryName,
              nextCosmosContinuationToken == null ? currRecoveryToken.getCosmosContinuationToken()
                  : page.getContinuationToken(), currRecoveryToken.getRequestUnits() + page.getRequestCharge(),
              currRecoveryToken.getNumItems() + (currRecoveryToken.isEndOfPartitionReached() ? 0 : numItems),
              currRecoveryToken.getNumBlobBytes() + (currRecoveryToken.isEndOfPartitionReached() ? 0
                  : totalBlobBytesRead), nextCosmosContinuationToken == null, currRecoveryToken.getTokenCreateTime(),
              backupStartTime, backupEndTime, lastQueryTime, firstBlobId, lastBlobId);
          ++numPages;
          long resultFetchtime = System.currentTimeMillis() - startTime;
          logger.trace(
              "Received cosmos query results page = {}, time = {} ms, RU = {}/s, numRows = {}, tokenLen = {}, isTokenNull = {}, isTokenSameAsPrevious = {}",
              queryName, numPages, resultFetchtime, requestCharge, page != null ? page.getResults().size() : "null",
              nextCosmosContinuationToken != null ? nextCosmosContinuationToken.length() : "null",
              nextCosmosContinuationToken != null ? nextCosmosContinuationToken.isEmpty() : "null",
              nextCosmosContinuationToken != null ? nextCosmosContinuationToken.equals(
                  currRecoveryToken.getCosmosContinuationToken()) : "null");

          break;
        }
        replicaMetadataResponseList.add(
            new ReplicaMetadataResponseInfo(partitionId, replicaType, nextRecoveryToken, messageEntries,
                getRemoteReplicaLag(store, totalBlobBytesRead), replicaMetadataRequestVersion));
        // Catching and printing CosmosException does not work. The error is thrown and printed elsewhere.
      } catch (Exception exception) {
        logger.error("[{}] Failed due to {}", queryName, exception);
        // Still sending a response back, but with io error
        replicaMetadataResponseList.add(
            new ReplicaMetadataResponseInfo(partitionId, replicaType, ServerErrorCode.IO_Error,
                replicaMetadataRequestVersion));
      }
    }
    return new ReplicaMetadataResponse(request.getCorrelationId(), request.getClientId(), ServerErrorCode.No_Error,
        replicaMetadataResponseList, replicaMetadataRequestVersion);
  }

  /**
   * Create {@link MessageInfo} object from {@link CloudBlobMetadata} object.
   * @param metadata {@link CloudBlobMetadata} object.
   * @return {@link MessageInfo} object.
   * @throws IOException
   */
  private MessageInfo getMessageInfoFromMetadata(CloudBlobMetadata metadata) throws IOException {
    BlobId blobId = new BlobId(metadata.getId(), clustermap);
    long operationTime = (metadata.getDeletionTime() > 0) ? metadata.getDeletionTime()
        : (metadata.getCreationTime() > 0) ? metadata.getCreationTime() : metadata.getUploadTime();
    boolean isDeleted = metadata.getDeletionTime() > 0;
    boolean isTtlUpdated = false;  // No way to know
    return new MessageInfo(blobId, metadata.getSize(), isDeleted, isTtlUpdated, metadata.getExpirationTime(),
        (short) metadata.getAccountId(), (short) metadata.getContainerId(), operationTime);
  }

  protected String getCosmosContinuationToken(String continuationToken) {
    if (continuationToken == null || continuationToken.isEmpty()) {
      return null;
    }
    try {
      JSONObject continuationTokenJson = new JSONObject(continuationToken);
      // compositeToken = continuationTokenJson.getString("token");
      // compositeToken = compositeToken.substring(compositeToken.indexOf('{'), compositeToken.lastIndexOf('}') + 1).replace('\"', '"');
      return continuationTokenJson.getString("token");
    } catch (Exception e) {
      logger.error("ContinuationToken = {} | failed to getToken due to {} ", continuationToken, e.toString());
    }
    return null;
  }

  protected long getRemoteReplicaLag(Store store, long totalBytesRead) {
    return store.getSizeInBytes() - totalBytesRead;
  }

  /**
   * Handle GetRequest but return fake blob content with all zero value bytes
   * @param content
   * @return
   * @throws IOException
   */
  private GetResponse handleGetRequest(ByteBuf content) throws IOException {
    GetRequest request = GetRequest.readFrom(new NettyByteBufDataInputStream(content), clustermap);
    if (request.getMessageFormatFlag() != MessageFormatFlags.All) {
      throw new IllegalArgumentException("GetRequest should have MessageFormatFlags being ALL");
    }
    List<PartitionResponseInfo> partitionResponseInfos = new ArrayList<>();
    List<Send> blobsToSend = new ArrayList<>();
    for (PartitionRequestInfo partitionRequestInfo : request.getPartitionInfoList()) {
      boolean hasError = false;
      List<MessageInfo> messageInfos = new ArrayList<>();
      List<MessageMetadata> messageMetadatas = new ArrayList<>();
      long allMessageInfoSize = 0;
      for (StoreKey storeKey : partitionRequestInfo.getBlobIds()) {
        // MessageInfo for the given store key is put in the cache in ReplicaMetadataRequest. All store keys in GetRequest
        // should already have MessageInfos in the cache. If replication thread retries, it would send the ReplicaMetadataRequest
        // again before the GetRequest.
        MessageInfo info = messageInfoCache.remove(storeKey);
        if (info != null) {
          messageInfos.add(info);
          messageMetadatas.add(null);
          allMessageInfoSize += info.getSize();
        } else {
          logger.error("Failed to find MessageInfo for store key {} at partition {}", storeKey,
              partitionRequestInfo.getPartition());
          hasError = true;
          break;
        }
      }
      PartitionResponseInfo partitionResponseInfo;
      if (!hasError) {
        partitionResponseInfo =
            new PartitionResponseInfo(partitionRequestInfo.getPartition(), messageInfos, messageMetadatas);
      } else {
        partitionResponseInfo =
            new PartitionResponseInfo(partitionRequestInfo.getPartition(), ServerErrorCode.Blob_Not_Found);
      }
      partitionResponseInfos.add(partitionResponseInfo);
      logger.trace("Create a AllZeroSend for length {} at partition {}", allMessageInfoSize,
          partitionRequestInfo.getPartition());
      blobsToSend.add(new AllZeroSend(allMessageInfoSize));
    }
    return new GetResponse(request.getCorrelationId(), request.getClientId(), partitionResponseInfos,
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
