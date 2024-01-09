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
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.AbstractByteBufHolder;
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

  public RecoveryNetworkClient(ClusterMap clustermap, FindTokenHelper findTokenHelper, StoreManager storeManager) {
    this.clustermap = clustermap;
    this.findTokenHelper = findTokenHelper;
    this.storeManager = storeManager;
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
    return null;
  }

  /**
   * Create {@link MessageInfo} object from {@link CloudBlobMetadata} object.
   * @param metadata {@link CloudBlobMetadata} object.
   * @return {@link MessageInfo} object.
   * @throws IOException
   */
  private MessageInfo getMessageInfoFromMetadata(CloudBlobMetadata metadata) throws IOException {
    return null;
  }


  protected long getRemoteReplicaLag(Store store, long totalBytesRead) {
    return store.getSizeInBytes() - totalBytesRead;
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
