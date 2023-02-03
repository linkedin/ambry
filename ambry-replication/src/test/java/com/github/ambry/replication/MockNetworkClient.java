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
package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A mock implementation of {@link NetworkClient}.
 */
public class MockNetworkClient implements NetworkClient {
  private final Map<DataNodeId, MockHost> hosts;
  private final Map<DataNodeId, MockConnectionPool.MockConnection> connections;
  private final ClusterMap clusterMap;
  private final FindTokenHelper findTokenHelper;
  private final int batchSize;
  private final Map<Integer, RequestInfo> correlationIdToRequestInfos = new HashMap<>();

  private volatile NetworkClientErrorCode expectedNetworkClientErrorCode = null;
  private volatile ServerErrorCode expectedReplicaMetadataResponseError = ServerErrorCode.No_Error;
  private volatile ServerErrorCode expectedGetResponseError = ServerErrorCode.No_Error;

  public MockNetworkClient(Map<DataNodeId, MockHost> hosts, ClusterMap clusterMap, int batchSize,
      FindTokenHelper findTokenHelper) {
    this.hosts = hosts;
    this.clusterMap = clusterMap;
    this.findTokenHelper = findTokenHelper;
    this.batchSize = batchSize;
    this.connections = new HashMap<>();
  }

  void setExpectedNetworkClientErrorCode(NetworkClientErrorCode code) {
    expectedNetworkClientErrorCode = code;
  }

  void setExpectedReplicaMetadataResponseError(ServerErrorCode code) {
    expectedReplicaMetadataResponseError = code;
  }

  void setExpectedGetResponseError(ServerErrorCode code) {
    expectedGetResponseError = code;
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    List<ResponseInfo> responseInfos = new ArrayList<>();
    // first drop requests
    for (Integer correlationId : requestsToDrop) {
      NetworkClientErrorCode errorCode = NetworkClientErrorCode.TimeoutError;
      RequestInfo requestInfo = correlationIdToRequestInfos.remove(correlationId);
      if (requestInfo != null) {
        responseInfos.add(new ResponseInfo(requestInfo, errorCode, null));
      }
    }

    // Now return what the request submitted last time
    for (RequestInfo requestInfo : correlationIdToRequestInfos.values()) {
      String host = requestInfo.getHost();
      Port port = requestInfo.getPort();
      DataNodeId dataNodeId = clusterMap.getDataNodeId(host, port.getPort());
      try {
        if (expectedNetworkClientErrorCode == null) {
          connections.get(dataNodeId).send(requestInfo.getRequest());
          ChannelOutput channelOutput = connections.get(dataNodeId).receive();
          byte[] bytes = Utils.readBytesFromStream(channelOutput.getInputStream(), (int) channelOutput.getStreamSize());
          if (requestInfo.getRequest()
              .getRequestOrResponseType()
              .equals(RequestOrResponseType.ReplicaMetadataRequest.name())) {
            if (expectedReplicaMetadataResponseError == ServerErrorCode.No_Error) {
              responseInfos.add(new ResponseInfo(requestInfo, null, Unpooled.wrappedBuffer(bytes)));
            } else {
              DataInputStream dis = new NettyByteBufDataInputStream(Unpooled.wrappedBuffer(bytes));
              ReplicaMetadataResponse ogResponse = ReplicaMetadataResponse.readFrom(dis, findTokenHelper, clusterMap);
              ReplicaMetadataResponse newResponse =
                  new ReplicaMetadataResponse(ogResponse.getCorrelationId(), ogResponse.getClientId(),
                      expectedReplicaMetadataResponseError, ogResponse.getVersionId());
              ByteBuf content = newResponse.content();
              content.readLong(); // remove the size
              responseInfos.add(new ResponseInfo(requestInfo, null, content));
              ogResponse.release();
            }
          } else if (requestInfo.getRequest()
              .getRequestOrResponseType()
              .equals(RequestOrResponseType.GetRequest.name())) {
            if (expectedGetResponseError == ServerErrorCode.No_Error) {
              responseInfos.add(new ResponseInfo(requestInfo, null, Unpooled.wrappedBuffer(bytes)));
            } else {
              DataInputStream dis = new NettyByteBufDataInputStream(Unpooled.wrappedBuffer(bytes));
              GetResponse ogResponse = GetResponse.readFrom(dis, clusterMap);
              GetResponse newResponse =
                  new GetResponse(ogResponse.getCorrelationId(), ogResponse.getClientId(), expectedGetResponseError);
              ByteBuf content = newResponse.content();
              content.readLong(); // remove the size
              responseInfos.add(new ResponseInfo(requestInfo, null, content));
              ogResponse.release();
            }
          }
        } else {
          responseInfos.add(new ResponseInfo(requestInfo, expectedNetworkClientErrorCode, null));
        }
      } catch (IOException e) {
        responseInfos.add(new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null));
      }
    }

    correlationIdToRequestInfos.clear();
    // Now adding new requests to the map so we can return next time
    for (RequestInfo requestInfo : requestsToSend) {
      int correlationId = requestInfo.getRequest().getCorrelationId();
      String host = requestInfo.getHost();
      Port port = requestInfo.getPort();
      DataNodeId dataNodeId = clusterMap.getDataNodeId(host, port.getPort());
      if (!hosts.containsKey(dataNodeId)) {
        responseInfos.add(new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null));
      } else {
        if (!connections.containsKey(dataNodeId)) {
          connections.put(dataNodeId, new MockConnectionPool.MockConnection(hosts.get(dataNodeId), batchSize));
        }
        correlationIdToRequestInfos.put(correlationId, requestInfo);
      }
    }

    return responseInfos;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    return 0;
  }

  @Override
  public void wakeup() {

  }

  @Override
  public void close() {

  }
}
