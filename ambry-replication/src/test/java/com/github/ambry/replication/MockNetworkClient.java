package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A mock of {@link NetworkClient} implementation
 */
public class MockNetworkClient implements NetworkClient {
  private final Map<DataNodeId, MockHost> hosts;
  private final Map<DataNodeId, MockConnectionPool.MockConnection> connections;
  private final ClusterMap clusterMap;
  private final int batchSize;
  private final Map<StoreKey, StoreKey> conversionMap;

  private final Map<Integer, RequestInfo> correlationIdToRequestInfos = new HashMap<>();

  public MockNetworkClient(Map<DataNodeId, MockHost> hosts, ClusterMap clusterMap, int batchSize,
      Map<StoreKey, StoreKey> conversionMap) {
    this.hosts = hosts;
    this.clusterMap = clusterMap;
    this.batchSize = batchSize;
    this.connections = new HashMap<>();
    this.conversionMap = conversionMap;
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
        connections.get(dataNodeId).send(requestInfo.getRequest());
        ChannelOutput channelOutput = connections.get(dataNodeId).receive();
        byte[] bytes = Utils.readBytesFromStream(channelOutput.getInputStream(), (int) channelOutput.getStreamSize());
        responseInfos.add(new ResponseInfo(requestInfo, null, Unpooled.wrappedBuffer(bytes)));
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
