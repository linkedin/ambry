/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link GetPeersHandler}.
 */
public class GetPeersHandlerTest {
  private final TailoredPeersClusterMap clusterMap;
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final GetPeersHandler getPeersHandler;

  public GetPeersHandlerTest() {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    clusterMap = new TailoredPeersClusterMap();
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    getPeersHandler = new GetPeersHandler(clusterMap, securityServiceFactory.getSecurityService(), metrics);
  }

  /**
   * Tests for the good cases where the datanodes are correct. The peers obtained from the response is compared
   * against the ground truth.
   * @throws Exception
   */
  @Test
  public void handleGoodCaseTest() throws Exception {
    for (String datanode : TailoredPeersClusterMap.DATANODE_NAMES) {
      RestRequest restRequest = getRestRequest(datanode);
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = sendRequestGetResponse(restRequest, restResponseChannel);
      assertNotNull("There should be a response", channel);
      Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-type is not as expected", RestUtils.JSON_CONTENT_TYPE,
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      assertEquals("Content-length is not as expected", channel.getSize(),
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      Set<String> expectedPeers = clusterMap.getPeers(datanode);
      Set<String> peersFromResponse = getPeersFromResponse(RestTestUtils.getJsonizedResponseBody(channel));
      assertEquals("Peer list returned does not match expected for " + datanode, expectedPeers, peersFromResponse);
    }
  }

  /**
   * Tests the case where the {@link SecurityService} denies the request.
   * @throws Exception
   */
  @Test
  public void securityServiceDenialTest() throws Exception {
    String msg = "@@expected";
    securityServiceFactory.exceptionToReturn = new IllegalStateException(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.PostProcessRequest;
    verifyFailureWithMsg(msg);
    securityServiceFactory.exceptionToThrow = new IllegalStateException(msg);
    securityServiceFactory.exceptionToReturn = null;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
    verifyFailureWithMsg(msg);
  }

  /**
   * Tests the case where the {@link ClusterMap} throws exceptions.
   * @throws Exception
   */
  @Test
  public void badClusterMapTest() throws Exception {
    String msg = "@@expected";
    clusterMap.exceptionToThrow = new IllegalStateException(msg);
    verifyFailureWithMsg(msg);
  }

  /**
   * Tests cases where the arguments are either missing, incorrect or do no refer to known datanodes.
   * @throws Exception
   */
  @Test
  public void badArgsTest() throws Exception {
    doBadArgsTest(null, "100", RestServiceErrorCode.MissingArgs);
    doBadArgsTest("host", null, RestServiceErrorCode.MissingArgs);
    doBadArgsTest("host", "abc", RestServiceErrorCode.InvalidArgs);
    doBadArgsTest("non-existent-host", "100", RestServiceErrorCode.NotFound);
    String host = TailoredPeersClusterMap.DATANODE_NAMES[0].split(":")[0];
    doBadArgsTest(host, "-1", RestServiceErrorCode.NotFound);
  }

  // helpers
  // general

  /**
   * Gets the peers from the response as a {@link Set} of strings, each of the from host:port.
   * @param response the {@link JSONObject} that contains the peers.
   * @return the peers from the response as a {@link Set} of strings, each of the from host:port.
   * @throws Exception
   */
  static Set<String> getPeersFromResponse(JSONObject response) throws Exception {
    JSONArray peers = response.getJSONArray(GetPeersHandler.PEERS_FIELD_NAME);
    Set<String> peersFromResponse = new HashSet<>(peers.length());
    for (int i = 0; i < peers.length(); i++) {
      JSONObject peer = peers.getJSONObject(i);
      String peerStr =
          peer.getString(GetPeersHandler.NAME_QUERY_PARAM) + ":" + peer.getInt(GetPeersHandler.PORT_QUERY_PARAM);
      peersFromResponse.add(peerStr);
    }
    return peersFromResponse;
  }

  /**
   * Get a {@link RestRequest} that requests for the peers of the given {@code datanode}
   * @param datanode the name of the datanode (host:port) whose peers are required.
   * @return a {@link RestRequest} that requests for the peers of the given {@code datanode}
   * @throws Exception
   */
  private RestRequest getRestRequest(String datanode) throws Exception {
    String[] parts = datanode.split(":");
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY,
        Operations.GET_PEERS + "?" + GetPeersHandler.NAME_QUERY_PARAM + "=" + parts[0] + "&"
            + GetPeersHandler.PORT_QUERY_PARAM + "=" + parts[1]);
    return new MockRestRequest(data, null);
  }

  /**
   * Sends the given {@link RestRequest} to the {@link GetPeersHandler} and waits for the response and returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @return the response body as a {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private ReadableStreamChannel sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    final AtomicReference<ReadableStreamChannel> channelRef = new AtomicReference<>();
    getPeersHandler.handle(restRequest, restResponseChannel, new Callback<ReadableStreamChannel>() {
      @Override
      public void onCompletion(ReadableStreamChannel result, Exception exception) {
        channelRef.set(result);
        exceptionRef.set(exception);
        latch.countDown();
      }
    });
    assertTrue("Latch did not count down in time", latch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
    return channelRef.get();
  }

  /**
   * Verifies that attempting to get peers of any datanode fails with the provided {@code msg}.
   * @param msg the message in the {@link Exception} that will be thrown.
   * @throws Exception
   */
  private void verifyFailureWithMsg(String msg) throws Exception {
    RestRequest restRequest = getRestRequest(TailoredPeersClusterMap.DATANODE_NAMES[0]);
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (Exception e) {
      assertEquals("Unexpected Exception", msg, e.getMessage());
    }
  }

  // badArgsTest() helpers.

  /**
   * Does the test where bad args are provided in the request to {@link GetPeersHandler}.
   * @param name the name of the host whose peers are required. Can be {@code null} if this param should be omitted.
   * @param port the port of the host whose peers are required. Can be {@code null} if this param should be omitted.
   * @param expectedErrorCode the {@link RestServiceErrorCode} expected in response.
   * @throws Exception
   */
  private void doBadArgsTest(String name, String port, RestServiceErrorCode expectedErrorCode) throws Exception {
    StringBuilder uri = new StringBuilder(Operations.GET_PEERS + "?");
    if (name != null) {
      uri.append(GetPeersHandler.NAME_QUERY_PARAM).append("=").append(name);
    }
    if (port != null) {
      if (name != null) {
        uri.append("&");
      }
      uri.append(GetPeersHandler.PORT_QUERY_PARAM).append("=").append(port);
    }
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, uri.toString());
    RestRequest restRequest = new MockRestRequest(data, null);
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }
}

/**
 * Implementation of {@link ClusterMap} that has well known structure and can provide peer lists via the
 * {@link #getPeers(String)} method for verification.
 * <p/>
 * Can also throw exceptions on demand.
 */
class TailoredPeersClusterMap implements ClusterMap {
  static final String[] DATANODE_NAMES = {"host1:100", "host1:200", "host2:100", "host3:100"};

  private static final String DC_NAME = "Datacenter";

  private final Map<String, MockDataNodeId> datanodes = new HashMap<>();
  private final Map<String, Set<String>> peerMap = new HashMap<>();
  private final List<MockPartitionId> partitions = new ArrayList<>();

  RuntimeException exceptionToThrow = null;

  TailoredPeersClusterMap() {
    for (String datanodeName : DATANODE_NAMES) {
      String[] parts = datanodeName.split(":");
      Port port = new Port(Integer.parseInt(parts[1]), PortType.PLAINTEXT);
      List<String> mountPaths = Collections.singletonList("/" + datanodeName);
      MockDataNodeId dataNodeId = new MockDataNodeId(parts[0], Collections.singletonList(port), mountPaths, DC_NAME);
      datanodes.put(datanodeName, dataNodeId);
    }

    List<MockDataNodeId> partNodes = new ArrayList<>();
    partNodes.add(datanodes.get(DATANODE_NAMES[0]));
    partNodes.add(datanodes.get(DATANODE_NAMES[1]));
    partitions.add(new MockPartitionId(0, MockClusterMap.DEFAULT_PARTITION_CLASS, partNodes, 0));
    partNodes.clear();
    partNodes.add(datanodes.get(DATANODE_NAMES[0]));
    partNodes.add(datanodes.get(DATANODE_NAMES[1]));
    partitions.add(new MockPartitionId(1, MockClusterMap.DEFAULT_PARTITION_CLASS, partNodes, 0));
    partNodes.clear();
    partNodes.add(datanodes.get(DATANODE_NAMES[0]));
    partNodes.add(datanodes.get(DATANODE_NAMES[2]));
    partitions.add(new MockPartitionId(2, MockClusterMap.DEFAULT_PARTITION_CLASS, partNodes, 0));
    partNodes.clear();
    partNodes.add(datanodes.get(DATANODE_NAMES[0]));
    partitions.add(new MockPartitionId(3, MockClusterMap.DEFAULT_PARTITION_CLASS, partNodes, 0));
    partNodes.clear();
    partNodes.add(datanodes.get(DATANODE_NAMES[3]));
    partitions.add(new MockPartitionId(4, MockClusterMap.DEFAULT_PARTITION_CLASS, partNodes, 0));

    peerMap.put(DATANODE_NAMES[0], new HashSet<>(Arrays.asList(DATANODE_NAMES[1], DATANODE_NAMES[2])));
    peerMap.put(DATANODE_NAMES[1], new HashSet<>(Arrays.asList(DATANODE_NAMES[0])));
    peerMap.put(DATANODE_NAMES[2], Collections.singleton(DATANODE_NAMES[0]));
    peerMap.put(DATANODE_NAMES[3], Collections.emptySet());
  }

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    throw new IllegalStateException();
  }

  @Override
  public List<? extends PartitionId> getWritablePartitionIds(String partitionClass) {
    throw new IllegalStateException();
  }

  @Override
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    throw new IllegalStateException();
  }

  @Override
  public List<? extends PartitionId> getAllPartitionIds(String partitionClass) {
    throw new IllegalStateException();
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    throw new IllegalStateException();
  }

  @Override
  public byte getLocalDatacenterId() {
    return ClusterMapUtils.UNKNOWN_DATACENTER_ID;
  }

  @Override
  public String getDatacenterName(byte id) {
    return null;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    return datanodes.get(hostname + ":" + port);
  }

  @Override
  public List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    List<ReplicaId> replicaIdsToReturn = new ArrayList<ReplicaId>();
    for (PartitionId partitionId : partitions) {
      List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        if (replicaId.getDataNodeId().getHostname().equals(dataNodeId.getHostname())
            && replicaId.getDataNodeId().getPort() == dataNodeId.getPort()) {
          replicaIdsToReturn.add(replicaId);
        }
      }
    }
    return replicaIdsToReturn;
  }

  @Override
  public List<? extends DataNodeId> getDataNodeIds() {
    throw new IllegalStateException();
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    throw new IllegalStateException();
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    throw new IllegalStateException();
  }

  @Override
  public JSONObject getSnapshot() {
    return null;
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
  }

  @Override
  public void close() {
  }

  @Override
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
    return null;
  }

  Set<String> getPeers(String datanode) {
    return peerMap.get(datanode);
  }
}
