/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.network;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.utils.Pair;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test {@link CompositeNetworkClient} and {@link CompositeNetworkClientFactory}.
 */
public class CompositeNetworkClientTest {
  int nextCorrelationId = 0;

  @Test
  public void testDelegation() throws IOException {
    Map<ReplicaType, NetworkClientFactory> childFactories = new HashMap<>();
    Pair<NetworkClientFactory, NetworkClient> mockOne = getMocks();
    childFactories.put(ReplicaType.DISK_BACKED, mockOne.getFirst());
    Pair<NetworkClientFactory, NetworkClient> mockTwo = getMocks();
    childFactories.put(ReplicaType.CLOUD_BACKED, mockTwo.getFirst());

    CompositeNetworkClientFactory factory = new CompositeNetworkClientFactory(childFactories);
    factory.getNetworkClient();
    // the second call should call the underlying factories again
    NetworkClient compositeClient = factory.getNetworkClient();
    verify(mockOne.getFirst(), times(2)).getNetworkClient();
    verify(mockTwo.getFirst(), times(2)).getNetworkClient();

    // warmUpConnections
//    reset(mockOne.getSecond());
//    when(mockOne.getSecond().warmUpConnections(any(), any(), any(), any())).thenReturn(25);
//    List<DataNodeId> nodesToWarmUp = Collections.nCopies(3, mock(DataNodeId.class));
//    assertEquals(0, compositeClient.warmUpConnections(nodesToWarmUp, 100, 1000, Collections.emptyList()));
//    verify(mockOne.getSecond()).warmUpConnections(nodesToWarmUp, 100, 1000, Collections.emptyList());

    // sendAndPoll
//    List<RequestInfo> requestsToSend =
//        Arrays.asList(getRequestInfo(ReplicaType.DISK_BACKED), getRequestInfo(ReplicaType.CLOUD_BACKED),
//            getRequestInfo(ReplicaType.DISK_BACKED));
//    // unknown correlation ID
//    Set<Integer> requestsToDrop = Collections.singleton(77);
//    int pollTimeoutMs = 1000;
//    List<ResponseInfo> responsesOne =
//        Collections.singletonList(new ResponseInfo(requestsToSend.get(0), null, Unpooled.EMPTY_BUFFER));
//    when(mockOne.getSecond().sendAndPoll(any(), any(), any())).thenReturn(responsesOne);
//    when(mockTwo.getSecond().sendAndPoll(any(), any(), any())).thenReturn(Collections.emptyList());
//    List<ResponseInfo> responses = compositeClient.sendAndPoll(requestsToSend, requestsToDrop, pollTimeoutMs);
//    verify(mockOne.getSecond()).sendAndPoll(Arrays.asList(requestsToSend.get(0), requestsToSend.get(2)),
//        Collections.emptySet(), pollTimeoutMs);
  }

  private Pair<NetworkClientFactory, NetworkClient> getMocks() throws IOException {
    NetworkClientFactory factory = mock(NetworkClientFactory.class);
    NetworkClient client = mock(NetworkClient.class);
    Pair<NetworkClientFactory, NetworkClient> mocks = new Pair<>(factory, client);
    when(factory.getNetworkClient()).thenReturn(client);
    return mocks;
  }

  private void resetMocks(Pair<NetworkClientFactory, NetworkClient> mocks) throws IOException {
    NetworkClientFactory factory = mocks.getFirst();
    NetworkClient client = mocks.getSecond();
    reset(factory, client);
  }

  private RequestInfo getRequestInfo(ReplicaType replicaType) {
    return new RequestInfo("a", new Port(1, PortType.SSL), new MockSend(nextCorrelationId++),
        new MockReplicaId(replicaType));
  }
}