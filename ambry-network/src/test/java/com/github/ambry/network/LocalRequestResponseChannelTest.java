/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.network.LocalRequestResponseChannel.LocalChannelRequest;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

import static org.junit.Assert.*;


public class LocalRequestResponseChannelTest {
  private int nextCorrelationId = 0;

  @Test
  public void testSendAndReceive() throws InterruptedException {

    LocalRequestResponseChannel channel = new LocalRequestResponseChannel();
    final int pollTimeoutInMs = 100;
    final int processorId1 = 1;
    final int processorId2 = 2;
    List<ResponseInfo> responseInfoList;

    // 1. verify request is sent and response is received correctly
    NetworkRequest networkRequest1 = new LocalChannelRequest(getRequestInfo(), processorId1);
    channel.sendRequest(networkRequest1);
    assertEquals("Mismatch in request sent and received", networkRequest1, channel.receiveRequest());
    Send response1 = new SocketRequestResponseChannelTest.MockSend();
    channel.sendResponse(response1, networkRequest1, null);
    responseInfoList = channel.receiveResponses(processorId1, pollTimeoutInMs);
    assertEquals("Mismatch in response sent and received", response1, responseInfoList.get(0).getResponse());

    // 2. verify multiple requests and responses
    NetworkRequest networkRequest2 = new LocalChannelRequest(getRequestInfo(), processorId1);
    NetworkRequest networkRequest3 = new LocalChannelRequest(getRequestInfo(), processorId1);
    channel.sendRequest(networkRequest2);
    assertEquals("Mismatch in request sent and received", networkRequest2, channel.receiveRequest());
    channel.sendRequest(networkRequest3);
    assertEquals("Mismatch in request sent and received", networkRequest3, channel.receiveRequest());
    Send response2 = new SocketRequestResponseChannelTest.MockSend();
    channel.sendResponse(response2, networkRequest2, null);
    Send response3 = new SocketRequestResponseChannelTest.MockSend();
    channel.sendResponse(response3, networkRequest3, null);
    responseInfoList = channel.receiveResponses(processorId1, pollTimeoutInMs);
    assertEquals("Mismatch in number of responses", 2, responseInfoList.size());
    assertEquals("Mismatch in response sent and received", response2, responseInfoList.get(0).getResponse());
    assertEquals("Mismatch in response sent and received", response3, responseInfoList.get(1).getResponse());

    // 3. verify requests and responses for multiple processors
    NetworkRequest networkRequest4 = new LocalChannelRequest(getRequestInfo(), processorId1);
    NetworkRequest networkRequest5 = new LocalChannelRequest(getRequestInfo(), processorId2);
    channel.sendRequest(networkRequest4);
    assertEquals("Mismatch in request sent and received", networkRequest4, channel.receiveRequest());
    channel.sendRequest(networkRequest5);
    assertEquals("Mismatch in request sent and received", networkRequest5, channel.receiveRequest());
    Send response4 = new SocketRequestResponseChannelTest.MockSend();
    Send response5 = new SocketRequestResponseChannelTest.MockSend();
    channel.sendResponse(response4, networkRequest4, null);
    channel.sendResponse(response5, networkRequest5, null);
    responseInfoList = channel.receiveResponses(processorId1, pollTimeoutInMs);
    assertEquals("Mismatch in number of responses", 1, responseInfoList.size());
    assertEquals("Mismatch in response sent and received", response4, responseInfoList.get(0).getResponse());
    responseInfoList = channel.receiveResponses(processorId2, pollTimeoutInMs);
    assertEquals("Mismatch in number of responses", 1, responseInfoList.size());
    assertEquals("Mismatch in response sent and received", response5, responseInfoList.get(0).getResponse());

    // 4. test wakeup
    int largePollTimeoutMs = 10000;
    long startTimeMs = System.currentTimeMillis();
    CompletableFuture<List<ResponseInfo>> pollFuture =
        CompletableFuture.supplyAsync(() -> channel.receiveResponses(processorId1, largePollTimeoutMs));
    channel.wakeup(processorId1);
    responseInfoList = pollFuture.join();
    long timeTaken = System.currentTimeMillis() - startTimeMs;
    assertTrue("Took too long to receive responses with wakeup call: " + timeTaken, timeTaken < largePollTimeoutMs);
    assertEquals("Unexpected responses received", Collections.emptyList(), responseInfoList);
    assertEquals("Response list size wrong", 0, responseInfoList.size());
  }

  /**
   * @return a new {@link RequestInfo} with a new correlation ID.
   */
  private RequestInfo getRequestInfo() {
    return new RequestInfo("a", new Port(1, PortType.SSL), new com.github.ambry.network.MockSend(nextCorrelationId++),
        new MockReplicaId(ReplicaType.CLOUD_BACKED), null);
  }
}
