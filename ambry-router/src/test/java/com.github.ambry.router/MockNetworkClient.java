/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.Selector;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;


/**
 * A mock class used for verifying whether certain methods of the {@link SocketNetworkClient} gets called in certain
 * tests and how many responses are received by the client.
 */
class MockNetworkClient extends SocketNetworkClient {
  private boolean wokenUp = false;
  private int responseCount = 0;
  private int processedResponseCount = 0;

  /**
   * Construct a MockNetworkClient with mock components.
   */
  MockNetworkClient() throws IOException {
    super(new MockSelector(new MockServerLayout(new MockClusterMap()), null, new MockTime(),
            new NetworkConfig(new VerifiableProperties(new Properties()))), null, new NetworkMetrics(new MetricRegistry()),
        0, 0, 0, new MockTime());
  }

  /**
   * {@inheritDoc}
   */
  MockNetworkClient(Selector selector, NetworkConfig networkConfig, NetworkMetrics networkMetrics,
      int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl, int checkoutTimeoutMs, Time time) {
    super(selector, networkConfig, networkMetrics, maxConnectionsPerPortPlainText, maxConnectionsPerPortSsl,
        checkoutTimeoutMs, time);
  }

  /**
   * Wake up the MockNetworkClient. This simply sets a flag that indicates that the method was invoked.
   */
  @Override
  public void wakeup() {
    wokenUp = true;
  }

  /**
   * This updates the processed request count along with normal send and poll actions.
   * {@inheritDoc}
   */
  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    processedResponseCount = responseCount;
    List<ResponseInfo> responseInfoList = super.sendAndPoll(requestsToSend, requestsToDrop, pollTimeoutMs);
    responseCount += responseInfoList.size();
    return responseInfoList;
  }

  /**
   * This returns the wokenUp status of this object and clears the status.
   * @return true if this MockNetworkClient was woken up since the last call to this method.
   */
  boolean getAndClearWokenUpStatus() {
    boolean ret = wokenUp;
    wokenUp = false;
    return ret;
  }

  /**
   * Get the number of responses received by the client before the current
   * {@link SocketNetworkClient#sendAndPoll(List, Set, int)} call.
   * @return the number of processed responses.
   */
  int getProcessedResponseCount() {
    return processedResponseCount;
  }

  /**
   * Reset the processed response count to zero
   */
  void resetProcessedResponseCount() {
    responseCount = 0;
    processedResponseCount = 0;
  }
}

