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
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.utils.MockTime;
import java.io.IOException;


class MockNetworkClient extends NetworkClient {
  boolean wokenUp = false;

  MockNetworkClient()
      throws IOException {
    super(new MockSelector(new MockServerLayout(new MockClusterMap()), null, new MockTime()), null,
        new NetworkMetrics(new MetricRegistry()), 0, 0, 0, new MockTime());
  }

  @Override
  public void wakeup() {
    wokenUp = true;
  }

  boolean getAndClearWokenUpStatus() {
    boolean ret = wokenUp;
    wokenUp = false;
    return ret;
  }
}

