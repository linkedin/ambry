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

package com.github.ambry.protocol;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.http2.Http2ServerMetrics;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test creation and shutting down of request handler pool
 */
public class RequestHandlerPoolTest {
  private final static int QUEUE_TIMEOUT_MS = 500;

  @Test
  public void testShutdown() throws InterruptedException {
    Properties properties = new Properties();
    properties.put(NetworkConfig.REQUEST_QUEUE_TIMEOUT_MS, String.valueOf(QUEUE_TIMEOUT_MS));
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()));
    AmbryRequests mockAmbryRequests = mock(AmbryRequests.class);
    doNothing().when(mockAmbryRequests).handleRequests(any());

    // 0. Create request handler pool
    RequestHandlerPool requestHandlerPool = new RequestHandlerPool(1, channel, mockAmbryRequests);

    // 1. Verify request handler is running
    RequestHandler requestHandler = requestHandlerPool.getHandlers()[0];
    assertTrue("Request handler should be running", requestHandler.isRunning());

    // 2. Verify shut down is successful
    requestHandlerPool.shutdown();
    assertFalse("Request handler should be running", requestHandler.isRunning());
  }
}
