/**
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
 */

package com.github.ambry.network.http2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for HTTP/2 Client
 */
public class Http2ClientMetrics {

  public final Histogram http2ConnectionAcquireTime;
  public final Histogram http2StreamAcquireTime;
  public final Histogram http2FirstStreamAcquireTime;
  public final Histogram http2RegularStreamAcquireTime;
  public final Histogram http2StreamWriteAndFlushTime;
  public final Histogram http2StreamRoundTripTime;
  public final Histogram http2StreamFirstToLastFrameTime;
  public final Histogram http2StreamFirstToAllFrameReadyTime;
  public final Histogram http2ClientSendTime;
  public final Histogram http2ClientSendAndPollTime;
  public final Histogram http2ResponseFrameCount;

  public final Counter http2NewPoolCount;
  public final Counter http2NewConnectionCount;
  public final Counter http2NewConnectionFailureCount;
  public final Counter http2NewStreamCount;
  public final Counter http2StreamAcquireFailureCount;
  public final Counter http2StreamSlipAcquireCount;
  public final Counter http2StreamWriteAndFlushErrorCount;
  public final Counter http2NetworkErrorCount;

  public final Meter http2ClientSendRate;
  public final Meter http2ClientReceiveRate;

  public Http2ClientMetrics(MetricRegistry registry) {
    http2ConnectionAcquireTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2ConnectionAcquireTime"));
    http2StreamAcquireTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamAcquireTime"));
    http2FirstStreamAcquireTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2FirstStreamAcquireTime"));
    http2RegularStreamAcquireTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2RegularStreamAcquireTime"));
    http2StreamWriteAndFlushTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamWriteAndFlushTime"));
    http2StreamRoundTripTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamRoundTripTime"));
    http2StreamFirstToLastFrameTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamFirstToLastFrameTime"));
    http2StreamFirstToAllFrameReadyTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamFirstToAllFrameReadyTime"));
    http2ClientSendTime = registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2ClientSendTime"));
    http2ClientSendAndPollTime =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2ClientSendAndPollTime"));
    http2ResponseFrameCount =
        registry.histogram(MetricRegistry.name(Http2NetworkClient.class, "Http2ResponseFrameCount"));

    http2NewPoolCount = registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2NewPoolCount"));
    http2NewConnectionCount =
        registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2NewConnectionCount"));
    http2NewConnectionFailureCount =
        registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2NewConnectionFailureCount"));
    http2NewStreamCount = registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2NewStreamCount"));
    http2StreamAcquireFailureCount =
        registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamAcquireFailureCount"));
    http2StreamSlipAcquireCount =
        registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamSlipAcquire"));
    http2StreamWriteAndFlushErrorCount =
        registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2StreamWriteAndFlushErrorCount"));
    http2NetworkErrorCount = registry.counter(MetricRegistry.name(Http2NetworkClient.class, "Http2NetworkErrorCount"));
    http2ClientSendRate = registry.meter(MetricRegistry.name(Http2NetworkClient.class, "Http2ClientSendRate"));
    http2ClientReceiveRate = registry.meter(MetricRegistry.name(Http2NetworkClient.class, "Http2ClientReceiveRate"));
  }
}
