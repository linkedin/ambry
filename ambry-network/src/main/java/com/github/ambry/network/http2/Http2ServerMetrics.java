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
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for HTTP/2 Client
 */
public class Http2ServerMetrics {

  public final Histogram requestEnqueueTime;
  public final Histogram requestQueuingTime;
  public final Histogram requestTotalProcessingTime;

  public final Counter requestResponseChannelErrorCount;
  public final Counter http2ParentExceptionCount;
  public final Counter http2StreamExceptionCount;

  public Http2ServerMetrics(MetricRegistry registry) {
    requestEnqueueTime = registry.histogram(MetricRegistry.name(Http2ServerMetrics.class, "RequestEnqueueTime"));
    requestQueuingTime = registry.histogram(MetricRegistry.name(Http2ServerMetrics.class, "RequestQueuingTime"));
    requestTotalProcessingTime =
        registry.histogram(MetricRegistry.name(Http2ServerMetrics.class, "RequestTotalProcessingTime"));

    requestResponseChannelErrorCount =
        registry.counter(MetricRegistry.name(Http2ServerMetrics.class, "RequestResponseChannelErrorCount"));
    http2ParentExceptionCount =
        registry.counter(MetricRegistry.name(Http2ServerMetrics.class, "Http2ParentExceptionCount"));
    http2StreamExceptionCount =
        registry.counter(MetricRegistry.name(Http2ServerMetrics.class, "Http2StreamExceptionCount"));
  }
}
