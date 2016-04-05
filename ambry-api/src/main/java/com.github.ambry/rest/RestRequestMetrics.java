/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.rest;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * A metrics object that is provided as input to {@link RestRequestMetricsTracker#injectMetrics(RestRequestMetrics)}.
 * </p>
 * It is expected that each request type will have it's own instance of RestRequestMetrics and the same instance is
 * used to track all requests of that type.
 */
public class RestRequestMetrics {
  protected static final String NIO_REQUEST_PROCESSING_TIME_SUFFIX = "NioRequestProcessingTimeInMs";
  protected static final String NIO_RESPONSE_PROCESSING_TIME_SUFFIX = "NioResponseProcessingTimeInMs";
  protected static final String NIO_ROUND_TRIP_TIME_SUFFIX = "NioRoundTripTimeInMs";

  protected static final String SC_REQUEST_PROCESSING_TIME_SUFFIX = "ScRequestProcessingTimeInMs";
  protected static final String SC_REQUEST_PROCESSING_WAIT_TIME_SUFFIX = "ScRequestProcessingWaitTimeInMs";
  protected static final String SC_RESPONSE_PROCESSING_TIME_SUFFIX = "ScResponseProcessingTimeInMs";
  protected static final String SC_RESPONSE_PROCESSING_WAIT_TIME_SUFFIX = "ScResponseProcessingWaitTimeInMs";
  protected static final String SC_ROUND_TRIP_TIME_SUFFIX = "ScRoundTripTimeInMs";

  protected static final String TOTAL_CPU_TIME_SUFFIX = "TotalCpuTimeInMs";

  protected final Histogram nioRequestProcessingTimeInMs;
  protected final Histogram nioResponseProcessingTimeInMs;
  protected final Histogram nioRoundTripTimeInMs;

  protected final Histogram scRequestProcessingTimeInMs;
  protected final Histogram scRequestProcessingWaitTimeInMs;
  protected final Histogram scResponseProcessingTimeInMs;
  protected final Histogram scResponseProcessingWaitTimeInMs;
  protected final Histogram scRoundTripTimeInMs;

  protected final Histogram totalCpuTimeInMs;

  /**
   * Creates an instance of RestRequestMetrics for {@code requestType} and attaches all the metrics related to the
   * request to the given {@code ownerClass}. The metrics are also registered in the provided {@code metricRegistry}.
   * @param ownerClass the {@link Class} that is supposed to own the metrics created by this tracker.
   * @param requestType the type of request for which a tracker is being created.
   * @param metricRegistry the {@link MetricRegistry} to use to register the created metrics.
   */
  public RestRequestMetrics(Class ownerClass, String requestType, MetricRegistry metricRegistry) {
    if (ownerClass == null || requestType == null || metricRegistry == null) {
      throw new IllegalArgumentException(
          "Null arg(s) during instantiation. Owner class - [" + ownerClass + "]. Request type - [" + requestType
              + "]. Metric registry - [" + metricRegistry + "]");
    }

    nioRequestProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_REQUEST_PROCESSING_TIME_SUFFIX));
    nioResponseProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_RESPONSE_PROCESSING_TIME_SUFFIX));
    nioRoundTripTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_ROUND_TRIP_TIME_SUFFIX));

    scRequestProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_REQUEST_PROCESSING_TIME_SUFFIX));
    scRequestProcessingWaitTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_REQUEST_PROCESSING_WAIT_TIME_SUFFIX));
    scResponseProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_RESPONSE_PROCESSING_TIME_SUFFIX));
    scResponseProcessingWaitTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(ownerClass, requestType + SC_RESPONSE_PROCESSING_WAIT_TIME_SUFFIX));
    scRoundTripTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_ROUND_TRIP_TIME_SUFFIX));

    totalCpuTimeInMs = metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + TOTAL_CPU_TIME_SUFFIX));
  }
}
