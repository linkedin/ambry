/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.github.ambry.frontend;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestUtils;


/**
 * Metrics for an operation on a specific entity.
 */
public class EntityOperationMetrics {
  public static final String SEPARATOR = "___";

  protected final Histogram roundTripTimeInMs;

  // counts by status code type
  // 2xx
  protected final Counter successCount;
  // 3xx
  protected final Counter redirectionCount;
  // 4xx
  protected final Counter clientErrorCount;
  // 5xx
  protected final Counter serverErrorCount;

  // counts for individual status codes
  // 400
  protected final Counter badRequestCount;
  // 401
  protected final Counter unauthorizedCount;
  // 403
  protected final Counter forbiddenCount;
  // 404
  protected final Counter notFoundCount;
  // 410
  protected final Counter goneCount;

  protected final Counter totalCount;

  protected final Histogram throughput;

  /**
   * Constructor to create operation metrics for given entity. The entity can be an account or a container.
   *
   * @param entityName     the name of the given entity.
   * @param ownerClass     the owner class of the given entity.
   * @param operationType  the operation type to use for naming metrics.
   * @param metricRegistry the {@link MetricRegistry}.
   * @param isGetRequest   the request operationType is get.
   */
  EntityOperationMetrics(String entityName, Class<?> ownerClass, String operationType, MetricRegistry metricRegistry,
      boolean isGetRequest) {
    String metricPrefix = entityName + SEPARATOR + operationType;
    if (ownerClass == null) {
      ownerClass = EntityOperationMetrics.class;
    }
    roundTripTimeInMs = metricRegistry.histogram(MetricRegistry.name(ownerClass, metricPrefix + "RoundTripTimeInMs"));
    // counts by status code type
    successCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "SuccessCount"));
    redirectionCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "RedirectionCount"));
    clientErrorCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "ClientErrorCount"));
    serverErrorCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "ServerErrorCount"));
    // counts for individual status codes
    badRequestCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "BadRequestCount"));
    unauthorizedCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "UnauthorizedCount"));
    forbiddenCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "ForbiddenCount"));
    notFoundCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "NotFoundCount"));
    goneCount = metricRegistry.counter(MetricRegistry.name(ownerClass, metricPrefix + "GoneCount"));
    String qpsMetricPrefix = entityName + SEPARATOR + (isGetRequest ? "GetRequest" : "PutRequest");
    totalCount = metricRegistry.counter(MetricRegistry.name(ownerClass, qpsMetricPrefix + "totalCount"));

    throughput = metricRegistry.histogram(MetricRegistry.name(ownerClass, qpsMetricPrefix + "Throughput"));
  }

  /**
   * Emit metrics for an operation on this entity.
   *
   * @param roundTripTimeInMs the time it took to receive a request and send a response.
   * @param responseStatus    the {@link ResponseStatus} sent in the response.
   */
  public void recordMetrics(long roundTripTimeInMs, ResponseStatus responseStatus, long bytesTransferred) {
    this.roundTripTimeInMs.update(roundTripTimeInMs);
    totalCount.inc();
    if (responseStatus.isSuccess()) {
      successCount.inc();
      throughput.update(RestUtils.calculateThroughput(bytesTransferred, roundTripTimeInMs));
    } else if (responseStatus.isRedirection()) {
      redirectionCount.inc();
    } else if (responseStatus.isClientError()) {
      clientErrorCount.inc();
      switch (responseStatus) {
        case BadRequest:
          badRequestCount.inc();
          break;
        case Unauthorized:
          unauthorizedCount.inc();
          break;
        case Forbidden:
          forbiddenCount.inc();
          break;
        case NotFound:
          notFoundCount.inc();
          break;
        case Gone:
          goneCount.inc();
          break;
      }
    } else if (responseStatus.isServerError()) {
      serverErrorCount.inc();
    }
  }
}
