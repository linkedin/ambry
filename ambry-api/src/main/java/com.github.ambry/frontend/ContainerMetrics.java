/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.ResponseStatus;


/**
 * Metrics for an operation on a specific container.
 */
public class ContainerMetrics {
  private static final String SEPARATOR = "___";

  private final Histogram roundTripTimeInMs;

  // counts by status code type
  // 2xx
  private final Counter successCount;
  // 3xx
  private final Counter redirectionCount;
  // 4xx
  private final Counter clientErrorCount;
  // 5xx
  private final Counter serverErrorCount;

  // counts for individual status codes
  // 400
  private final Counter badRequestCount;
  // 401
  private final Counter unauthorizedCount;
  // 403
  private final Counter forbiddenCount;
  // 404
  private final Counter notFoundCount;
  // 410
  private final Counter goneCount;

  /**
   * Metric names will be in the following format:
   * {@code com.github.ambry.frontend.ContainerMetrics.{accountName}___{containerName}___{operationType}{metric}}
   * For example:
   * {@code com.github.ambry.frontend.ContainerMetrics.account-a___container-b___GetBlobSuccessCount}
   * @param accountName the account name to use for naming metrics.
   * @param containerName the container name to use for naming metrics.
   * @param operationType the operation type to use for naming metrics.
   * @param metricRegistry the {@link MetricRegistry}.
   */
  ContainerMetrics(String accountName, String containerName, String operationType, MetricRegistry metricRegistry) {
    String metricPrefix = accountName + SEPARATOR + containerName + SEPARATOR + operationType;
    roundTripTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "RoundTripTimeInMs"));
    // counts by status code type
    successCount = metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "SuccessCount"));
    redirectionCount =
        metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "RedirectionCount"));
    clientErrorCount =
        metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "ClientErrorCount"));
    serverErrorCount =
        metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "ServerErrorCount"));
    // counts for individual status codes
    badRequestCount =
        metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "BadRequestCount"));
    unauthorizedCount =
        metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "UnauthorizedCount"));
    forbiddenCount =
        metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "ForbiddenCount"));
    notFoundCount = metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "NotFoundCount"));
    goneCount = metricRegistry.counter(MetricRegistry.name(ContainerMetrics.class, metricPrefix + "GoneCount"));
  }

  /**
   * Emit metrics for an operation on this container.
   * @param roundTripTimeInMs the time it took to receive a request and send a response.
   * @param responseStatus the {@link ResponseStatus} sent in the response.
   */
  public void recordMetrics(long roundTripTimeInMs, ResponseStatus responseStatus) {
    this.roundTripTimeInMs.update(roundTripTimeInMs);

    if (responseStatus.isSuccess()) {
      successCount.inc();
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
