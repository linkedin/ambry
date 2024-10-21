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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.ResponseStatus;


/**
 * Metrics for an operation on a specific container.
 */
public class ContainerMetrics extends EntityOperationMetrics {
  private final AccountMetrics accountMetrics;

  /**
   * Metric names will be in the following format:
   * {@code com.github.ambry.frontend.ContainerMetrics.{accountName}___{containerName}___{operationType}{metric}} For
   * example: {@code com.github.ambry.frontend.ContainerMetrics.account-a___container-b___GetBlobSuccessCount}
   *
   * @param accountName    the account name to use for naming metrics.
   * @param containerName  the container name to use for naming metrics.
   * @param operationType  the operation type to use for naming metrics.
   * @param metricRegistry the {@link MetricRegistry}.
   * @param isGetRequest   the request operationType is get.
   * @param accountMetrics the {@link AccountMetrics} for this account. If it's null, then there will no account
   *                       metrics
   */
  ContainerMetrics(String accountName, String containerName, String operationType, MetricRegistry metricRegistry,
      boolean isGetRequest, AccountMetrics accountMetrics) {
    super(accountName + EntityOperationMetrics.SEPARATOR + containerName, ContainerMetrics.class, operationType,
        metricRegistry, isGetRequest);
    this.accountMetrics = accountMetrics;
  }

  public void recordMetrics(long roundTripTimeInMs, ResponseStatus responseStatus) {
    super.recordMetrics(roundTripTimeInMs, responseStatus);
    if (accountMetrics != null) {
      accountMetrics.recordMetrics(roundTripTimeInMs, responseStatus);
    }
  }
}
