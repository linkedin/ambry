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

import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for an operation on a specific account.
 */
public class AccountMetrics extends EntityOperationMetrics {

  /**
   * Metric names will be in the following format:
   * {@code com.github.ambry.frontend.AccountMetrics.{accountName}___{operationType}{metric}} For example:
   * {@code com.github.ambry.frontend.AccountMetrics.account-a___GetBlobSuccessCount}
   *
   * @param accountName    the account name to use for naming metrics.
   * @param operationType  the operation type to use for naming metrics.
   * @param metricRegistry the {@link MetricRegistry}.
   * @param isGetRequest   the request operationType is get.
   */
  AccountMetrics(String accountName, String operationType, MetricRegistry metricRegistry, boolean isGetRequest) {
    super(accountName, AccountMetrics.class, operationType, metricRegistry, isGetRequest);
  }
}
