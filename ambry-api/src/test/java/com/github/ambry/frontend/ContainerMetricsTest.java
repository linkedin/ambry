/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ContainerMetrics}.
 */
public class ContainerMetricsTest {
  private static final String ACCOUNT_NAME = "account";
  private static final String CONTAINER_NAME = "container";
  private static final String OPERATION_TYPE = "PostBlob";

  /**
   * Tests that a client abort is counted against both the container and its account, mirroring how
   * {@link ContainerMetrics#recordMetrics} already reports to both. Without the account level count, an account
   * dashboard could see aborts in its bad request count with no way to subtract them back out.
   */
  @Test
  public void clientAbortIsRecordedForContainerAndAccountTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    AccountMetrics accountMetrics = new AccountMetrics(ACCOUNT_NAME, OPERATION_TYPE, metricRegistry, false);
    ContainerMetrics containerMetrics =
        new ContainerMetrics(ACCOUNT_NAME, CONTAINER_NAME, OPERATION_TYPE, metricRegistry, false, accountMetrics);

    containerMetrics.recordClientAbort();

    assertEquals("Container should have recorded the abort", 1, containerCounter(metricRegistry, "ClientAbortCount"));
    assertEquals("Account should have recorded the abort", 1, accountCounter(metricRegistry, "ClientAbortCount"));
  }

  /**
   * Tests that recording an abort touches nothing but the abort counters. The status counters are fed separately by
   * {@link ContainerMetrics#recordMetrics}, which still sees the 400 that the abort is reported as.
   */
  @Test
  public void clientAbortDoesNotDisturbStatusCountersTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    AccountMetrics accountMetrics = new AccountMetrics(ACCOUNT_NAME, OPERATION_TYPE, metricRegistry, false);
    ContainerMetrics containerMetrics =
        new ContainerMetrics(ACCOUNT_NAME, CONTAINER_NAME, OPERATION_TYPE, metricRegistry, false, accountMetrics);

    containerMetrics.recordMetrics(10, ResponseStatus.BadRequest, 0);
    containerMetrics.recordClientAbort();

    assertEquals("Abort should be counted once", 1, containerCounter(metricRegistry, "ClientAbortCount"));
    assertEquals("Bad request count should be unchanged by the abort counter", 1,
        containerCounter(metricRegistry, "BadRequestCount"));
    assertEquals("Client error count should be unchanged by the abort counter", 1,
        containerCounter(metricRegistry, "ClientErrorCount"));
    assertEquals("A client abort is not a server error", 0, containerCounter(metricRegistry, "ServerErrorCount"));
    assertEquals("A client abort is not a success", 0, containerCounter(metricRegistry, "SuccessCount"));
  }

  /**
   * @param metricRegistry the {@link MetricRegistry} to read from.
   * @param suffix the metric name suffix to read.
   * @return the value of the named container level counter.
   */
  private static long containerCounter(MetricRegistry metricRegistry, String suffix) {
    return metricRegistry.getCounters()
        .get(ContainerMetrics.class.getCanonicalName() + "." + ACCOUNT_NAME + EntityOperationMetrics.SEPARATOR
            + CONTAINER_NAME + EntityOperationMetrics.SEPARATOR + OPERATION_TYPE + suffix)
        .getCount();
  }

  /**
   * @param metricRegistry the {@link MetricRegistry} to read from.
   * @param suffix the metric name suffix to read.
   * @return the value of the named account level counter.
   */
  private static long accountCounter(MetricRegistry metricRegistry, String suffix) {
    return metricRegistry.getCounters()
        .get(AccountMetrics.class.getCanonicalName() + "." + ACCOUNT_NAME + EntityOperationMetrics.SEPARATOR
            + OPERATION_TYPE + suffix)
        .getCount();
  }
}
