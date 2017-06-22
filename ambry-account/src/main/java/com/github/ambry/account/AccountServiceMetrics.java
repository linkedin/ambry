/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * {@link HelixAccountService} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link HelixAccountService} to the provided {@link MetricRegistry}.
 */
public class AccountServiceMetrics {
  // Histogram
  public final Histogram startupTimeInMs;
  public final Histogram accountUpdateTimeInMs;

  // Counter
  public final Counter unRecognizedMessageErrorCount;
  public final Counter processFullAccountMessageErrorCount;
  public final Counter nullZNRecordCount;
  public final Counter nullAccountMapInZNRecordCount;
  public final Counter buildAccountInfoMapFromRemoteRecordErrorCount;
  public final Counter conflictWithRemoteRecordErrorCount;
  public final Counter putAccountInAccountMapErrorCount;
  public final Counter publishMessageErrorCount;
  public final Counter updateAccountErrorCount;
  public final Counter unExpectedNullZNRecordErrorCount;
  public final Counter unExpectedNullAccountMapInZNRecordErrorCount;
  public final Counter caseDConflictErrorCount;
  public final Counter caseEConflictErrorCount;
  public final Counter invalidRecordErrorCount;
  public final Counter nullIdKeyErrorCount;
  public final Counter unMatchedIdKeyAndAccountErrorCount;
  public final Counter conflictInAccountInfoMapErrorCount;

  public AccountServiceMetrics(MetricRegistry metricRegistry) {
    // Histogram
    startupTimeInMs = metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "startupTimeInMs"));
    accountUpdateTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "accountUpdateTimeInMs"));

    // Counter
    unRecognizedMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "unRecognizedMessageErrorCount"));
    processFullAccountMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "processFullAccountMessageErrorCount"));
    nullZNRecordCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "nullZNRecordCount"));
    nullAccountMapInZNRecordCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "nullAccountMapInZNRecordCount"));
    buildAccountInfoMapFromRemoteRecordErrorCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, "buildAccountInfoMapFromRemoteRecordErrorCount"));
    conflictWithRemoteRecordErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "conflictWithRemoteRecordErrorCount"));
    putAccountInAccountMapErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "putAccountInAccountMapErrorCount"));
    publishMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "publishMessageErrorCount"));
    updateAccountErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "updateAccountErrorCount"));
    unExpectedNullZNRecordErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "unExpectedNullZNRecordErrorCount"));
    unExpectedNullAccountMapInZNRecordErrorCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, "unExpectedNullAccountMapInZNRecordErrorCount"));
    caseDConflictErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "caseDConflictErrorCount"));
    caseEConflictErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "caseEConflictErrorCount"));
    invalidRecordErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "invalidRecordErrorCount"));
    nullIdKeyErrorCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "nullIdKeyErrorCount"));
    unMatchedIdKeyAndAccountErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "unMatchedIdKeyAndAccountErrorCount"));
    conflictInAccountInfoMapErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "conflictInAccountInfoMapErrorCount"));
  }
}
