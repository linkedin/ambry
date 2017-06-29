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
  public final Histogram updateAccountTimeInMs;
  public final Histogram fetchRemoteAccountTimeInMs;

  // Counter
  public final Counter unRecognizedMessageErrorCount;
  public final Counter notifyAccountDataChangeErrorCount;
  public final Counter updateAccountErrorCount;
  public final Counter fetchRemoteAccountErrorCount;
  public final Counter remoteDataCorruptionErrorCount;

  public AccountServiceMetrics(MetricRegistry metricRegistry) {
    // Histogram
    startupTimeInMs = metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "startupTimeInMs"));
    updateAccountTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "updateAccountTimeInMs"));
    fetchRemoteAccountTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "fetchRemoteAccountTimeInMs"));

    // Counter
    unRecognizedMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "unRecognizedMessageErrorCount"));
    notifyAccountDataChangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "notifyAccountDataChangeErrorCount"));
    updateAccountErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "updateAccountErrorCount"));
    fetchRemoteAccountErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "fetchRemoteAccountErrorCount"));
    remoteDataCorruptionErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "remoteDataCorruptionErrorCount"));
  }
}
