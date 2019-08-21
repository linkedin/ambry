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
  public final Histogram accountUpdateConsumerTimeInMs;
  public final Histogram accountUpdateToAmbryTimeInMs;
  public final Histogram accountFetchFromAmbryTimeInMs;
  public final Histogram backupWriteTimeInMs;
  public final Histogram backupReadTimeInMs;

  // Counter
  public final Counter unrecognizedMessageErrorCount;
  public final Counter notifyAccountDataChangeErrorCount;
  public final Counter updateAccountErrorCount;
  public final Counter fetchRemoteAccountErrorCount;
  public final Counter remoteDataCorruptionErrorCount;
  public final Counter backupErrorCount;
  public final Counter nullNotifierCount;
  public final Counter accountUpdatesCapturedByScheduledUpdaterCount;
  public final Counter accountUpdatesToAmbryServerErrorCount;
  public final Counter accountDeletesToAmbryServerErrorCount;
  public final Counter accountFetchFromAmbryServerErrorCount;

  public AccountServiceMetrics(MetricRegistry metricRegistry) {
    // Histogram
    startupTimeInMs = metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "StartupTimeInMs"));
    updateAccountTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "UpdateAccountTimeInMs"));
    fetchRemoteAccountTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "FetchRemoteAccountTimeInMs"));
    accountUpdateConsumerTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "AccountUpdateConsumerTimeInMs"));
    accountUpdateToAmbryTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "AccountUpdateToAmbryTimeInMs"));
    accountFetchFromAmbryTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "AccountFetchFromAmbryTimeInMs"));
    backupWriteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "BackupWriteTimeInMs"));
    backupReadTimeInMs = metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, "BackupReadTimeInMs"));

    // Counter
    unrecognizedMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "UnrecognizedMessageErrorCount"));
    notifyAccountDataChangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "NotifyAccountDataChangeErrorCount"));
    updateAccountErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "UpdateAccountErrorCount"));
    fetchRemoteAccountErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "FetchRemoteAccountErrorCount"));
    remoteDataCorruptionErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "RemoteDataCorruptionErrorCount"));
    backupErrorCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "BackupErrorCount"));
    nullNotifierCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "NullNotifierCount"));
    accountUpdatesCapturedByScheduledUpdaterCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, "AccountUpdatesCapturedByScheduledUpdaterCount"));
    accountUpdatesToAmbryServerErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "AccountUpdatesToAmbryServerErrorCount"));
    accountDeletesToAmbryServerErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "AccountDeletesToAmbryServerErrorCount"));
    accountFetchFromAmbryServerErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, "AccountFetchFromAmbryServerErrorCount"));
  }
}
