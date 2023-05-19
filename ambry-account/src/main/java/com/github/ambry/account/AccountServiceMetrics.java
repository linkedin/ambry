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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * {@link AccountService} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link AccountService} to the provided {@link MetricRegistry}.
 */
public class AccountServiceMetrics {

  public static final String STARTUP_TIME_MSEC = "StartupTimeInMs";
  public static final String UPDATE_ACCOUNT_TIME_MSEC = "UpdateAccountTimeInMs";
  public static final String UPDATE_ACCOUNT_TIME_MSEC_NEW = "UpdateAccountTimeInMsNew";
  public static final String FETCH_REMOTE_ACCOUNT_TIME_MSEC = "FetchRemoteAccountTimeInMs";
  public static final String FETCH_REMOTE_ACCOUNT_TIME_MSEC_NEW = "FetchRemoteAccountTimeInMsNew";
  public static final String ACCOUNT_UPDATE_CONSUMER_TIME_MSEC = "AccountUpdateConsumerTimeInMs";
  public static final String ACCOUNT_UPDATE_TO_AMBRY_TIME_MSEC = "AccountUpdateToAmbryTimeInMs";
  public static final String ACCOUNT_FETCH_FROM_AMBRY_TIME_MSEC = "AccountFetchFromAmbryTimeInMs";
  public static final String BACKUP_WRITE_TIME_MSEC = "BackupWriteTimeInMs";
  public static final String BACKUP_READ_TIME_MSEC = "BackupReadTimeInMs";
  public static final String UNRECOGNIZED_MESSAGE_ERROR_COUNT = "UnrecognizedMessageErrorCount";
  public static final String NOTIFY_ACCOUNT_DATA_CHANGE_ERROR_COUNT = "NotifyAccountDataChangeErrorCount";
  public static final String UPDATE_ACCOUNT_ERROR_COUNT = "UpdateAccountErrorCount";
  public static final String UPDATE_CONTAINERS_ERROR_COUNT = "UpdateContainersErrorCount";
  public static final String UPDATE_CONTAINERS_ERROR_COUNT_NEW = "UpdateContainersErrorCountNew";
  public static final String UPDATE_ACCOUNTS_ERROR_COUNT = "UpdateAccountsErrorCount";
  public static final String UPDATE_ACCOUNTS_ERROR_COUNT_NEW = "UpdateAccountsErrorCountNew";
  public static final String FETCH_AND_UPDATE_CACHE_ERROR_COUNT = "FetchAndUpdateCacheErrorCount";
  public static final String FETCH_AND_UPDATE_CACHE_ERROR_COUNT_NEW = "FetchAndUpdateCacheErrorCountNew";
  public static final String UPDATE_ACCOUNT_ERROR_COUNT_NEW = "UpdateAccountErrorCountNew";
  public static final String CONFLICT_RETRY_COUNT = "ConflictRetryCount";
  public static final String ACCOUNT_UPDATES_TO_STORE_ERROR_COUNT = "AccountUpdatesToStoreErrorCount";
  public static final String FETCH_REMOTE_ACCOUNT_ERROR_COUNT = "FetchRemoteAccountErrorCount";
  public static final String FETCH_REMOTE_ACCOUNT_ERROR_COUNT_NEW = "FetchRemoteAccountErrorCountNew";
  public static final String REMOTE_DATA_CORRUPTION_ERROR_COUNT = "RemoteDataCorruptionErrorCount";
  public static final String BACKUP_ERROR_COUNT = "BackupErrorCount";
  public static final String NULL_NOTIFIER_COUNT = "NullNotifierCount";
  public static final String ACCOUNT_UPDATES_CAPTURED_BY_SCHEDULED_UPDATER_COUNT =
      "AccountUpdatesCapturedByScheduledUpdaterCount";
  public static final String ACCOUNT_UPDATES_TO_AMBRY_SERVER_ERROR_COUNT = "AccountUpdatesToAmbryServerErrorCount";
  public static final String ACCOUNT_DELETES_TO_AMBRY_SERVER_ERROR_COUNT = "AccountDeletesToAmbryServerErrorCount";
  public static final String ACCOUNT_FETCH_FROM_AMBRY_SERVER_ERROR_COUNT = "AccountFetchFromAmbryServerErrorCount";
  public static final String GET_ACCOUNT_INCONSISTENCY_COUNT = "GetAccountInconsistencyCount";
  public static final String ON_DEMAND_CONTAINER_FETCH_COUNT = "OnDemandContainerFetchCount";
  public static final String ON_DEMAND_CONTAINER_FETCH_COUNT_NEW = "OnDemandContainerFetchCountNew";
  public static final String ACCOUNT_DATA_INCONSISTENCY_COUNT = "AccountDataInconsistencyCount";
  public static final String TIME_IN_SECONDS_SINCE_LAST_SYNC = "TimeInSecondsSinceLastSync";
  public static final String TIME_IN_SECONDS_SINCE_LAST_SYNC_NEW = "TimeInSecondsSinceLastSyncNew";
  public static final String UPDATE_ACCOUNT_FROM_OLD_CACHE_TO_NEW_DB_COUNT = "UpdateAccountFromOldCacheToNewDbCount";

  public static final String CONTAINER_COUNT = "ContainerCount";
  public static final String CONTAINER_COUNT_NEW = "ContainerCountNew";

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
  public final Counter conflictRetryCount;
  public final Counter fetchRemoteAccountErrorCount;
  public final Counter remoteDataCorruptionErrorCount;
  public final Counter backupErrorCount;
  public final Counter nullNotifierCount;
  public final Counter accountUpdatesCapturedByScheduledUpdaterCount;
  public final Counter accountUpdatesToAmbryServerErrorCount;
  public final Counter accountDeletesToAmbryServerErrorCount;
  public final Counter accountFetchFromAmbryServerErrorCount;
  public final Counter accountUpdatesToStoreErrorCount;
  public final Counter getAccountInconsistencyCount;
  public final Counter onDemandContainerFetchCount;
  public final Counter updateAccountFromOldCacheToNewDbCount;
  public final Counter updateContainersErrorCount;
  public final Counter fetchAndUpdateCacheErrorCount;
  public final Counter updateAccountsErrorCount;

  // Gauge
  Gauge<Integer> accountDataInconsistencyCount;
  Gauge<Integer> timeInSecondsSinceLastSyncGauge;
  Gauge<Integer> containerCountGauge;

  private final MetricRegistry metricRegistry;
  private final boolean enableNewMetricsForMigration;

  public AccountServiceMetrics(MetricRegistry metricRegistry, boolean enableNewMetricsForMigration) {
    this.metricRegistry = metricRegistry;
    this.enableNewMetricsForMigration = enableNewMetricsForMigration;
    // Histogram
    startupTimeInMs = metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, STARTUP_TIME_MSEC));
    if (enableNewMetricsForMigration) {
      updateAccountTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, UPDATE_ACCOUNT_TIME_MSEC_NEW));
      fetchRemoteAccountTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, FETCH_REMOTE_ACCOUNT_TIME_MSEC_NEW));
    } else {
      updateAccountTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, UPDATE_ACCOUNT_TIME_MSEC));
      fetchRemoteAccountTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, FETCH_REMOTE_ACCOUNT_TIME_MSEC));
    }
    accountUpdateConsumerTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, ACCOUNT_UPDATE_CONSUMER_TIME_MSEC));
    accountUpdateToAmbryTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, ACCOUNT_UPDATE_TO_AMBRY_TIME_MSEC));
    accountFetchFromAmbryTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, ACCOUNT_FETCH_FROM_AMBRY_TIME_MSEC));
    backupWriteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, BACKUP_WRITE_TIME_MSEC));
    backupReadTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HelixAccountService.class, BACKUP_READ_TIME_MSEC));

    // Counter
    unrecognizedMessageErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, UNRECOGNIZED_MESSAGE_ERROR_COUNT));
    notifyAccountDataChangeErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, NOTIFY_ACCOUNT_DATA_CHANGE_ERROR_COUNT));
    if (enableNewMetricsForMigration) {
      updateAccountErrorCount =
          metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, UPDATE_ACCOUNT_ERROR_COUNT_NEW));
      fetchRemoteAccountErrorCount =
          metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, FETCH_REMOTE_ACCOUNT_ERROR_COUNT_NEW));
      onDemandContainerFetchCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, ON_DEMAND_CONTAINER_FETCH_COUNT_NEW));
      updateContainersErrorCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, UPDATE_CONTAINERS_ERROR_COUNT_NEW));
      fetchAndUpdateCacheErrorCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, FETCH_AND_UPDATE_CACHE_ERROR_COUNT_NEW));
      updateAccountsErrorCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, UPDATE_ACCOUNTS_ERROR_COUNT_NEW));
    } else {
      updateAccountErrorCount =
          metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, UPDATE_ACCOUNT_ERROR_COUNT));
      fetchRemoteAccountErrorCount =
          metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, FETCH_REMOTE_ACCOUNT_ERROR_COUNT));
      onDemandContainerFetchCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, ON_DEMAND_CONTAINER_FETCH_COUNT));
      updateContainersErrorCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, UPDATE_CONTAINERS_ERROR_COUNT));
      fetchAndUpdateCacheErrorCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, FETCH_AND_UPDATE_CACHE_ERROR_COUNT));
      updateAccountsErrorCount =
          metricRegistry.counter(MetricRegistry.name(MySqlAccountService.class, UPDATE_ACCOUNTS_ERROR_COUNT));
    }
    conflictRetryCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, CONFLICT_RETRY_COUNT));
    accountUpdatesToStoreErrorCount =
        metricRegistry.counter(MetricRegistry.name(AbstractAccountService.class, ACCOUNT_UPDATES_TO_STORE_ERROR_COUNT));
    remoteDataCorruptionErrorCount =
        metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, REMOTE_DATA_CORRUPTION_ERROR_COUNT));
    backupErrorCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, BACKUP_ERROR_COUNT));
    nullNotifierCount = metricRegistry.counter(MetricRegistry.name(HelixAccountService.class, NULL_NOTIFIER_COUNT));
    accountUpdatesCapturedByScheduledUpdaterCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, ACCOUNT_UPDATES_CAPTURED_BY_SCHEDULED_UPDATER_COUNT));
    accountUpdatesToAmbryServerErrorCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, ACCOUNT_UPDATES_TO_AMBRY_SERVER_ERROR_COUNT));
    accountDeletesToAmbryServerErrorCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, ACCOUNT_DELETES_TO_AMBRY_SERVER_ERROR_COUNT));
    accountFetchFromAmbryServerErrorCount = metricRegistry.counter(
        MetricRegistry.name(HelixAccountService.class, ACCOUNT_FETCH_FROM_AMBRY_SERVER_ERROR_COUNT));
    getAccountInconsistencyCount =
        metricRegistry.counter(MetricRegistry.name(CompositeAccountService.class, GET_ACCOUNT_INCONSISTENCY_COUNT));
    updateAccountFromOldCacheToNewDbCount = metricRegistry.counter(
        MetricRegistry.name(MySqlAccountService.class, UPDATE_ACCOUNT_FROM_OLD_CACHE_TO_NEW_DB_COUNT));
  }

  /**
   * Tracks the number of accounts different between primary and secondary {@link AccountService}s
   * @param compositeAccountService instance of {@link CompositeAccountService}
   */
  void trackAccountDataInconsistency(CompositeAccountService compositeAccountService) {
    accountDataInconsistencyCount = compositeAccountService::getAccountsMismatchCount;
    metricRegistry.register(MetricRegistry.name(CompositeAccountService.class, ACCOUNT_DATA_INCONSISTENCY_COUNT),
        accountDataInconsistencyCount);
  }

  /**
   * Tracks the number of seconds elapsed since the last database sync.
   * @param gauge the function returning the elapsed time.
   */
  void trackTimeSinceLastSync(Gauge<Integer> gauge) {
    timeInSecondsSinceLastSyncGauge = gauge;
    if (enableNewMetricsForMigration) {
      metricRegistry.register(MetricRegistry.name(MySqlAccountService.class, TIME_IN_SECONDS_SINCE_LAST_SYNC_NEW),
          timeInSecondsSinceLastSyncGauge);
    } else {
      metricRegistry.register(MetricRegistry.name(MySqlAccountService.class, TIME_IN_SECONDS_SINCE_LAST_SYNC),
          timeInSecondsSinceLastSyncGauge);
    }
  }

  /**
   * Tracks the total number of containers across all accounts.
   * @param gauge the function returning the container count.
   */
  void trackContainerCount(Gauge<Integer> gauge) {
    containerCountGauge = gauge;
    if (enableNewMetricsForMigration) {
      metricRegistry.register(MetricRegistry.name(MySqlAccountService.class, CONTAINER_COUNT_NEW), containerCountGauge);
    } else {
      metricRegistry.register(MetricRegistry.name(MySqlAccountService.class, CONTAINER_COUNT), containerCountGauge);
    }
  }
}
