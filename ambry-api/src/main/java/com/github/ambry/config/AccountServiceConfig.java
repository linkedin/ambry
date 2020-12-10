/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.config;

/**
 * Common config for {@link AccountServiceConfig}
 */
public class AccountServiceConfig {
  public static final String ACCOUNT_SERVICE_PREFIX = "account.service.";
  public static final String CONTAINER_ID_START_NUMBER = ACCOUNT_SERVICE_PREFIX + "container.id.start.number";
  public static final String MAX_RETRY_COUNT_ON_UPDATE_FAILURE =
      ACCOUNT_SERVICE_PREFIX + "max.retry.count.on.update.failure";
  public static final String RETRY_DELAY_MS = ACCOUNT_SERVICE_PREFIX + "retry.delay.ms";
  public static final String IGNORE_VERSION_MISMATCH = ACCOUNT_SERVICE_PREFIX + "ignore.version.mismatch";
  public static final String CONTAINER_DEPRECATION_RETENTION_DAYS = ACCOUNT_SERVICE_PREFIX + "container.deprecation.retention.days";

  /** The numeric container Id to be used for the first container created within an account. */
  @Config(CONTAINER_ID_START_NUMBER)
  @Default("1")
  public final short containerIdStartNumber;

  /**
   * The number of retry times when the update accounts fails by marking delete_in_progress container status to inactive;
   */
  @Config(MAX_RETRY_COUNT_ON_UPDATE_FAILURE)
  @Default("10")
  public final int maxRetryCountOnUpdateFailure;

  /**
   * The sleep time between each retry action when the update accounts fails by marking delete_in_progress container status to inactive;
   */
  @Config(RETRY_DELAY_MS)
  @Default("1000")
  public final long retryDelayMs;

  /**
   * If true, AccountService would ignore version mismatch while updating Accounts and Containers.
   */
  @Config(IGNORE_VERSION_MISMATCH)
  @Default("false")
  public final boolean ignoreVersionMismatch;

  /**
   * The period of time during which the deprecated container can be restored. After retention time, compaction starts
   * to compact blobs in this container.
   */
  @Config(CONTAINER_DEPRECATION_RETENTION_DAYS)
  public final int containerDeprecationRetentionDays;

  public AccountServiceConfig(VerifiableProperties verifiableProperties) {
    containerIdStartNumber =
        verifiableProperties.getShortInRange(CONTAINER_ID_START_NUMBER, (short) 1, (short) 0, Short.MAX_VALUE);
    maxRetryCountOnUpdateFailure = verifiableProperties.getIntInRange(MAX_RETRY_COUNT_ON_UPDATE_FAILURE, 10, 1, 100);
    retryDelayMs = verifiableProperties.getLongInRange(RETRY_DELAY_MS, 1000, 1, Long.MAX_VALUE);
    ignoreVersionMismatch = verifiableProperties.getBoolean(IGNORE_VERSION_MISMATCH, false);
    containerDeprecationRetentionDays = verifiableProperties.getIntInRange(CONTAINER_DEPRECATION_RETENTION_DAYS, 13, 0, Integer.MAX_VALUE);
  }
}
