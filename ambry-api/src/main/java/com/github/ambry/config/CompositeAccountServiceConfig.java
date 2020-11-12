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
 * Configs for CompositeAccountService
 */
public class CompositeAccountServiceConfig {
  public static final String COMPOSITE_ACCOUNT_SERVICE_PREFIX = "composite.account.service.";
  public static final String CONSISTENCY_CHECKER_INTERVAL_MINUTES =
      COMPOSITE_ACCOUNT_SERVICE_PREFIX + "consistency.checker.interval.minutes";
  public static final String CONSISTENCY_CHECKER_SHUTDOWN_TIMEOUT_MINUTES =
      COMPOSITE_ACCOUNT_SERVICE_PREFIX + "consistency.checker.shutdown.timeout.minutes";
  public static final String SAMPLING_PERCENTAGE_FOR_GET_CONSISTENCY_CHECK =
      COMPOSITE_ACCOUNT_SERVICE_PREFIX + "sampling.percentage.for.get.consistency.check";
  public static final String PRIMARY_ACCOUNT_SERVICE_FACTORY =
      COMPOSITE_ACCOUNT_SERVICE_PREFIX + "primary.account.service.factory";
  public static final String SECONDARY_ACCOUNT_SERVICE_FACTORY =
      COMPOSITE_ACCOUNT_SERVICE_PREFIX + "secondary.account.service.factory";

  /**
   * The time interval in minutes for checking consistency in account data between primary and secondary sources.
   * Setting to 0 will disable it. Default value is 5 minutes.
   */
  @Config(CONSISTENCY_CHECKER_INTERVAL_MINUTES)
  @Default("5")
  public final int consistencyCheckerIntervalMinutes;

  /**
   * The timeout in minutes to shut down the consistency checker of {@code CompositeAccountService}. Default value is 1 minutes.
   */
  @Config(CONSISTENCY_CHECKER_SHUTDOWN_TIMEOUT_MINUTES)
  @Default("1")
  public final int consistencyCheckerShutdownTimeoutMinutes;

  /**
   * Percentage of GET requests we want to compare results between primary and secondary sources. Default value is 50%.
   * If we want to avoid latency due to comparison, this value can be set to 0.
   */
  @Config(SAMPLING_PERCENTAGE_FOR_GET_CONSISTENCY_CHECK)
  @Default("50")
  public final int samplingPercentageForGetConsistencyCheck;

  /**
   * The AccountServiceFactory that is used in {@code CompositeAccountService} as primary source of account-related information.
   */
  @Config(PRIMARY_ACCOUNT_SERVICE_FACTORY)
  @Default("com.github.ambry.account.HelixAccountServiceFactory")
  public final String primaryAccountServiceFactory;

  /**
   * The AccountServiceFactory that is used in {@code CompositeAccountService} as secondary source of account-related information.
   */
  @Config(SECONDARY_ACCOUNT_SERVICE_FACTORY)
  @Default("com.github.ambry.account.MySqlAccountServiceFactory")
  public final String secondaryAccountServiceFactory;

  public CompositeAccountServiceConfig(VerifiableProperties verifiableProperties) {
    consistencyCheckerIntervalMinutes =
        verifiableProperties.getIntInRange(CONSISTENCY_CHECKER_INTERVAL_MINUTES, 5, 0, Integer.MAX_VALUE);
    consistencyCheckerShutdownTimeoutMinutes =
        verifiableProperties.getIntInRange(CONSISTENCY_CHECKER_SHUTDOWN_TIMEOUT_MINUTES, 1, 0, Integer.MAX_VALUE);
    samplingPercentageForGetConsistencyCheck =
        verifiableProperties.getIntInRange(SAMPLING_PERCENTAGE_FOR_GET_CONSISTENCY_CHECK, 50, 0, 100);
    primaryAccountServiceFactory = verifiableProperties.getString(PRIMARY_ACCOUNT_SERVICE_FACTORY);
    secondaryAccountServiceFactory = verifiableProperties.getString(SECONDARY_ACCOUNT_SERVICE_FACTORY);
  }
}
