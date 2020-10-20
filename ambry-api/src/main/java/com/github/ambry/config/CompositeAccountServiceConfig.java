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

  public CompositeAccountServiceConfig(VerifiableProperties verifiableProperties) {
    consistencyCheckerIntervalMinutes =
        verifiableProperties.getIntInRange(CONSISTENCY_CHECKER_INTERVAL_MINUTES, 5, 0, Integer.MAX_VALUE);
    consistencyCheckerShutdownTimeoutMinutes =
        verifiableProperties.getIntInRange(CONSISTENCY_CHECKER_SHUTDOWN_TIMEOUT_MINUTES, 1, 0, Integer.MAX_VALUE);
  }
}
