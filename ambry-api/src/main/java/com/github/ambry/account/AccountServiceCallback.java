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
package com.github.ambry.account;

import com.github.ambry.commons.Callback;
import com.github.ambry.server.StatsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccountServiceCallback implements Callback<StatsSnapshot> {
  private final AccountService accountService;
  private static final Logger logger = LoggerFactory.getLogger(AccountServiceCallback.class);

  /**
   * Construct a AccountServiceCallback object
   * @param accountService the {@link AccountService} associated with this callback.
   */
  public AccountServiceCallback(AccountService accountService) {
    this.accountService = accountService;
  }

  /**
   * When the aggregation report has been generated successfully, this method will be invoked and associated {@link AccountService}
   * will select container which valid data size equals to zero from DELETE_IN_PROGRESS {@link Container} set and mark it INACTIVE in zookeeper.
   * @param results the StatsSnapshot whose values represents aggregated stats across all partitions.
   * @param exception Exception occurred when updating Helix property store, not in aggregation phase, the result is still solid.
   */
  @Override
  public void onCompletion(StatsSnapshot results, Exception exception) {
    if (exception != null) {
      logger.info(
          "Aggregator task encountered an exception but result is not null. Processing the result as aggregation is complete.");
    }
    accountService.selectInactiveContainersAndMarkInZK(results);
  }
}
