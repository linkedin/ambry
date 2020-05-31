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

import com.github.ambry.router.Callback;
import com.github.ambry.server.StatsSnapshot;


public class AccountServiceCallback implements Callback<StatsSnapshot> {
  private final AccountService accountService;

  /**
   * Construct a AccountServiceCallback object
   * @param accountService the {@link AccountService} associated with this callback.
   */
  public AccountServiceCallback(AccountService accountService) {
    this.accountService = accountService;
  }

  /**
   * When the aggregation report has been generated successfully, this method will be invoked and associated {@link AccountService}
   * will
   * @param results the StatsSnapshot whose values represents aggregated stats across all partitions.
   * @param exception Exception won't affect the execution.
   */
  @Override
  public void onCompletion(StatsSnapshot results, Exception exception) {
    accountService.selectInvalidContainersAndMarkInZK(results);
  }
}
