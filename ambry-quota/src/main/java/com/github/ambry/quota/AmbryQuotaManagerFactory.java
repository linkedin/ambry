/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;


/**
 * Factory to instantiate {@link AmbryQuotaManager} class.
 */
public class AmbryQuotaManagerFactory implements QuotaManagerFactory {
  private final QuotaManager quotaManager;

  /**
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountStatsStore
   * @throws ReflectiveOperationException
   */
  public AmbryQuotaManagerFactory(QuotaConfig quotaConfig, ThrottlePolicy throttlePolicy, AccountService accountService,
      AccountStatsStore accountStatsStore) throws ReflectiveOperationException {
    AmbryQuotaManager.AmbryQuotaManagerComponents.accountStatsStore = accountStatsStore;
    quotaManager = new AmbryQuotaManager(quotaConfig, throttlePolicy, accountService);
  }

  @Override
  public QuotaManager getQuotaManager() {
    return quotaManager;
  }
}
