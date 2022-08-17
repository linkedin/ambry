/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import java.io.IOException;


/**
 * An implementation of {@link QuotaSourceFactory} for that creates and returns {@link AmbryCUQuotaSource} object.
 */
public class AmbryCUQuotaSourceFactory implements QuotaSourceFactory {
  private final AmbryCUQuotaSource ambryCUQuotaSource;

  /**
   * Constructor for {@link AmbryCUQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   */
  public AmbryCUQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) throws IOException {
    ambryCUQuotaSource = new AmbryCUQuotaSource(quotaConfig, accountService, quotaMetrics, routerConfig);
  }

  @Override
  public QuotaSource getQuotaSource() {
    return ambryCUQuotaSource;
  }
}
