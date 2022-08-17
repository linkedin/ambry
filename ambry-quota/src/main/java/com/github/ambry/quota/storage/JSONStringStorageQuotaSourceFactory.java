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
package com.github.ambry.quota.storage;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import java.io.IOException;


/**
 * Factory for {@link JSONStringStorageQuotaSource}.
 */
public class JSONStringStorageQuotaSourceFactory implements QuotaSourceFactory {
  private final QuotaSource source;

  /**
   * Constructor to create a {@link JSONStringStorageQuotaSourceFactory}.
   * @param quotaConfig The {@link QuotaConfig} to use.
   * @param accountService The {@link AccountService} to use.
   * @param quotaMetrics The {@link QuotaMetrics} to use.
   * @param routerConfig The {@link RouterConfig} to use.
   * @throws IOException
   */
  public JSONStringStorageQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService,
      QuotaMetrics quotaMetrics, RouterConfig routerConfig) throws IOException {
    source = new JSONStringStorageQuotaSource(quotaConfig.storageQuotaConfig, accountService);
  }

  @Override
  public QuotaSource getQuotaSource() {
    return source;
  }
}
