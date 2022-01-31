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
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import java.io.IOException;


/**
 * An implementation of {@link QuotaSourceFactory} for that creates and returns {@link InMemoryCUQuotaSource} object which
 * is the default implementation of Capacity Unit {@link QuotaSource} for Ambry.
 */
public class InMemoryCUQuotaSourceFactory implements QuotaSourceFactory {
  private final InMemoryCUQuotaSource inMemoryCUQuotaSource;

  /**
   * Constructor for {@link InMemoryCUQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   */
  public InMemoryCUQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService) throws IOException {
    inMemoryCUQuotaSource = new InMemoryCUQuotaSource(quotaConfig, accountService);
  }

  @Override
  public QuotaSource getQuotaSource() {
    return inMemoryCUQuotaSource;
  }
}
