/**
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
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;


/**
 * Factory to create {@link DummyQuotaSource} for test.
 */
public class DummyQuotaSourceFactory implements QuotaSourceFactory {

  /**
   * Constructor for {@link DummyQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   */
  public DummyQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) {

  }

  @Override
  public QuotaSource getQuotaSource() {
    return new DummyQuotaSource();
  }
}
