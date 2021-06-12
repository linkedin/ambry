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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import java.io.IOException;


/**
 * Implementation of {@link QuotaSourceFactory} that instantiates {@link UnlimitedQuotaSourceFactory}.
 */
public class JsonStringCapacityUnitQuotaSourceFactory implements QuotaSourceFactory {
  private final QuotaConfig quotaConfig;

  /**
   * Constructor for {@link UnlimitedQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   */
  public JsonStringCapacityUnitQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService) {
    this.quotaConfig = quotaConfig;
  }

  @Override
  public QuotaSource getQuotaSource() {
    try {
      return new JsonStringCapacityUnitQuotaSource(quotaConfig);
    } catch (IOException ioEx) {
      throw new IllegalStateException(String.format("Unable to create JsonStringCapacityUnitQuotaSource due to %s",
          ioEx.toString()));
    }
  }
}
