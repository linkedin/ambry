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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test for {@link AmbryCUQuotaEnforcerFactory}.
 */
public class AmbryCUQuotaEnforcerFactoryTest {
  @Test
  public void testGetRequestQuotaEnforcer() throws IOException {
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
    QuotaSource quotaSource =
        new AmbryCUQuotaSource(quotaConfig, Mockito.mock(AccountService.class), new QuotaMetrics(new MetricRegistry()),
            QuotaTestUtils.getDefaultRouterConfig());
    AccountStatsStore mockAccountStatsStore = Mockito.mock(AccountStatsStore.class);
    AmbryCUQuotaEnforcerFactory ambryCUQuotaEnforcerFactory =
        new AmbryCUQuotaEnforcerFactory(quotaConfig, quotaSource, mockAccountStatsStore,
            new QuotaMetrics(new MetricRegistry()));
    Assert.assertEquals(AmbryCUQuotaEnforcer.class, ambryCUQuotaEnforcerFactory.getQuotaEnforcer().getClass());
  }
}
