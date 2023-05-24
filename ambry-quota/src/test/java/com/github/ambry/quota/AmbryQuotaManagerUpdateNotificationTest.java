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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AbstractAccountService;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountServiceMetrics;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.AccountServiceConfig;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSource;
import com.github.ambry.utils.AccountTestUtils;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 *  Test that notification for account updates make it all the way to the QuotaSource.
 */
public class AmbryQuotaManagerUpdateNotificationTest {

  @Test
  public void ambryQuotaManagerUpdateNotificationTest() throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    MetricRegistry metricRegistry = new MetricRegistry();

    QuotaConfig quotaConfig = new QuotaConfig(verifiableProperties);
    QuotaRecommendationMergePolicy quotaRecommendationMergePolicy =
        new SimpleQuotaRecommendationMergePolicy(quotaConfig);
    AccountServiceForConsumerTest accountService =
        new AccountServiceForConsumerTest(new AccountServiceConfig(verifiableProperties),
            new AccountServiceMetrics(metricRegistry, false), null);
    AmbryQuotaManager ambryQuotaManager =
        new AmbryQuotaManager(quotaConfig, quotaRecommendationMergePolicy, accountService, null,
            new QuotaMetrics(metricRegistry), QuotaTestUtils.getDefaultRouterConfig());
    AmbryCUQuotaSource quotaSource = (AmbryCUQuotaSource) getQuotaSourceMember(ambryQuotaManager);
    assertTrue("updated accounts should be empty", quotaSource.getAllQuota().isEmpty());

    Set<Short> accountIdSet = new HashSet<>();
    accountIdSet.add((short) 1);
    accountIdSet.add((short) 2);
    Map<Short, Account> idToRefAccountMap = new HashMap<>();
    AccountTestUtils.generateRefAccounts(idToRefAccountMap, new HashMap<>(), accountIdSet, 2, 3);
    accountService.notifyAccountUpdateConsumers(idToRefAccountMap.values());
    assertEquals("Invalid size of updated accounts", quotaSource.getAllQuota().size(), 2);
  }

  /**
   * Gets the first {@link QuotaSource} from the {@link AmbryQuotaManager} object via reflection.
   * @param ambryQuotaManager {@link AmbryQuotaManager} object.
   * @return QuotaSource object.
   * @throws Exception
   */
  private QuotaSource getQuotaSourceMember(AmbryQuotaManager ambryQuotaManager) throws Exception {
    Field field = ambryQuotaManager.getClass().getDeclaredField("quotaEnforcers");
    field.setAccessible(true);
    return new ArrayList<>((Set<QuotaEnforcer>) field.get(ambryQuotaManager)).get(0).getQuotaSource();
  }

  /**
   * {@link AbstractAccountService} implementation to call {@link AbstractAccountService#notifyAccountUpdateConsumers(Collection, boolean)}.
   */
  static class AccountServiceForConsumerTest extends AbstractAccountService {

    /**
     * Constructor for {@link AccountServiceForConsumerTest}.
     * @param config {@link AccountServiceConfig} object.
     * @param accountServiceMetrics {@link AccountServiceMetrics} object.
     * @param notifier {@link Notifier} object.
     */
    public AccountServiceForConsumerTest(AccountServiceConfig config, AccountServiceMetrics accountServiceMetrics,
        Notifier<String> notifier) {
      super(config, accountServiceMetrics, notifier);
    }

    @Override
    protected void checkOpen() {

    }

    @Override
    protected void onAccountChangeMessage(String topic, String message) {

    }

    @Override
    public void updateAccounts(Collection<Account> accounts) {
    }

    @Override
    public void close() {

    }

    /**
     * Method to call {@link AbstractAccountService#notifyAccountUpdateConsumers(Collection, boolean)}.
     * @param updatedAccounts {@link Collection} of updated {@link Account}s.
     */
    public void notifyAccountUpdateConsumers(Collection<Account> updatedAccounts) {
      super.notifyAccountUpdateConsumers(updatedAccounts, false);
    }
  }
}
