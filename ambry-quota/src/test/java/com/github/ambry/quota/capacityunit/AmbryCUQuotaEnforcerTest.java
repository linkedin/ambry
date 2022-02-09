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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link AmbryCUQuotaEnforcer}.
 */
public class AmbryCUQuotaEnforcerTest {
  private final static long WCU = 10;
  private final static long RCU = 10;
  private final static long FE_WCU = 1024;
  private final static long FE_RCU = 1024;
  private static final InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, false);
  private static AmbryCUQuotaEnforcer AMBRY_QUOTA_ENFORCER;
  private static ExceptionQuotaSource QUOTA_SOURCE;
  private static Account ACCOUNT;

  @Before
  public void setup() throws IOException {
    ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON,
        String.format("{\n" + "  \"%s\": {\n" + "    \"wcu\": %d,\n" + "    \"rcu\": %d\n" + "  }\n" + "}",
            String.valueOf(ACCOUNT.getId()), WCU, RCU));
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON,
        String.format("{\n" + "  \"wcu\": %d,\n" + "  \"rcu\": %d\n" + "}", FE_WCU, FE_RCU));
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    QUOTA_SOURCE = new ExceptionQuotaSource(quotaConfig, ACCOUNT_SERVICE);
    QUOTA_SOURCE.init();
    AMBRY_QUOTA_ENFORCER = new AmbryCUQuotaEnforcer(QUOTA_SOURCE, quotaConfig);
  }

  @Test
  public void testCharge() throws Exception {
    Container container = ACCOUNT.getAllContainers().iterator().next();
    BlobProperties blobProperties = new BlobProperties(10, "test", ACCOUNT.getId(), container.getId(), false);
    BlobInfo blobInfo = new BlobInfo(blobProperties, new byte[10]);
    Map<QuotaName, Double> readRequestCostMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 9.0);
    Map<QuotaName, Double> writeRequestCostMap = Collections.singletonMap(QuotaName.WRITE_CAPACITY_UNIT, 9.0);
    Map<String, CapacityUnit> usageMap = QUOTA_SOURCE.getAllQuotaUsage();

    // 1. Test that usage is updated and recommendation is serve when usage is within limit.
    RestRequest restRequest = QuotaTestUtils.createRestRequest(ACCOUNT, container, RestMethod.GET);
    AMBRY_QUOTA_ENFORCER.charge(restRequest, readRequestCostMap);
    QuotaRecommendation quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(restRequest);
    assertEquals(quotaRecommendation.getQuotaName(), QuotaName.READ_CAPACITY_UNIT);
    assertEquals(quotaRecommendation.getQuotaUsagePercentage(), 90, 0.1);
    assertFalse(quotaRecommendation.shouldThrottle());
    assertEquals(usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu(), 0); // make sure that correct quota is charged.

    restRequest = QuotaTestUtils.createRestRequest(ACCOUNT, container, RestMethod.POST);
    AMBRY_QUOTA_ENFORCER.charge(restRequest, writeRequestCostMap);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(restRequest);
    assertEquals(quotaRecommendation.getQuotaName(), QuotaName.WRITE_CAPACITY_UNIT);
    assertEquals(quotaRecommendation.getQuotaUsagePercentage(), 90, 0.1);

    assertFalse(quotaRecommendation.shouldThrottle());
    assertEquals(usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu(), 9); // make sure that correct quota is charged.

    // 2. Test that retryable quota exception is thrown when quota not found.
    Account newAccount = ACCOUNT_SERVICE.generateRandomAccount(QuotaResourceType.ACCOUNT);
    restRequest =
        QuotaTestUtils.createRestRequest(newAccount, newAccount.getAllContainers().iterator().next(), RestMethod.GET);
    try {
      AMBRY_QUOTA_ENFORCER.charge(restRequest, readRequestCostMap);
      AMBRY_QUOTA_ENFORCER.recommend(restRequest);
      fail("if quota is not found we should see exception");
    } catch (QuotaException quotaException) {
      Assert.assertTrue(quotaException.isRetryable());
    }

    // 3. Test that retryable quota exception is thrown in case of any error.
    restRequest = QuotaTestUtils.createRestRequest(ACCOUNT, container, RestMethod.GET);
    QUOTA_SOURCE.throwException = true;
    try {
      AMBRY_QUOTA_ENFORCER.charge(restRequest, readRequestCostMap);
      fail("QuotaException should be thrown in case of any error.");
    } catch (QuotaException quotaException) {
      Assert.assertTrue(quotaException.isRetryable());
    }
    QUOTA_SOURCE.throwException = false;

    // 4. Test that usage is updated and recommendation is deny when usage >= quota.
    AMBRY_QUOTA_ENFORCER.charge(restRequest, readRequestCostMap);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(restRequest);
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(180, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertTrue(quotaRecommendation.shouldThrottle());
    assertEquals(9, usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.

    restRequest = QuotaTestUtils.createRestRequest(ACCOUNT, container, RestMethod.POST);
    AMBRY_QUOTA_ENFORCER.charge(restRequest, writeRequestCostMap);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(restRequest);
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(180, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertTrue(quotaRecommendation.shouldThrottle());
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.

    restRequest = QuotaTestUtils.createRestRequest(ACCOUNT, container, RestMethod.DELETE);
    AMBRY_QUOTA_ENFORCER.charge(restRequest, writeRequestCostMap);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(restRequest);
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(270, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertTrue(quotaRecommendation.shouldThrottle());
    assertEquals(27,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.

    restRequest = QuotaTestUtils.createRestRequest(ACCOUNT, container, RestMethod.PUT);
    AMBRY_QUOTA_ENFORCER.charge(restRequest, writeRequestCostMap);
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(restRequest);
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(360, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertTrue(quotaRecommendation.shouldThrottle());
    assertEquals(36,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getWcu()); // make sure that correct quota is charged.
    assertEquals(18,
        usageMap.get(String.valueOf(ACCOUNT.getId())).getRcu()); // make sure that correct quota is charged.
  }

  @Test
  public void testRecommend() throws Exception {
    // 1. Test that recommendation is serve when usage is within limit and correct quota is used based on rest method.
    QuotaRecommendation quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET));
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertFalse(quotaRecommendation.shouldThrottle());

    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.POST));
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertFalse(quotaRecommendation.shouldThrottle());

    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.PUT));
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertFalse(quotaRecommendation.shouldThrottle());

    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.DELETE));
    assertEquals(QuotaName.WRITE_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(0, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertFalse(quotaRecommendation.shouldThrottle());

    // 2. Test that retryable QuotaException is thrown when quota not found.
    Account newAccount = ACCOUNT_SERVICE.generateRandomAccount(QuotaResourceType.ACCOUNT);
    try {
      AMBRY_QUOTA_ENFORCER.recommend(
          QuotaTestUtils.createRestRequest(newAccount, newAccount.getAllContainers().iterator().next(),
              RestMethod.GET));
      fail("QuotaException should be thrown when quota not found");
    } catch (QuotaException quotaException) {
      Assert.assertTrue(quotaException.isRetryable());
    }

    // 3. Test that recommendation is serve in case of any error.
    QUOTA_SOURCE.throwException = true;
    try {
      AMBRY_QUOTA_ENFORCER.recommend(
          QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET));
      fail("QuotaException should be thrown.");
    } catch (QuotaException quotaException) {
      Assert.assertTrue(quotaException.isRetryable());
    }
    QUOTA_SOURCE.throwException = false;

    // 4. Test that recommendation is deny when usage >= quota.
    Map<String, CapacityUnit> usageMap = QUOTA_SOURCE.getAllQuotaUsage();
    Map<String, CapacityUnit> quotaMap = QUOTA_SOURCE.getAllQuota();
    String id = String.valueOf(ACCOUNT.getId());
    usageMap.put(id, new CapacityUnit(quotaMap.get(id).getRcu() + 1, quotaMap.get(id).getWcu()));
    float usagePercentage = (float) (usageMap.get(id).getRcu() * 100) / quotaMap.get(id).getRcu();
    quotaRecommendation = AMBRY_QUOTA_ENFORCER.recommend(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET));
    assertEquals(QuotaName.READ_CAPACITY_UNIT, quotaRecommendation.getQuotaName());
    assertEquals(usagePercentage, quotaRecommendation.getQuotaUsagePercentage(), 0.1);
    assertTrue(quotaRecommendation.shouldThrottle());
  }

  @Test
  public void testIsQuotaExceedAllowed() throws Exception {
    // 1. Test that quota exceed is allowed if feusage is lesser than quota.
    assertTrue(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));

    // 2. Test that quota exceed is not allowed if feusage is greater than quota.
    QUOTA_SOURCE.setFeUsage(QUOTA_SOURCE.getFeQuota().getRcu(), QUOTA_SOURCE.getFeQuota().getWcu());
    assertFalse(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));

    // 3. Test that quota exceed allowed doesn't depend upon resource's quota usage.
    Map<String, CapacityUnit> usageMap = QUOTA_SOURCE.getAllQuotaUsage();
    Map<String, CapacityUnit> quotaMap = QUOTA_SOURCE.getAllQuota();
    String id = String.valueOf(ACCOUNT.getId());
    usageMap.put(id, new CapacityUnit(quotaMap.get(id).getRcu() + 1, quotaMap.get(id).getWcu() + 1));
    QUOTA_SOURCE.setFeUsage(0, 0);
    assertTrue(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));

    // 4. Test that quota exceed is allowed in case of any error.
    QUOTA_SOURCE.throwException = true;
    QUOTA_SOURCE.setFeUsage(QUOTA_SOURCE.getFeQuota().getRcu(), QUOTA_SOURCE.getFeQuota().getWcu());
    try {
      assertTrue(AMBRY_QUOTA_ENFORCER.isQuotaExceedAllowed(
          QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET)));
      fail("QuotaException should be thrown.");
    } catch (QuotaException quotaException) {
      assertTrue(quotaException.isRetryable());
    }
    QUOTA_SOURCE.throwException = false;
  }

  @Test
  public void testSupportedQuotaNames() {
    List<QuotaName> supportedQuotas = Arrays.asList(QuotaName.WRITE_CAPACITY_UNIT, QuotaName.READ_CAPACITY_UNIT);
    assertEquals(new HashSet<>(supportedQuotas), new HashSet<>(AMBRY_QUOTA_ENFORCER.supportedQuotaNames()));
  }

  static class ExceptionQuotaSource extends AmbryCUQuotaSource {
    private boolean throwException = false;
    private boolean throwSystemResourceChargeException = false;
    private boolean throwResourceChargeException = false;

    public ExceptionQuotaSource(QuotaConfig config, AccountService accountService) throws IOException {
      super(config, accountService);
    }

    @Override
    public void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) throws QuotaException {
      if (throwResourceChargeException || throwException) {
        throw new RuntimeException("test exception");
      }
      super.chargeUsage(quotaResource, quotaName, usageCost);
    }

    @Override
    public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
      throwExceptionIfNeeded();
      return super.getQuota(quotaResource, quotaName);
    }

    @Override
    public float getUsage(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
      throwExceptionIfNeeded();
      return super.getUsage(quotaResource, quotaName);
    }

    @Override
    public void updateNewQuotaResources(Collection<Account> quotaResources) {
      throwExceptionIfNeeded();
      super.updateNewQuotaResources(quotaResources);
    }

    @Override
    public float getSystemResourceUsage(QuotaName quotaName) {
      throwExceptionIfNeeded();
      return super.getSystemResourceUsage(quotaName);
    }

    @Override
    public void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) {
      if (throwSystemResourceChargeException || throwException) {
        throw new RuntimeException("test exception");
      }
      super.chargeSystemResourceUsage(quotaName, usageCost);
    }

    public CapacityUnit getFeQuota() {
      return feQuota;
    }

    @Override
    public Map<String, CapacityUnit> getAllQuotaUsage() {
      return cuUsage;
    }

    public void setFeUsage(long rcu, long wcu) {
      feUsage.get().setRcu(rcu);
      feUsage.get().setWcu(wcu);
    }

    private void throwExceptionIfNeeded() {
      if (throwException) {
        throw new RuntimeException("test exception");
      }
    }
  }
}
