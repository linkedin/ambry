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
package com.github.ambry.quota;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcer;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * Tests for {@link AmbryQuotaManager} and {@link AtomicAmbryQuotaManager}.
 */
@RunWith(Parameterized.class)
public class AmbryQuotaManagerTest {
  private final static InMemAccountService ACCOUNT_SERVICE = new InMemAccountService(false, true);
  private final static Account ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
  private final static Account ANOTHER_ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
  private final TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy testQuotaRecommendationMergePolicy;
  private final boolean isAtomic;
  private final boolean isRequestThrottlingEnabled;
  private final QuotaAction expectedDelayRecommendationQuotaExceed;
  private final QuotaAction expectedRejectRecommendationQuotaExceed;
  private final RouterConfig routerConfig;
  private TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer testAmbryCUQuotaEnforcer;
  private QuotaConfig quotaConfig;

  /**
   * Constructor for {@link AmbryQuotaManagerTest}.
   * @param isAtomic flag. If set to {@code true} {@link AtomicAmbryQuotaManager} is tested.
   *                 Otherwise {@link AmbryQuotaManager} is tested.
   * @param isRequestThrottlingEnabled flag to disable and enable request throttling enabled in tests.
   */
  public AmbryQuotaManagerTest(boolean isAtomic, boolean isRequestThrottlingEnabled) {
    TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy.quotaRecommendationsCount = 0;
    this.isAtomic = isAtomic;
    this.isRequestThrottlingEnabled = isRequestThrottlingEnabled;
    // If request throttling is disabled, then even though quota is exceeded, the recommendation should be ALLOW.
    this.expectedDelayRecommendationQuotaExceed = isRequestThrottlingEnabled ? QuotaAction.DELAY : QuotaAction.ALLOW;
    this.expectedRejectRecommendationQuotaExceed = isRequestThrottlingEnabled ? QuotaAction.REJECT : QuotaAction.ALLOW;
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, Boolean.toString(isRequestThrottlingEnabled));
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    testQuotaRecommendationMergePolicy = new TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy(quotaConfig);
    routerConfig = QuotaTestUtils.getDefaultRouterConfig();
  }

  /**
   * Running for both {@link AmbryQuotaManager} as well as {@link AtomicAmbryQuotaManager}.
   * @return an array container a flag that is {@code true} for {@link AtomicAmbryQuotaManager} and {@code false} for
   * {@link AmbryQuotaManager}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{true, true}, {false, true}, {true, false}, {false, false}});
  }

  @Test
  public void testInit() throws Exception {
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManagerWithAmbryCUEnforcer();
    ambryQuotaManager.init();
    // 1. test that init is called for quota enforcers.
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);

    // 2. test that if init is called again, quota enforcer init is not called again.
    ambryQuotaManager.init();
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);

    // 3. test that is a quota enforcer init throws exception, quota manager init also throws exception.
    ambryQuotaManager.shutdown();
    testAmbryCUQuotaEnforcer.resetDefaults();
    testAmbryCUQuotaEnforcer.throwException = true;
    try {
      ambryQuotaManager.init();
      Assert.fail("init should throw an exception.");
    } catch (Exception ignored) {
    }
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);

    // 4. test that if quota manager init threw exception, the initialization wasn't done.
    testAmbryCUQuotaEnforcer.throwException = false;
    ambryQuotaManager.init();
    Assert.assertEquals(2, testAmbryCUQuotaEnforcer.initCallCount);
  }

  @Test
  public void testShutdown() throws Exception {
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManagerWithAmbryCUEnforcer();

    // 1. test that shutdown doesn't do anything if init isn't called yet.
    ambryQuotaManager.shutdown();
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.shutdownCallCount);

    // 2. test that shutdown works after init.
    ambryQuotaManager.init();
    ambryQuotaManager.shutdown();
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.shutdownCallCount);

    // 3. test that repeated call of shutdown doesn't do anything.
    ambryQuotaManager.shutdown();
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.shutdownCallCount);
  }

  @Test
  public void testRecommend() throws Exception {
    RestRequest restRequest =
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET);
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManagerWithNoEnforcers();
    ambryQuotaManager.init();

    // 1. test that recommend returns null with not enforcers.
    Assert.assertNull(ambryQuotaManager.recommend(restRequest));

    // 2. test that enforcers are called.
    ambryQuotaManager = buildAmbryQuotaManagerWithAmbryCUEnforcer();
    testAmbryCUQuotaEnforcer.resetDefaults();
    ambryQuotaManager.init();
    ThrottlingRecommendation throttlingRecommendation = ambryQuotaManager.recommend(restRequest);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(throttlingRecommendation.getQuotaAction(), QuotaAction.ALLOW);
    Assert.assertEquals(2, TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy.quotaRecommendationsCount);

    // 3. test that if any quota enforcer throws exception, then other quota enforcers are called nonetheless
    testAmbryCUQuotaEnforcer.resetDefaults();
    testAmbryCUQuotaEnforcer.throwException = true;
    throttlingRecommendation = ambryQuotaManager.recommend(restRequest);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(throttlingRecommendation.getQuotaAction(), QuotaAction.ALLOW);
    Assert.assertEquals(3, TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy.quotaRecommendationsCount);

    ambryQuotaManager = buildAmbryQuotaManager();
    List<QuotaEnforcer> quotaEnforcers = new ArrayList<>(ambryQuotaManager.quotaEnforcers);
    TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer testAmbryCUQuotaEnforcer1 =
        (TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer) quotaEnforcers.get(0);
    TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer testAmbryCUQuotaEnforcer2 =
        (TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer) quotaEnforcers.get(1);
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    ambryQuotaManager.init();

    // 4. test that if any quota enforcer recommends DELAY action, then that's the recommendation
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer1.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.DELAY_QUOTA_RECOMMENDATION;
    testAmbryCUQuotaEnforcer2.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.ALLOW_QUOTA_RECOMMENDATION;
    throttlingRecommendation = ambryQuotaManager.recommend(restRequest);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(throttlingRecommendation.getQuotaAction(), expectedDelayRecommendationQuotaExceed);
    Assert.assertEquals(5, TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy.quotaRecommendationsCount);

    // 5. test that if any quota enforcer recommends REJECT action, then that's the recommendation
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer1.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.DELAY_QUOTA_RECOMMENDATION;
    testAmbryCUQuotaEnforcer2.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.REJECT_QUOTA_RECOMMENDATION;
    throttlingRecommendation = ambryQuotaManager.recommend(restRequest);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(throttlingRecommendation.getQuotaAction(), expectedRejectRecommendationQuotaExceed);
    Assert.assertEquals(7, TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy.quotaRecommendationsCount);

    // 6. test if all the enforcers throw exception.
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer1.throwException = true;
    testAmbryCUQuotaEnforcer2.throwException = true;
    throttlingRecommendation = ambryQuotaManager.recommend(restRequest);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(throttlingRecommendation.getQuotaAction(), QuotaAction.ALLOW);
    Assert.assertEquals(7, TestCUQuotaEnforcerFactory.TestQuotaRecommendationMergePolicy.quotaRecommendationsCount);
  }

  @Test
  public void testChargeAndRecommend() throws Exception {
    testChargeAndRecommendForNoExceedNoForce();
    testChargeAndRecommendForExceeds();
    testChargeAndRecommendForForcedCharge();
  }

  /**
   * Test {@link QuotaManager#chargeAndRecommend} when quota exceed check and force charge are set to false.
   * @throws Exception in case of any exception.
   */
  private void testChargeAndRecommendForNoExceedNoForce() throws Exception {
    RestRequest restRequest =
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET);
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManagerWithNoEnforcers();
    ambryQuotaManager.init();

    // 1. test that chargeAndRecommend returns ALLOW with not enforcers.
    QuotaAction quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, null, false, false);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);

    // 2. test that enforcers are called.
    ambryQuotaManager = buildAmbryQuotaManagerWithAmbryCUEnforcer();
    testAmbryCUQuotaEnforcer.resetDefaults();
    ambryQuotaManager.init();
    Map<QuotaName, Double> costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 11.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 3. test that if one enforcer recommends delay then no charge is done and delay is returned.
    testAmbryCUQuotaEnforcer.resetDefaults();
    ambryQuotaManager.init();
    costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 1.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
    Assert.assertEquals(expectedDelayRecommendationQuotaExceed, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(isRequestThrottlingEnabled ? 0 : 1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(isRequestThrottlingEnabled ? 110 : 120, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 4. test that if one enforcer recommends reject then no charge is done and reject is returned.
    ambryQuotaManager = buildAmbryQuotaManager();
    List<QuotaEnforcer> quotaEnforcers = new ArrayList<>(ambryQuotaManager.quotaEnforcers);
    TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer testAmbryCUQuotaEnforcer1 =
        (TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer) quotaEnforcers.get(0);
    TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer testAmbryCUQuotaEnforcer2 =
        (TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer) quotaEnforcers.get(1);
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    ambryQuotaManager.init();

    testAmbryCUQuotaEnforcer1.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.REJECT_QUOTA_RECOMMENDATION;
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
    Assert.assertEquals(QuotaAction.REJECT, quotaAction);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.chargeCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.initCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.isQuotaExceedAllowedCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.isQuotaExceedAllowedCallCount);

    // 4a. flip the enforcer that returns reject.
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer2.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.REJECT_QUOTA_RECOMMENDATION;
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
    Assert.assertEquals(QuotaAction.REJECT, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.isQuotaExceedAllowedCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.isQuotaExceedAllowedCallCount);

    // 5. test that if one enforcer recommends reject and another returns delay then no charge is done and reject is returned.
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer1.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.REJECT_QUOTA_RECOMMENDATION;
    testAmbryCUQuotaEnforcer2.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.DELAY_QUOTA_RECOMMENDATION;
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
    Assert.assertEquals(QuotaAction.REJECT, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.initCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.isQuotaExceedAllowedCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.isQuotaExceedAllowedCallCount);

    // 5a. flip the enforcer that returns reject.
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer1.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.DELAY_QUOTA_RECOMMENDATION;
    testAmbryCUQuotaEnforcer2.recommendReturnVal =
        TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer.REJECT_QUOTA_RECOMMENDATION;
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
    Assert.assertEquals(QuotaAction.REJECT, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.isQuotaExceedAllowedCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.isQuotaExceedAllowedCallCount);

    // 6. test that if any enforcer's recommend throws exception, the exception is thrown from chargeAndRecommend.
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer1.throwException = true;
    try {
      ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
      Assert.fail("If any enforcer's recommend throws exception, then chargeAndRecommend should throw exception.");
    } catch (QuotaException ignored) {
    }
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.initCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.isQuotaExceedAllowedCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.isQuotaExceedAllowedCallCount);

    // 6a. flip the enforcer that throws exception
    testAmbryCUQuotaEnforcer1.resetDefaults();
    testAmbryCUQuotaEnforcer2.resetDefaults();
    testAmbryCUQuotaEnforcer2.throwException = true;
    try {
      ambryQuotaManager.chargeAndRecommend(restRequest, costMap, false, false);
      Assert.fail("If any enforcer's recommend throws exception, then chargeAndRecommend should throw exception.");
    } catch (QuotaException ignored) {
    }
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer1.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer2.recommendCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.chargeCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer1.isQuotaExceedAllowedCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer2.isQuotaExceedAllowedCallCount);
  }

  /**
   * Test {@link QuotaManager#chargeAndRecommend} when quota exceed check is set to true and force charge is set to false.
   * @throws Exception in case of any exception.
   */
  private void testChargeAndRecommendForExceeds() throws Exception {
    RestRequest restRequest =
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET);
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManagerWithNoEnforcers();
    ambryQuotaManager.init();

    // 1. test that chargeAndRecommend returns ALLOW with not enforcers.
    QuotaAction quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, null, true, false);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);

    // 2. test that enforcers are called and usage is charged if usage is within quota limits.
    ambryQuotaManager = buildAmbryQuotaManagerWithAmbryCUEnforcer();
    testAmbryCUQuotaEnforcer.resetDefaults();
    ambryQuotaManager.init();
    Map<QuotaName, Double> costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 11.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, false);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(11 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 3. test that enforcers are called and usage is charged if usage is outside quota limits, but quota exceed is allowed.
    testAmbryCUQuotaEnforcer.resetDefaults();
    costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 100.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, false);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(1110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(111 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 4. test that enforcers are called and usage is not charged if usage is outside quota limits and quota exceed is not allowed.
    testAmbryCUQuotaEnforcer.resetDefaults();
    costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 100.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, false);
    Assert.assertEquals(expectedDelayRecommendationQuotaExceed, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(isRequestThrottlingEnabled ? 0 : 1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(isRequestThrottlingEnabled ? 1110 : 2110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(isRequestThrottlingEnabled ? 111 * routerConfig.routerGetRequestParallelism
            : 211 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 5. test that even if quota exceed allowed might have returned false, if a request is within its limits, the recommendation is allow
    testAmbryCUQuotaEnforcer.resetDefaults();
    costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 100.0);
    restRequest =
        QuotaTestUtils.createRestRequest(ANOTHER_ACCOUNT, ANOTHER_ACCOUNT.getAllContainers().iterator().next(),
            RestMethod.GET);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, false);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(1000, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(isRequestThrottlingEnabled ? 211 * routerConfig.routerGetRequestParallelism
            : 311 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);
  }

  /**
   * Test {@link QuotaManager#chargeAndRecommend} when quota exceed check is set to true and force charge is set to true.
   * @throws Exception in case of any exception.
   */
  private void testChargeAndRecommendForForcedCharge() throws Exception {
    RestRequest restRequest =
        QuotaTestUtils.createRestRequest(ACCOUNT, ACCOUNT.getAllContainers().iterator().next(), RestMethod.GET);
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManagerWithNoEnforcers();
    ambryQuotaManager.init();

    // 1. test that chargeAndRecommend returns ALLOW if there are no enforcers.
    QuotaAction quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, null, true, true);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);

    // 2. test that enforcers are called and usage is charged.
    ambryQuotaManager = buildAmbryQuotaManagerWithAmbryCUEnforcer();
    testAmbryCUQuotaEnforcer.resetDefaults();
    ambryQuotaManager.init();
    Map<QuotaName, Double> costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 11.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, true);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(11 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 3. test that enforcers are called and usage is charged even if usage is outside quota limits.
    testAmbryCUQuotaEnforcer.resetDefaults();
    costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 100.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, true);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(1110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(111 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);

    // 3. test that enforcers are called and usage is charged even if usage is outside quota limits and exceed is not allowed.
    testAmbryCUQuotaEnforcer.resetDefaults();
    costMap = Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, 100.0);
    quotaAction = ambryQuotaManager.chargeAndRecommend(restRequest, costMap, true, true);
    Assert.assertEquals(QuotaAction.ALLOW, quotaAction);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.initCallCount);
    Assert.assertEquals(0, testAmbryCUQuotaEnforcer.recommendCallCount);
    Assert.assertEquals(1, testAmbryCUQuotaEnforcer.chargeCallCount);
    Assert.assertEquals(2110, testAmbryCUQuotaEnforcer.getQuotaSource()
        .getUsage(QuotaResource.fromRestRequest(restRequest), QuotaName.READ_CAPACITY_UNIT), 0.0001);
    Assert.assertEquals(211 * routerConfig.routerGetRequestParallelism,
        testAmbryCUQuotaEnforcer.getQuotaSource().getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.0001);
  }

  @Test
  public void testGetQuotaConfig() throws ReflectiveOperationException {
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManager();
    Assert.assertEquals(quotaConfig, ambryQuotaManager.getQuotaConfig());
  }

  @Test
  public void testGetQuotaMode() throws ReflectiveOperationException {
    // test that quota mode is initialized based on config.
    AmbryQuotaManager ambryQuotaManager = buildAmbryQuotaManager();
    QuotaMode quotaMode = quotaConfig.throttlingMode;
    Assert.assertEquals(quotaMode, ambryQuotaManager.getQuotaMode());

    // test that quota mode is set correctly.
    ambryQuotaManager.setQuotaMode(QuotaMode.THROTTLING);
    Assert.assertEquals(QuotaMode.THROTTLING, ambryQuotaManager.getQuotaMode());

    ambryQuotaManager.setQuotaMode(QuotaMode.TRACKING);
    Assert.assertEquals(QuotaMode.TRACKING, ambryQuotaManager.getQuotaMode());
  }

  /**
   * Build {@link AmbryQuotaManager} object with {@link AmbryCUQuotaEnforcer} as one of the enforcers.
   * @return AmbryQuotaManager object.
   * @throws ReflectiveOperationException in case the {@link AmbryQuotaManager} object could not be created.
   */
  private AmbryQuotaManager buildAmbryQuotaManagerWithAmbryCUEnforcer() throws ReflectiveOperationException {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON, String.format(
        "{\n" + "    \"%s\": {\n" + "        \"wcu\": %d,\n" + "        \"rcu\": %d\n" + "    },\n" + "    \"%s\": {\n"
            + "        \"wcu\": %d,\n" + "        \"rcu\": %d\n" + "    }\n" + "}", String.valueOf(ACCOUNT.getId()), 10,
        10, String.valueOf(ANOTHER_ACCOUNT.getId()), 10, 10));
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON,
        String.format("{\n" + "  \"wcu\": %d,\n" + "  \"rcu\": %d\n" + "}", 100, 100));
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, Boolean.toString(isRequestThrottlingEnabled));
    quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    quotaConfig.requestQuotaEnforcerSourcePairInfoJson = buildDefaultQuotaEnforcerSourceInfoPairJson().toString();
    QuotaMetrics quotaMetrics = new QuotaMetrics(new MetricRegistry());
    AmbryQuotaManager ambryQuotaManager = createAmbryQuotaManager(quotaConfig, quotaMetrics);
    Assert.assertEquals(2, ambryQuotaManager.quotaEnforcers.size());
    List<QuotaEnforcer> quotaEnforcers = new ArrayList<>(ambryQuotaManager.quotaEnforcers);
    quotaEnforcers.sort(Comparator.comparing(o -> o.getClass().getSimpleName()));
    Assert.assertTrue(quotaEnforcers.get(0) instanceof AmbryCUQuotaEnforcer);
    Assert.assertTrue(quotaEnforcers.get(1) instanceof TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer);
    testAmbryCUQuotaEnforcer = (TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer) quotaEnforcers.get(1);
    return ambryQuotaManager;
  }

  /**
   * Build {@link AmbryQuotaManager} object with two test enforcers.
   * @return AmbryQuotaManager object.
   * @throws ReflectiveOperationException in case the {@link AmbryQuotaManager} object could not be created.
   */
  private AmbryQuotaManager buildAmbryQuotaManager() throws ReflectiveOperationException {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, Boolean.toString(isRequestThrottlingEnabled));
    quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    quotaConfig.requestQuotaEnforcerSourcePairInfoJson = buildTestQuotaEnforcerSourceInfoPairJson().toString();
    QuotaMetrics quotaMetrics = new QuotaMetrics(new MetricRegistry());
    AmbryQuotaManager ambryQuotaManager = createAmbryQuotaManager(quotaConfig, quotaMetrics);
    Assert.assertEquals(2, ambryQuotaManager.quotaEnforcers.size());
    List<QuotaEnforcer> quotaEnforcers = new ArrayList<>(ambryQuotaManager.quotaEnforcers);
    Assert.assertTrue(quotaEnforcers.get(0) instanceof TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer);
    Assert.assertTrue(quotaEnforcers.get(1) instanceof TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer);
    testAmbryCUQuotaEnforcer = (TestCUQuotaEnforcerFactory.TestAmbryCUQuotaEnforcer) quotaEnforcers.get(1);
    return ambryQuotaManager;
  }

  /**
   * Build {@link AmbryQuotaManager} object with no enforcers.
   * @return AmbryQuotaManager object.
   * @throws ReflectiveOperationException in case the {@link AmbryQuotaManager} object could not be created.
   */
  private AmbryQuotaManager buildAmbryQuotaManagerWithNoEnforcers() throws ReflectiveOperationException {
    quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
    quotaConfig.requestQuotaEnforcerSourcePairInfoJson =
        new JSONObject().put(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, new JSONArray()).toString();
    QuotaMetrics quotaMetrics = new QuotaMetrics(new MetricRegistry());
    AmbryQuotaManager ambryQuotaManager = createAmbryQuotaManager(quotaConfig, quotaMetrics);
    Assert.assertTrue(ambryQuotaManager.quotaEnforcers.isEmpty());
    return ambryQuotaManager;
  }

  /**
   * Create one of {@link AmbryQuotaManager} or {@link AtomicAmbryQuotaManager} object based on specified params and isAtomic flag.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @return QuotaManager instance representing {@link AtomicAmbryQuotaManager} if isAtomic is {@code true}. {@link AmbryQuotaManager} object otherwise.
   * @throws ReflectiveOperationException in case the {@link AmbryQuotaManager} could not be created.
   */
  private AmbryQuotaManager createAmbryQuotaManager(QuotaConfig quotaConfig, QuotaMetrics quotaMetrics)
      throws ReflectiveOperationException {
    if (isAtomic) {
      return new AtomicAmbryQuotaManager(quotaConfig, testQuotaRecommendationMergePolicy, ACCOUNT_SERVICE, null,
          quotaMetrics, routerConfig);
    } else {
      return new AmbryQuotaManager(quotaConfig, testQuotaRecommendationMergePolicy, ACCOUNT_SERVICE, null, quotaMetrics,
          routerConfig);
    }
  }

  /**
   * Build the default quota enforcer and source pair json.
   * @return JSONObject representing the pair json.
   */
  private JSONObject buildDefaultQuotaEnforcerSourceInfoPairJson() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(QuotaConfig.ENFORCER_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcerFactory");
    jsonObject.put(QuotaConfig.SOURCE_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory");
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(jsonObject);
    jsonObject = new JSONObject();
    jsonObject.put(QuotaConfig.ENFORCER_STR, "com.github.ambry.quota.TestCUQuotaEnforcerFactory");
    jsonObject.put(QuotaConfig.SOURCE_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory");
    jsonArray.put(jsonObject);
    return new JSONObject().put(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, jsonArray);
  }

  /**
   * Build the test quota enforcer and source pair json.
   * @return JSONObject representing the pair json.
   */
  private JSONObject buildTestQuotaEnforcerSourceInfoPairJson() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(QuotaConfig.ENFORCER_STR, "com.github.ambry.quota.TestCUQuotaEnforcerFactory");
    jsonObject.put(QuotaConfig.SOURCE_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory");
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(jsonObject);
    jsonObject = new JSONObject();
    jsonObject.put(QuotaConfig.ENFORCER_STR, "com.github.ambry.quota.TestCUQuotaEnforcerFactory");
    jsonObject.put(QuotaConfig.SOURCE_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory");
    jsonArray.put(jsonObject);
    return new JSONObject().put(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, jsonArray);
  }
}
