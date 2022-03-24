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

import com.github.ambry.account.Account;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.mockito.Mockito.*;


/**
 * Test for {@link PreProcessQuotaChargeCallback}.
 */
@RunWith(Parameterized.class)
public class PreProcessQuotaChargeCallbackTest {
  private final QuotaConfig quotaConfig;
  private final SimpleRequestQuotaCostPolicy simpleRequestQuotaCostPolicy;
  private final boolean isThrottling;
  private QuotaManager quotaManager;
  private Account account;

  /**
   * Constructor for {@link PreProcessQuotaChargeCallbackTest}.
   * @param isThrottling {@code true} if running with {@link QuotaMode#THROTTLING}. {@code false} otherwise.
   */
  public PreProcessQuotaChargeCallbackTest(boolean isThrottling) {
    Properties properties = new Properties();
    this.isThrottling = isThrottling;
    if (isThrottling) {
      properties.setProperty(QuotaConfig.THROTTLING_MODE, QuotaMode.THROTTLING.name());
    }
    quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    simpleRequestQuotaCostPolicy = new SimpleRequestQuotaCostPolicy(quotaConfig);
  }

  /**
   * Running for both {@link QuotaMode#TRACKING} as well as {@link QuotaMode#THROTTLING}.
   * @return an array container a flag that is {@code true} for {@link QuotaMode#THROTTLING} and {@code false} for
   * {@link QuotaMode#TRACKING}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.stream(new Boolean[]{Boolean.TRUE, Boolean.FALSE})
        .map(isThrottling -> new Object[]{isThrottling})
        .collect(Collectors.toList());
  }

  @Before
  public void setUp() {
    quotaManager = mock(QuotaManager.class);
    when(quotaManager.getQuotaConfig()).thenReturn(quotaConfig);
    when(quotaManager.getQuotaMode()).thenReturn(quotaConfig.throttlingMode);
    InMemAccountService accountService = new InMemAccountService(false, false);
    account = accountService.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
  }

  @Test
  public void testCheckAndCharge() throws Exception {
    checkAndChargeTest(true);
    checkAndChargeTest(false);
  }

  @Test
  public void testGetQuotaResource() throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET);
    PreProcessQuotaChargeCallback preProcessQuotaChargeCallback =
        new PreProcessQuotaChargeCallback(quotaManager, restRequest);
    QuotaResource quotaResource = preProcessQuotaChargeCallback.getQuotaResource();
    Assert.assertEquals(quotaResource.getResourceId(), String.valueOf(account.getId()));
    Assert.assertEquals(quotaResource.getQuotaResourceType(), account.getQuotaResourceType());
  }

  @Test
  public void testGetQuotaConfig() throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET);
    PreProcessQuotaChargeCallback preProcessQuotaChargeCallback =
        new PreProcessQuotaChargeCallback(quotaManager, restRequest);
    Assert.assertEquals(quotaConfig, preProcessQuotaChargeCallback.getQuotaConfig());
  }

  /**
   * Helper method to test {@link PreProcessQuotaChargeCallback#checkAndCharge} for all possible parameters.
   * @param testWithChunkSize {@code true} if checkAndCharge is called with chunkSize param. {@code false} otherwise.
   * @throws Exception In case of any error
   */
  private void checkAndChargeTest(boolean testWithChunkSize) throws Exception {
    List<Boolean> boolValues = Arrays.asList(true, false);
    for (RestMethod restMethod : RestMethod.values()) {
      RestRequest restRequest = createRestRequest(restMethod);
      PreProcessQuotaChargeCallback preProcessQuotaChargeCallback =
          new PreProcessQuotaChargeCallback(quotaManager, restRequest);
      long chunkSize = quotaConfig.quotaAccountingUnit;
      if (testWithChunkSize) {
        chunkSize = new Random().nextInt(10000);
      }
      Map<QuotaName, Double> costMap = getCostMap(chunkSize, restRequest);
      for (boolean shouldCheckQuotaExceedAllowed : boolValues) {
        for (boolean forceCharge : boolValues) {
          int callCount = 0;
          for (QuotaAction quotaAction : QuotaAction.values()) {
            when(quotaManager.chargeAndRecommend(eq(restRequest), eq(costMap), eq(shouldCheckQuotaExceedAllowed),
                eq(forceCharge))).thenReturn(quotaAction);
            QuotaAction expectedQuotaAction = isThrottling ? quotaAction : QuotaAction.ALLOW;
            if (testWithChunkSize) {
              Assert.assertEquals(expectedQuotaAction,
                  preProcessQuotaChargeCallback.checkAndCharge(shouldCheckQuotaExceedAllowed, forceCharge, chunkSize));
            } else {
              Assert.assertEquals(expectedQuotaAction,
                  preProcessQuotaChargeCallback.checkAndCharge(shouldCheckQuotaExceedAllowed, forceCharge));
            }
            callCount++;
            verify(quotaManager, times(callCount)).chargeAndRecommend(restRequest, costMap,
                shouldCheckQuotaExceedAllowed, forceCharge);
          }
        }
      }
    }
  }

  private RestRequest createRestRequest(RestMethod restMethod) throws Exception {
    JSONObject requestData = new JSONObject();
    requestData.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    requestData.put(MockRestRequest.URI_KEY, "/");
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.InternalKeys.REQUEST_PATH,
        RequestPath.parse("/", Collections.emptyMap(), Collections.emptyList(), "ambry-test"));
    headers.put(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
    headers.put(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, account.getAllContainers().iterator().next());
    requestData.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(requestData, null);
  }

  private Map<QuotaName, Double> getCostMap(long chunkSize, RestRequest restRequest) {
    Map<QuotaName, Double> costMap = new HashMap<>();
    for (Map.Entry<String, Double> entry : simpleRequestQuotaCostPolicy.calculateRequestQuotaCharge(restRequest,
        chunkSize).entrySet()) {
      costMap.put(QuotaName.valueOf(entry.getKey()), entry.getValue());
    }
    return costMap;
  }
}
