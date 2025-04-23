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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import java.util.Map;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Utils for testing and initializing quota.
 */
public class QuotaTestUtils {

  /**
   * Create {@link QuotaConfig} object with {@link QuotaEnforcer} and corresponding {@link QuotaSource} map.
   * @param enforcerSourcemap {@link Map} for {@link QuotaEnforcer} and corresponding {@link QuotaSource} object.
   * @param isRequestThrottlingEnabled boolean flag indicating if request quota throttling should be enabled.
   * @param quotaMode {@link QuotaMode} for quota enforcement.
   * @return QuotaConfig object.
   */
  public static QuotaConfig createQuotaConfig(Map<String, String> enforcerSourcemap, boolean isRequestThrottlingEnabled,
      QuotaMode quotaMode) {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, "" + isRequestThrottlingEnabled);
    properties.setProperty(QuotaConfig.THROTTLING_MODE, quotaMode.name());
    JSONArray jsonArray = new JSONArray();
    for (String enforcerFactoryClass : enforcerSourcemap.keySet()) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put(QuotaConfig.ENFORCER_STR, enforcerFactoryClass);
      jsonObject.put(QuotaConfig.SOURCE_STR, enforcerSourcemap.get(enforcerFactoryClass));
      jsonArray.put(jsonObject);
    }
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON,
        new JSONObject().put(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, jsonArray).toString());
    return new QuotaConfig(new VerifiableProperties(properties));
  }

  /**
   * @return the default {@link RouterConfig} object.
   */
  public static RouterConfig getDefaultRouterConfig() {
    Properties properties = new Properties();
    properties.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    properties.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, "DEV");
    return new RouterConfig(new VerifiableProperties(properties));
  }

  /**
   * Create a dummy {@link QuotaManager} object, that does nothing, for test.
   * @return QuotaManager object.
   */
  public static QuotaManager createDummyQuotaManager() {
    return new QuotaManager() {
      @Override
      public void init() {
      }

      @Override
      public ThrottlingRecommendation recommend(RestRequest restRequest) {
        return null;
      }

      @Override
      public QuotaConfig getQuotaConfig() {
        return new QuotaConfig(new VerifiableProperties(new Properties()));
      }

      @Override
      public QuotaMode getQuotaMode() {
        return null;
      }

      @Override
      public void setQuotaMode(QuotaMode mode) {

      }

      @Override
      public QuotaAction chargeAndRecommend(RestRequest restRequest, Map<QuotaName, Double> requestCostMap, boolean checkQuotaExceedAllowed, boolean forceCharge) {
        return null;
      }

      @Override
      public void shutdown() {

      }
    };
  }

  /**
   * Create an implementation of {@link QuotaChargeCallback} object for test.
   * @return TestQuotaChargeCallback object.
   */
  public static TestQuotaChargeCallback createTestQuotaChargeCallback() {
    return new TestQuotaChargeCallback();
  }

  /**
   * Create an implementation of {@link QuotaChargeCallback} object for test.
   * @param quotaConfig for the {@link QuotaChargeCallback} implementation.
   * @return TestQuotaChargeCallback object.
   */
  public static TestQuotaChargeCallback createTestQuotaChargeCallback(QuotaConfig quotaConfig) {
    return new TestQuotaChargeCallback(quotaConfig);
  }

  /**
   * Create an implementation of {@link QuotaChargeCallback} object for test.
   * @param quotaMethod {@link QuotaMethod} object.
   * @return TestQuotaChargeCallback object.
   */
  public static TestQuotaChargeCallback createTestQuotaChargeCallback(QuotaMethod quotaMethod) {
    return new TestQuotaChargeCallback(quotaMethod);
  }

  /**
   * Create {@link MockRestRequest} object using the specified {@link Account}, {@link Container} and {@link RestMethod}.
   * @param account {@link Account} object.
   * @param container {@link Container} object.
   * @param restMethod {@link RestMethod} object.
   * @return MockRestRequest object.
   * @throws Exception in case of any exception.
   */
  public static MockRestRequest createRestRequest(Account account, Container container, RestMethod restMethod)
      throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    data.put(MockRestRequest.URI_KEY, "/");
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
    headers.put(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, container);
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data, null);
  }

  /**
   * An implementation of {@link QuotaChargeCallback} for tests.
   */
  public static class TestQuotaChargeCallback implements QuotaChargeCallback {
    public int numCheckAndChargeCalls = 0;
    private final QuotaConfig quotaConfig;
    private final QuotaMethod quotaMethod;

    /**
     * Default constructor for {@link TestQuotaChargeCallback}.
     */
    public TestQuotaChargeCallback() {
      this.quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
      this.quotaMethod = QuotaMethod.READ;
    }

    /**
     * Constructor for {@link TestQuotaChargeCallback} with the specified {@link QuotaConfig}.
     * @param quotaConfig {@link QuotaConfig} object.
     */
    public TestQuotaChargeCallback(QuotaConfig quotaConfig) {
      this.quotaConfig = quotaConfig;
      this.quotaMethod = QuotaMethod.READ;
    }

    /**
     * Constructor for {@link TestQuotaChargeCallback} with the specified {@link QuotaConfig}.
     * @param quotaMethod {@link QuotaMethod} object.
     */
    public TestQuotaChargeCallback(QuotaMethod quotaMethod) {
      this.quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
      this.quotaMethod = quotaMethod;
    }


    @Override
    public QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge, long chunkSize) {
      numCheckAndChargeCalls++;
      return QuotaAction.ALLOW;
    }

    @Override
    public QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge) {
      return checkAndCharge(shouldCheckExceedAllowed, forceCharge, quotaConfig.quotaAccountingUnit);
    }

    @Override
    public QuotaResource getQuotaResource() {
      return new QuotaResource("test", QuotaResourceType.ACCOUNT);
    }

    @Override
    public QuotaMethod getQuotaMethod() {
      return null;
    }

    @Override
    public QuotaConfig getQuotaConfig() {
      return quotaConfig;
    }
  }
}
