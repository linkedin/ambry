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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
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

  public static QuotaManager createDummyQuotaManager() {
    return new QuotaManager() {
      @Override
      public void init() {
      }

      @Override
      public ThrottlingRecommendation getThrottleRecommendation(RestRequest restRequest) {
        return null;
      }

      @Override
      public ThrottlingRecommendation charge(RestRequest restRequest, BlobInfo blobInfo,
          Map<QuotaName, Double> requestCostMap) {
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
      public void shutdown() {

      }
    };
  }

  public static QuotaChargeCallback createDummyQuotaChargeEventListener() {
    return new QuotaChargeCallback() {
      @Override
      public void charge(long chunkSize){
      }

      @Override
      public void charge() {
      }

      @Override
      public boolean check() {
        return false;
      }

      @Override
      public boolean quotaExceedAllowed() {
        return false;
      }

      @Override
      public QuotaResource getQuotaResource() {
        return null;
      }

      @Override
      public QuotaMethod getQuotaMethod() {
        return null;
      }
    };
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
}
