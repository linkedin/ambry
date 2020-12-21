/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Utils for testing and initializing quota.
 */
public class QuotaTestUtils {

  /**
   * Create a dummy {@link QuotaConfig} object with empty string values for required configs.
   * @return QuotaConfig object.
   */
  public static QuotaConfig createDummyQuotaConfig() {
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    return new QuotaConfig(new VerifiableProperties(properties));
  }

  /**
   * Create {@link QuotaConfig} object with {@link RequestQuotaEnforcer} and corresponding {@link QuotaSource} map.
   * @param enforcerSourcemap {@link Map} for {@link RequestQuotaEnforcer} and corresponding {@link QuotaSource} object.
   * @param isRequestQuotaThrottlingEnabled boolean flag indicating if request quota throttling should be enabled.
   * @param quotaMode {@link QuotaMode} for quota enforcement.
   * @return QuotaConfig object.
   */
  public static QuotaConfig createQuotaConfig(Map<String, String> enforcerSourcemap,
      boolean isRequestQuotaThrottlingEnabled, QuotaMode quotaMode) {
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_THROTTLING_ENABLED, "" + isRequestQuotaThrottlingEnabled);
    properties.setProperty(QuotaConfig.QUOTA_THROTTLING_MODE, quotaMode.name());
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
   * Create {@link QuotaConfig} object with the {@link HostQuotaEnforcer} class.
   * @param hostEnforcerFactoryClass {@link HostQuotaEnforcer} object.
   * @param isRequestThrottlingEnabled boolean flag indicating if request quota throttling should be enabled.
   * @param quotaMode {@link QuotaMode} for quota enforcement.
   * @param isHostThrottlingEnabled boolean flag indicating if host quota throttling should be enabled.
   * @return QuotaConfig object.
   */
  public static QuotaConfig createQuotaConfig(Collection<String> hostEnforcerFactoryClass,
      boolean isRequestThrottlingEnabled, QuotaMode quotaMode, boolean isHostThrottlingEnabled) {
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_THROTTLING_ENABLED, "" + isRequestThrottlingEnabled);
    properties.setProperty(QuotaConfig.HOST_QUOTA_THROTTLING_ENABLED, "" + isHostThrottlingEnabled);
    properties.setProperty(QuotaConfig.QUOTA_THROTTLING_MODE, quotaMode.name());
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON, "");
    String hostEnforcerFactoryClassConfig = "";
    for (String factoryClass : hostEnforcerFactoryClass) {
      if (hostEnforcerFactoryClassConfig.isEmpty()) {
        hostEnforcerFactoryClassConfig = factoryClass;
      } else {
        hostEnforcerFactoryClassConfig += "," + factoryClass;
      }
    }
    properties.setProperty(QuotaConfig.HOST_QUOTA_ENFORCER_FACTORIES, hostEnforcerFactoryClassConfig);
    return new QuotaConfig(new VerifiableProperties(properties));
  }
}
