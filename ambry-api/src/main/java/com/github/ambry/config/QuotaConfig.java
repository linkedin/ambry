/*
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
package com.github.ambry.config;

import com.github.ambry.quota.QuotaMode;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Config for quota of various resources in Ambry.
 */
public class QuotaConfig {
  public static final String QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR = "quotaEnforcerSourcePairInfo";
  public static final String ENFORCER_STR = "enforcer";
  public static final String SOURCE_STR = "source";
  public static final String QUOTA_CONFIG_PREFIX = "quota.";
  public static final String REQUEST_THROTTLING_ENABLED = QUOTA_CONFIG_PREFIX + "request.throttling.enabled";
  public static final String THROTTLING_MODE = QUOTA_CONFIG_PREFIX + "throttling.mode";
  public static final String REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON =
      QUOTA_CONFIG_PREFIX + "request.enforcer.source.pair.info.json";
  public static final String QUOTA_MANAGER_FACTORY = QUOTA_CONFIG_PREFIX + "manager.factory";
  public static final String QUOTA_ACCOUNTING_UNIT = QUOTA_CONFIG_PREFIX + "accounting.unit";
  public static final String MAX_FRONTEND_CU_USAGE_TO_ALLOW_EXCEED =
      QUOTA_CONFIG_PREFIX + "max.frontend.cu.usage.to.allow.exceed";
  public static final String RESOURCE_CU_QUOTA_IN_JSON = QUOTA_CONFIG_PREFIX + "resource.cu.quota.in.json";
  public static final String FRONTEND_CU_CAPACITY_IN_JSON = QUOTA_CONFIG_PREFIX + "frontend.cu.capacity.in.json";
  public static final String BANDWIDTH_THROTTLING_FEATURE_ENABLED =
      QUOTA_CONFIG_PREFIX + "bandwidth.throttling.feature.enabled";
  public static final String CU_QUOTA_AGGREGATION_WINDOW_IN_SECS =
      QUOTA_CONFIG_PREFIX + "cu.quota.aggregation.windows.in.secs";
  public static final String QUOTA_USAGE_WARNING_THRESHOLD_IN_PERCENTAGE =
      QUOTA_CONFIG_PREFIX + "usage.warning.threshold.in.precentage";

  public static final String DEFAULT_QUOTA_MANAGER_FACTORY = "com.github.ambry.quota.AmbryQuotaManagerFactory";
  public static final String DEFAULT_QUOTA_THROTTLING_MODE = QuotaMode.TRACKING.name();
  public static final long DEFAULT_QUOTA_ACCOUNTING_UNIT = 1024; //1kb
  public static final float DEFAULT_MAX_FRONTEND_CU_USAGE_TO_ALLOW_EXCEED = 80.0f;
  public static final String DEFAULT_CU_QUOTA_IN_JSON = "{}";
  public static final String DEFAULT_FRONTEND_BANDWIDTH_CAPACITY_IN_JSON = "{}";
  public static final boolean DEFAULT_BANDWIDTH_THROTTLING_FEATURE_ENABLED = false;
  public static final long DEFAULT_CU_QUOTA_AGGREGATION_WINDOW_IN_SECS = 10;
  public static final int DEFAULT_QUOTA_USAGE_WARNING_THRESHOLD_IN_PERCENTAGE = 80;
  public StorageQuotaConfig storageQuotaConfig;

  /**
   * Serialized json containing pairs of enforcer classes and corresponding source classes.
   * This information should be of the following form:
   * <pre>
   * {
   *   "quotaEnforcerSourcePairInfo" : [
   *     {
   *       "enforcer":"com.github.ambry.quota.QuotaEnforcer",
   *       "source": "com.github.ambry.quota.QuotaSource"
   *     },
   *     {
   *       "enforcer":"com.github.ambry.quota.AnotherQuotaEnforcer",
   *       "source": "com.github.ambry.quota.AnotherQuotaSource"
   *     }
   *   ]
   * }
   * </pre>
   */
  @Config(REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON)
  public String requestQuotaEnforcerSourcePairInfoJson;

  /**
   * The quota manager factory class.
   */
  @Config(QUOTA_MANAGER_FACTORY)
  @Default(DEFAULT_QUOTA_MANAGER_FACTORY)
  public String quotaManagerFactory;

  /**
   * The mode in which quota throttling is being done (TRACKING/THROTTLING). To throttle the requests, you have to change
   * the mode to THROTTLING and turn on requestThrottlingEnabled or storageQuotaConfig.shouldThrottle (or both).
   */
  @Config(THROTTLING_MODE)
  public QuotaMode throttlingMode;

  /**
   * Config to enable request throttling on customer's account or container.
   */
  @Config(REQUEST_THROTTLING_ENABLED)
  @Default("true")
  public boolean requestThrottlingEnabled;

  /**
   * Size of chunk that is considered for one unit of quota.
   */
  @Config(QUOTA_ACCOUNTING_UNIT)
  public long quotaAccountingUnit;

  /**
   * A JSON string representing CU quota for all accounts and containers. eg:
   * {
   *   "101": {
   *     "1": {
   *       "rcu": 1024000000,
   *       "wcu": 1024000000
   *     },
   *     "1": {
   *       "rcu": 258438456,
   *       "wcu": 258438456
   *     },
   *   },
   *   "102": {
   *     "1": {
   *       "rcu": 1024000000,
   *       "wcu": 1024000000
   *     }
   *   },
   *   "103": {
   *     "rcu": 10737418240,
   *     "wcu": 10737418240
   *   }
   * }
   * The key of the top object is the account id and the key of the inner object is the container id.
   * If there is no inner object, then the quota is for account.
   * Each quota comprises of a rcu value representing read capacity unit quota, and a wcu value
   * representing write capacity unit quota.
   */
  @Config(RESOURCE_CU_QUOTA_IN_JSON)
  @Default("{}")
  public final String resourceCUQuotaInJson;

  /**
   * A JSON string representing bandwidth capacity of frontend node in terms of read capacity unit and write capacity unit.
   * {
   *   "rcu": 1024000000,
   *   "wcu": 1024000000
   * }
   */
  @Config(FRONTEND_CU_CAPACITY_IN_JSON)
  @Default("{}")
  public final String frontendCUCapacityInJson;

  /**
   * Flag to identify if the bandwidth throttling feature is enabled.
   */
  @Config(BANDWIDTH_THROTTLING_FEATURE_ENABLED)
  public boolean bandwidthThrottlingFeatureEnabled;

  /**
   * Aggregation window for CU quotas.
   */
  @Config(CU_QUOTA_AGGREGATION_WINDOW_IN_SECS)
  public long cuQuotaAggregationWindowInSecs;

  /**
   * Quota usage threshold in percentage at which Ambry will generate usage warning in response headers.
   */
  @Config(QUOTA_USAGE_WARNING_THRESHOLD_IN_PERCENTAGE)
  public int quotaUsageWarningThresholdInPercentage;

  /*
   * Threshold of CU usage percentage of frontend to allow requests to exceed quota.
   */
  @Config(MAX_FRONTEND_CU_USAGE_TO_ALLOW_EXCEED)
  public float maxFrontendCuUsageToAllowExceed;

  /**
   * Constructor for {@link QuotaConfig}.
   * @param verifiableProperties {@link VerifiableProperties} object.
   */
  public QuotaConfig(VerifiableProperties verifiableProperties) {
    storageQuotaConfig = new StorageQuotaConfig(verifiableProperties);
    requestThrottlingEnabled = verifiableProperties.getBoolean(REQUEST_THROTTLING_ENABLED, true);
    requestQuotaEnforcerSourcePairInfoJson =
        verifiableProperties.getString(REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON,
            buildDefaultQuotaEnforcerSourceInfoPairJson().toString());
    quotaManagerFactory = verifiableProperties.getString(QUOTA_MANAGER_FACTORY, DEFAULT_QUOTA_MANAGER_FACTORY);
    throttlingMode = QuotaMode.valueOf(verifiableProperties.getString(THROTTLING_MODE, DEFAULT_QUOTA_THROTTLING_MODE));
    quotaAccountingUnit = verifiableProperties.getLong(QUOTA_ACCOUNTING_UNIT, DEFAULT_QUOTA_ACCOUNTING_UNIT);
    resourceCUQuotaInJson = verifiableProperties.getString(RESOURCE_CU_QUOTA_IN_JSON, DEFAULT_CU_QUOTA_IN_JSON);
    frontendCUCapacityInJson =
        verifiableProperties.getString(FRONTEND_CU_CAPACITY_IN_JSON, DEFAULT_FRONTEND_BANDWIDTH_CAPACITY_IN_JSON);
    bandwidthThrottlingFeatureEnabled = verifiableProperties.getBoolean(BANDWIDTH_THROTTLING_FEATURE_ENABLED,
        DEFAULT_BANDWIDTH_THROTTLING_FEATURE_ENABLED);
    cuQuotaAggregationWindowInSecs = verifiableProperties.getLongInRange(CU_QUOTA_AGGREGATION_WINDOW_IN_SECS,
        DEFAULT_CU_QUOTA_AGGREGATION_WINDOW_IN_SECS, 1, Long.MAX_VALUE);
    quotaUsageWarningThresholdInPercentage =
        verifiableProperties.getIntInRange(QUOTA_USAGE_WARNING_THRESHOLD_IN_PERCENTAGE,
            DEFAULT_QUOTA_USAGE_WARNING_THRESHOLD_IN_PERCENTAGE, 0, 100);
    maxFrontendCuUsageToAllowExceed = verifiableProperties.getFloatInRange(MAX_FRONTEND_CU_USAGE_TO_ALLOW_EXCEED,
        DEFAULT_MAX_FRONTEND_CU_USAGE_TO_ALLOW_EXCEED, 0.0f, 100.0f);
  }

  /**
   * Build the default quota enforcer and source pair json.
   * @return JSONObject representing the pair json.
   */
  private static JSONObject buildDefaultQuotaEnforcerSourceInfoPairJson() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ENFORCER_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcerFactory");
    jsonObject.put(SOURCE_STR, "com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory");
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(jsonObject);
    return new JSONObject().put(QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, jsonArray);
  }
}
