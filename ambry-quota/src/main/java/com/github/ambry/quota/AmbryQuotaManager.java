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
import com.codahale.metrics.Timer;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.Utils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QuotaManager} implementation to handle all the quota and quota enforcement for Ambry.
 */
public class AmbryQuotaManager implements QuotaManager {
  private static final Logger logger = LoggerFactory.getLogger(AmbryQuotaManager.class);
  private final Set<QuotaEnforcer> requestQuotaEnforcers;
  private final ThrottlePolicy throttlePolicy;
  private final QuotaConfig quotaConfig;
  private final QuotaMetrics quotaMetrics;

  /**
   * Constructor for {@link AmbryQuotaManager}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param throttlePolicy {@link ThrottlePolicy} object that makes the overall recommendation.
   * @param accountService {@link AccountService} object to get all the accounts and container information.
   * @param accountStatsStore {@link AccountStatsStore} object to get all the account stats related information.
   * @param metricRegistry {@link MetricRegistry} object for creating quota metrics.
   * @throws ReflectiveOperationException in case of any exception.
   */
  public AmbryQuotaManager(QuotaConfig quotaConfig, ThrottlePolicy throttlePolicy, AccountService accountService,
      AccountStatsStore accountStatsStore, MetricRegistry metricRegistry) throws ReflectiveOperationException {
    Map<String, String> quotaEnforcerSourceMap =
        parseQuotaEnforcerAndSourceInfo(quotaConfig.requestQuotaEnforcerSourcePairInfoJson);
    Map<String, QuotaSource> quotaSourceObjectMap =
        buildQuotaSources(quotaEnforcerSourceMap.values(), quotaConfig, accountService);
    requestQuotaEnforcers = new HashSet<>();
    for (String quotaEnforcerFactory : quotaEnforcerSourceMap.keySet()) {
      requestQuotaEnforcers.add(((QuotaEnforcerFactory) Utils.getObj(quotaEnforcerFactory, quotaConfig,
          quotaSourceObjectMap.get(quotaEnforcerSourceMap.get(quotaEnforcerFactory)),
          accountStatsStore)).getRequestQuotaEnforcer());
    }
    this.throttlePolicy = throttlePolicy;
    this.quotaConfig = quotaConfig;
    this.quotaMetrics = new QuotaMetrics(metricRegistry);
  }

  @Override
  public void init() throws InstantiationException {
    Timer.Context timer = quotaMetrics.quotaManagerInitTime.time();
    try {
      for (QuotaEnforcer quotaEnforcer : requestQuotaEnforcers) {
        quotaEnforcer.init();
      }
    } catch (Exception e) {
      logger.error("Failed to init quotaEnforcer", e);
      if (e instanceof InstantiationException) {
        throw (InstantiationException) e;
      } else {
        throw new InstantiationException(e.getMessage());
      }
    } finally {
      timer.stop();
    }
  }

  @Override
  public ThrottlingRecommendation getThrottleRecommendation(RestRequest restRequest) {
    if (!quotaConfig.requestThrottlingEnabled || requestQuotaEnforcers.isEmpty()) {
      return null;
    }
    ThrottlingRecommendation throttlingRecommendation = null;
    Timer.Context timer = quotaMetrics.quotaEnforcementTime.time();
    try {
      List<QuotaRecommendation> quotaRecommendations = requestQuotaEnforcers.stream()
          .map(quotaEnforcer -> quotaEnforcer.recommend(restRequest))
          .filter(quotaRecommendation -> quotaRecommendation != null)
          .collect(Collectors.toList());
      if (quotaRecommendations.size() == 0) {
        quotaMetrics.quotaNotEnforcedCount.inc();
      }
      throttlingRecommendation = throttlePolicy.recommend(quotaRecommendations);
      if (throttlingRecommendation.shouldThrottle()) {
        quotaMetrics.quotaExceededCount.inc();
      }
    } finally {
      timer.stop();
    }
    return throttlingRecommendation;
  }

  @Override
  public ThrottlingRecommendation charge(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) {
    if (!quotaConfig.requestThrottlingEnabled || requestQuotaEnforcers.isEmpty()) {
      return null;
    }
    ThrottlingRecommendation throttlingRecommendation = null;
    Timer.Context timer = quotaMetrics.quotaChargeTime.time();
    try {
      throttlingRecommendation = throttlePolicy.recommend(requestQuotaEnforcers.stream()
          .map(quotaEnforcer -> quotaEnforcer.chargeAndRecommend(restRequest, blobInfo, requestCostMap))
          .filter(quotaRecommendation -> quotaRecommendation != null)
          .collect(Collectors.toList()));
    } finally {
      timer.stop();
    }
    return throttlingRecommendation;
  }

  @Override
  public void shutdown() {
    for (QuotaEnforcer quotaEnforcer : requestQuotaEnforcers) {
      quotaEnforcer.shutdown();
    }
  }

  @Override
  public QuotaConfig getQuotaConfig() {
    return quotaConfig;
  }

  /**
   * Parse the json config for {@link QuotaEnforcer} and {@link QuotaSource} factory pair and return them in a {@link Map}.
   * @param quotaEnforcerSourceJson json config string.
   * @return Map of {@link QuotaEnforcer} and {@link QuotaSource} factory pair.
   */
  private Map<String, String> parseQuotaEnforcerAndSourceInfo(String quotaEnforcerSourceJson) {
    if (quotaEnforcerSourceJson.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> quotaEnforcerSourceMap = new HashMap<>();
    JSONObject root = new JSONObject(quotaEnforcerSourceJson);
    JSONArray all = root.getJSONArray(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR);
    for (int i = 0; i < all.length(); i++) {
      JSONObject entry = all.getJSONObject(i);
      String enforcer = entry.getString(QuotaConfig.ENFORCER_STR);
      String source = entry.getString(QuotaConfig.SOURCE_STR);
      quotaEnforcerSourceMap.put(enforcer, source);
    }
    return quotaEnforcerSourceMap;
  }

  /**
   * Create the specified {@link QuotaSource} objects.
   * @param quotaSourceFactoryClasses {@link Collection} of {@link QuotaSourceFactory} classes.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @return Map of {@link QuotaSourceFactory} class names to {@link QuotaSource} objects.
   * @throws ReflectiveOperationException
   */
  private Map<String, QuotaSource> buildQuotaSources(Collection<String> quotaSourceFactoryClasses,
      QuotaConfig quotaConfig, AccountService accountService) throws ReflectiveOperationException {
    Map<String, QuotaSource> quotaSourceObjectMap = new HashMap<>();
    for (String quotaSourceFactoryClass : quotaSourceFactoryClasses) {
      if (!quotaSourceObjectMap.containsKey(quotaSourceFactoryClass)) {
        quotaSourceObjectMap.put(quotaSourceFactoryClass,
            ((QuotaSourceFactory) Utils.getObj(quotaSourceFactoryClass, quotaConfig, accountService)).getQuotaSource());
      }
    }
    return quotaSourceObjectMap;
  }
}
