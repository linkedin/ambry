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

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.Utils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * {@link QuotaManager} implementation to handle all the quota and quota enforcement for Ambry.
 */
public class AmbryQuotaManager implements QuotaManager {
  private final Set<QuotaEnforcer> requestQuotaEnforcers;
  private final ThrottlePolicy throttlePolicy;
  private final QuotaConfig quotaConfig;

  /**
   * Constructor for {@link AmbryQuotaManager}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param throttlePolicy {@link ThrottlePolicy} object that makes the overall recommendation.
   * @param accountService {@link AccountService} object to get all the accounts and container information.
   * @throws ReflectiveOperationException in case of any exception.
   */
  public AmbryQuotaManager(QuotaConfig quotaConfig, ThrottlePolicy throttlePolicy, AccountService accountService)
      throws ReflectiveOperationException {
    Map<String, String> quotaEnforcerSourceMap =
        parseQuotaEnforcerAndSourceInfo(quotaConfig.requestQuotaEnforcerSourcePairInfoJson);
    Map<String, QuotaSource> quotaSourceObjectMap =
        buildQuotaSources(quotaEnforcerSourceMap.values(), quotaConfig, accountService);
    requestQuotaEnforcers = new HashSet<>();
    for (String quotaEnforcerFactory : quotaEnforcerSourceMap.keySet()) {
      requestQuotaEnforcers.add(((QuotaEnforcerFactory) Utils.getObj(quotaEnforcerFactory, quotaConfig,
          quotaSourceObjectMap.get(quotaEnforcerSourceMap.get(quotaEnforcerFactory)))).getRequestQuotaEnforcer());
    }
    this.throttlePolicy = throttlePolicy;
    this.quotaConfig = quotaConfig;
  }

  @Override
  public void init() {
    for (QuotaEnforcer quotaEnforcer : requestQuotaEnforcers) {
      quotaEnforcer.init();
    }
  }

  @Override
  public ThrottlingRecommendation getThrottleRecommendation(RestRequest restRequest) {
    if (!quotaConfig.requestThrottlingEnabled || requestQuotaEnforcers.isEmpty()) {
      return null;
    }
    return throttlePolicy.recommend(requestQuotaEnforcers.stream()
        .map(quotaEnforcer -> quotaEnforcer.recommend(restRequest))
        .filter(quotaRecommendation -> quotaRecommendation != null)
        .collect(Collectors.toList()));
  }

  @Override
  public ThrottlingRecommendation charge(RestRequest restRequest, BlobInfo blobInfo) {
    if (!quotaConfig.requestThrottlingEnabled || requestQuotaEnforcers.isEmpty()) {
      return null;
    }
    return throttlePolicy.recommend(requestQuotaEnforcers.stream()
        .map(quotaEnforcer -> quotaEnforcer.chargeAndRecommend(restRequest, blobInfo))
        .collect(Collectors.toList()));
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
