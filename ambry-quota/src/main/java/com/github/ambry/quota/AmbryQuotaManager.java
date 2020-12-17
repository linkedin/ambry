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
package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.Utils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * {@link QuotaManager} implementation to handle all the quota and quota enforcement for Ambry.
 */
public class AmbryQuotaManager implements QuotaManager {
  private final Set<HostQuotaEnforcer> hostQuotaEnforcers;
  private final Set<RequestQuotaEnforcer> requestQuotaEnforcers;

  /**
   * Constructor for {@link AmbryQuotaManager}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @throws ReflectiveOperationException in case of any exception.
   */
  public AmbryQuotaManager(QuotaConfig quotaConfig, List<HostQuotaEnforcer> addedHostQuotaEnforcers,
      List<RequestQuotaEnforcer> addedRequestQuotaEnforcers) throws ReflectiveOperationException {
    Map<String, String> quotaEnforcerSourceMap =
        parseQuotaEnforcerAndSourceInfo(quotaConfig.requestQuotaEnforcerSourcePairInfoJson);
    Map<String, QuotaSource> quotaSourceObjectMap = buildQuotaSources(quotaEnforcerSourceMap.values(), quotaConfig);
    requestQuotaEnforcers = new HashSet<>(addedRequestQuotaEnforcers);
    hostQuotaEnforcers = new HashSet<>(addedHostQuotaEnforcers);
    for (String quotaEnforcerFactory : quotaEnforcerSourceMap.keySet()) {
      requestQuotaEnforcers.add(((RequestQuotaEnforcerFactory) Utils.getObj(quotaEnforcerFactory, quotaConfig,
          quotaSourceObjectMap.get(quotaEnforcerSourceMap.get(quotaEnforcerFactory)))).getRequestQuotaEnforcer());
    }
    for (String quotaEnforcerFactory : quotaConfig.hostQuotaEnforcerFactories) {
      hostQuotaEnforcers.add(
          ((HostQuotaEnforcerFactory) Utils.getObj(quotaEnforcerFactory, quotaConfig)).getHostQuotaEnforcer());
    }
  }

  @Override
  public void init() {
    for (HostQuotaEnforcer quotaEnforcer : hostQuotaEnforcers) {
      quotaEnforcer.init();
    }
    for (RequestQuotaEnforcer quotaEnforcer : requestQuotaEnforcers) {
      quotaEnforcer.init();
    }
  }

  @Override
  public boolean shouldThrottleOnHost(List<EnforcementRecommendation> enforcementRecommendations) {
    if (hostQuotaEnforcers.isEmpty()) {
      return false;
    }
    boolean shouldThrottle = true;
    Iterator<HostQuotaEnforcer> quotaEnforcerIterator = hostQuotaEnforcers.iterator();
    while (quotaEnforcerIterator.hasNext()) {
      EnforcementRecommendation enforcementRecommendation = quotaEnforcerIterator.next().recommend();
      shouldThrottle = shouldThrottle && enforcementRecommendation.shouldThrottle();
      enforcementRecommendations.add(enforcementRecommendation);
    }
    return shouldThrottle;
  }

  @Override
  public boolean shouldThrottleOnRequest(RestRequest restRequest,
      List<EnforcementRecommendation> enforcementRecommendations) {
    if (requestQuotaEnforcers.isEmpty()) {
      return false;
    }
    boolean shouldThrottle = true;
    Iterator<RequestQuotaEnforcer> quotaEnforcerIterator = requestQuotaEnforcers.iterator();
    while (quotaEnforcerIterator.hasNext()) {
      EnforcementRecommendation enforcementRecommendation = quotaEnforcerIterator.next().recommend(restRequest);
      shouldThrottle = shouldThrottle && enforcementRecommendation.shouldThrottle();
      enforcementRecommendations.add(enforcementRecommendation);
    }
    return shouldThrottle;
  }

  @Override
  public boolean shouldThrottleOnRequestAndCharge(RestRequest restRequest, BlobInfo blobInfo,
      List<EnforcementRecommendation> enforcementRecommendations) {
    if (requestQuotaEnforcers.isEmpty()) {
      return false;
    }
    boolean shouldThrottle = true;
    Iterator<RequestQuotaEnforcer> quotaEnforcerIterator = requestQuotaEnforcers.iterator();
    while (quotaEnforcerIterator.hasNext()) {
      EnforcementRecommendation enforcementRecommendation =
          quotaEnforcerIterator.next().chargeAndRecommend(restRequest, blobInfo);
      shouldThrottle = shouldThrottle && enforcementRecommendation.shouldThrottle();
      enforcementRecommendations.add(enforcementRecommendation);
    }
    return shouldThrottle;
  }

  @Override
  public void shutdown() {
    for (RequestQuotaEnforcer quotaEnforcer : requestQuotaEnforcers) {
      quotaEnforcer.shutdown();
    }
    for (HostQuotaEnforcer quotaEnforcer : hostQuotaEnforcers) {
      quotaEnforcer.shutdown();
    }
  }

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

  private Map<String, QuotaSource> buildQuotaSources(Collection<String> quotaSourceClasses, QuotaConfig quotaConfig)
      throws ReflectiveOperationException {
    Map<String, QuotaSource> quotaSourceObjectMap = new HashMap<>();
    for (String quotaSourceClass : quotaSourceClasses) {
      if (!quotaSourceObjectMap.containsKey(quotaSourceClass)) {
        quotaSourceObjectMap.put(quotaSourceClass,
            ((QuotaSourceFactory) Utils.getObj(quotaSourceClass, quotaConfig)).getQuotaSource());
      }
    }
    return quotaSourceObjectMap;
  }
}
