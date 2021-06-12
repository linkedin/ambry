/**
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
package com.github.ambry.quota.capacityunit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class JsonStringCapacityUnitQuotaSource implements QuotaSource {
  private final UnlimitedQuotaSource unlimitedQuotaSource = new UnlimitedQuotaSource();
  private final Map<String, Map<QuotaName, Long>> containerCapacityUnitQuota;
  private final long FRONTEND_RCU = 125L;
  private final long FRONTEND_WCU = 125L;
  private final Map<QuotaName, Long> frontendCapacityUnitQuota = Stream.of(
      new AbstractMap.SimpleImmutableEntry<>(QuotaName.READ_CAPACITY_UNIT, FRONTEND_RCU),
      new AbstractMap.SimpleImmutableEntry<>(QuotaName.READ_CAPACITY_UNIT, FRONTEND_WCU))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  public JsonStringCapacityUnitQuotaSource(QuotaConfig quotaConfig) throws IOException {
    Map<String, Map<String, Map<QuotaName, Long>>> quotaMap = Collections.EMPTY_MAP;
    if (quotaConfig.requestQuotaValuesInJson != null && !quotaConfig.requestQuotaValuesInJson.trim().isEmpty()) {
      ObjectMapper mapper = new ObjectMapper();
      quotaMap = mapper.readValue(quotaConfig.requestQuotaValuesInJson, new TypeReference<Map<String, Map<String, Map<QuotaName, Long>>>>() {
      });
    }
    containerCapacityUnitQuota = new HashMap<>();
    for(String accountId : quotaMap.keySet()) {
      for(String containerId : quotaMap.get(accountId).keySet()) {
        String accountContainerId = accountId + QuotaResource.DELIM + containerId;
        containerCapacityUnitQuota.put(accountContainerId, quotaMap.get(accountId).get(containerId));
      }
    }
  }

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) {
    if(containerCapacityUnitQuota.containsKey(quotaResource.getResourceId()) && containerCapacityUnitQuota.get(quotaResource.getResourceId()).containsKey(quotaName)) {
      return new Quota(quotaName, containerCapacityUnitQuota.get(quotaResource.getResourceId()).get(quotaName), quotaResource);
    } else {
      return unlimitedQuotaSource.getQuota(quotaResource, quotaName);
    }
  }

  @Override
  public void updateNewQuotaResources(Collection<QuotaResource> quotaResources) {
  }
}
