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
package com.github.ambry.quota.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A JSON string implementation of {@link StorageQuotaSource} interface. The entire storage quota is encoded as json
 * string in {@link StorageQuotaConfig#containerStorageQuotaInJson}.
 */
public class JSONStringStorageQuotaSource implements StorageQuotaSource {
  private static final Logger logger = LoggerFactory.getLogger(JSONStringStorageQuotaSource.class);

  private final Map<String, Map<String, Long>> containerStorageQuota;
  private final StorageQuotaConfig config;

  /**
   * Constructor to create a {@link JSONStringStorageQuotaSource}.
   * @param config The {@link StorageQuotaSource}.
   * @throws IOException
   */
  public JSONStringStorageQuotaSource(StorageQuotaConfig config) throws IOException {
    this.config = config;
    Map<String, Map<String, Long>> quota = Collections.EMPTY_MAP;
    if (config.containerStorageQuotaInJson != null && !config.containerStorageQuotaInJson.trim().isEmpty()) {
      ObjectMapper mapper = new ObjectMapper();
      quota = mapper.readValue(config.containerStorageQuotaInJson, new TypeReference<Map<String, Map<String, Long>>>() {
      });
    }
    this.containerStorageQuota = quota;
  }

  @Override
  public Map<String, Map<String, Long>> getContainerQuota() {
    return Collections.unmodifiableMap(containerStorageQuota);
  }

  @Override
  public void registerListener(Listener listener) {
    // no-op
    return;
  }

  @Override
  public void addStorageUsageCallback(StorageUsageCallback callback) {
    // no-op
  }

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) {
    if (quotaName != QuotaName.STORAGE_IN_GB) {
      return null;
    }
    if (quotaResource.getQuotaResourceType() != QuotaResource.QuotaResourceType.CONTAINER) {
      return null;
    }
    // We know this is accountId_containerId
    String[] accountContainer = quotaResource.getResourceId().split(QuotaResource.DELIM);
    String accountId = accountContainer[0];
    String containerId = accountContainer[1];
    if (containerStorageQuota.containsKey(accountId)) {
      if (containerStorageQuota.get(accountId).containsKey(containerId)) {
        long quotaValue = containerStorageQuota.get(accountId).get(containerId);
        return new Quota(quotaName, quotaValue, quotaResource);
      }
    }
    return null;
  }

  @Override
  public void updateNewQuotaResources(Collection<QuotaResource> quotaResources) {
    // no-op
  }

  @Override
  public void shutdown() {
    // no-op
  }
}
