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

import com.github.ambry.config.StorageQuotaConfig;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
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
}
