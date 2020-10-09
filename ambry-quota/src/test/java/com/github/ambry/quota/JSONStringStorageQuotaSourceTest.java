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
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * The unit test class for {@link JSONStringStorageQuotaSource}.
 */
public class JSONStringStorageQuotaSourceTest {

  @Test
  public void testJSONStringStorageQuotaSource() throws IOException {
    // Trick to create a string literal without escape.
    String json = "{`10`: {`1`: 1000, `2`: 3000}, `20`: {`4`: 2000, `5`: 1000}}".replace("`", "\"");
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.CONTAINER_STORAGE_QUOTA_IN_JSON, json);
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    StorageQuotaConfig config = new StorageQuotaConfig(new VerifiableProperties(properties));

    JSONStringStorageQuotaSource source = new JSONStringStorageQuotaSource(config);
    Map<String, Map<String, Long>> containerQuota = source.getContainerQuota();
    assertEquals(containerQuota.size(), 2);
    assertTrue(containerQuota.containsKey("10"));
    assertTrue(containerQuota.containsKey("20"));
    Map<String, Long> quota = containerQuota.get("10");
    assertEquals(quota.size(), 2);
    assertEquals(quota.get("1").longValue(), 1000);
    assertEquals(quota.get("2").longValue(), 3000);
    quota = containerQuota.get("20");
    assertEquals(quota.size(), 2);
    assertEquals(quota.get("4").longValue(), 2000);
    assertEquals(quota.get("5").longValue(), 1000);
  }
}
