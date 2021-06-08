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

import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import java.io.IOException;
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
    StorageQuotaConfig config = new StorageQuotaConfig(new VerifiableProperties(properties));

    JSONStringStorageQuotaSource source = new JSONStringStorageQuotaSource(config);
    QuotaResource.QuotaResourceType resourceType = QuotaResource.QuotaResourceType.CONTAINER;
    Quota quota = source.getQuota(new QuotaResource("1000_1", resourceType), QuotaName.STORAGE_IN_GB);
    assertNull(quota);
    quota = source.getQuota(new QuotaResource("10_1", resourceType), QuotaName.STORAGE_IN_GB);
    assertEquals(1000L, (long) quota.getQuotaValue());
    quota = source.getQuota(new QuotaResource("10_2", resourceType), QuotaName.STORAGE_IN_GB);
    assertEquals(3000L, (long) quota.getQuotaValue());
    quota = source.getQuota(new QuotaResource("20_4", resourceType), QuotaName.STORAGE_IN_GB);
    assertEquals(2000L, (long) quota.getQuotaValue());
    quota = source.getQuota(new QuotaResource("20_5", resourceType), QuotaName.STORAGE_IN_GB);
    assertEquals(1000L, (long) quota.getQuotaValue());
  }
}
