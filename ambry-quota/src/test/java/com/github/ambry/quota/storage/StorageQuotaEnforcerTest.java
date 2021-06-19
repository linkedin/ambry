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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link StorageQuotaEnforcer}.
 */
public class StorageQuotaEnforcerTest {
  private static final long BYTES_IN_GB = 1024 * 1024 * 1024;
  StorageQuotaConfig config = new StorageQuotaConfig(new VerifiableProperties(new Properties()));

  /**
   * Test to initialize the storage usage with empty map.
   */
  @Test
  public void testInitEmptyStorageUsage() throws Exception {
    StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, null, (StorageUsageRefresher) null);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);
    assertEquals(Collections.EMPTY_MAP, enforcer.getStorageUsage());
  }

  /**
   * Test to initialize the storage usage with non-empty map.
   */
  @Test
  public void testInitStorageUsage() throws Exception {
    StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, null, (StorageUsageRefresher) null);
    Map<String, Map<String, Long>> containerUsage = TestUtils.makeStorageMap(10, 10, 1000, 100);
    enforcer.initStorageUsage(containerUsage);
    assertEquals(containerUsage, enforcer.getStorageUsage());
  }

  /**
   * Test when storage usage updates.
   */
  @Test
  public void testStorageUsageRefresherListener() throws Exception {
    StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, null, (StorageUsageRefresher) null);
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedUsage = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    enforcer.initStorageUsage(expectedUsage);
    assertEquals(expectedUsage, enforcer.getStorageUsage());

    StorageUsageRefresher.Listener listener = enforcer.getUsageRefresherListener();
    int numUpdates = 10;
    for (int i = 1; i <= numUpdates; i++) {
      if (i % 2 == 0) {
        // add new storage usage
        Map<String, Map<String, Long>> additionalUsage = TestUtils.makeStorageMap(1, 10, 10000, 1000);
        expectedUsage.put(String.valueOf(initNumAccounts + i), additionalUsage.remove("1"));
      } else {
        // change existing storage usage
        Random random = new Random();
        int accountId = random.nextInt(initNumAccounts) + 1;
        int containerId = random.nextInt(10) + 1;
        long newValue = random.nextLong();
        expectedUsage.get(String.valueOf(accountId)).put(String.valueOf(containerId), newValue);
      }
      listener.onNewContainerStorageUsage(expectedUsage);
      assertEquals(expectedUsage, enforcer.getStorageUsage());
    }
  }

  /**
   * Test {@link StorageQuotaEnforcer#getQuotaAndUsage} and {@link StorageQuotaEnforcer#charge} methods.
   * @throws Exception
   */
  @Test
  public void testGetQuotaAndUsageAndCharge() throws Exception {
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedQuota = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    JSONStringStorageQuotaSource source = new JSONStringStorageQuotaSource(expectedQuota);
    StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, source, (StorageUsageRefresher) null);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);

    for (Map.Entry<String, Map<String, Long>> accountEntry : expectedQuota.entrySet()) {
      short accountId = Short.valueOf(accountEntry.getKey());
      for (Map.Entry<String, Long> containerEntry : accountEntry.getValue().entrySet()) {
        short containerId = Short.valueOf(containerEntry.getKey());
        long quota = containerEntry.getValue() * BYTES_IN_GB;
        RestRequest restRequest = createRestRequest(accountId, containerId);
        Pair<Long, Long> quotaAndUsage = enforcer.getQuotaAndUsage(restRequest);
        assertEquals(quota, quotaAndUsage.getFirst().longValue());
        assertEquals(0L, quotaAndUsage.getSecond().longValue());

        quotaAndUsage = enforcer.charge(restRequest, quota / 2);
        assertEquals(quota, quotaAndUsage.getFirst().longValue());
        assertEquals(quota / 2, quotaAndUsage.getSecond().longValue());

        quotaAndUsage = enforcer.charge(restRequest, quota);
        assertEquals(quota, quotaAndUsage.getFirst().longValue());
        assertEquals(quota / 2 + quota, quotaAndUsage.getSecond().longValue());
      }
    }

    // Now create a restRequest that doesn't carry account and container
    RestRequest restRequest = createRestRequest();
    Pair<Long, Long> quotaAndUsage = enforcer.getQuotaAndUsage(restRequest);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());
    quotaAndUsage = enforcer.charge(restRequest, 100L);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());

    restRequest = createRestRequest((short) 1000, (short) 10000);
    quotaAndUsage = enforcer.getQuotaAndUsage(restRequest);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());
    quotaAndUsage = enforcer.charge(restRequest, 100L);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());
  }

  /**
   * Create a {@link MockRestRequest} without any header.
   * @return a {@link MockRestRequest} without any header.
   * @throws Exception
   */
  private RestRequest createRestRequest() throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, "/");
    return new MockRestRequest(data, null);
  }

  /**
   * Create a {@link MockRestRequest} with account and container headers.
   * @param accountId the account id.
   * @param containerId the container id.
   * @return a {@link MockRestRequest} with account and container headers.
   * @throws Exception
   */
  private RestRequest createRestRequest(short accountId, short containerId) throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, "/");
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY,
        new AccountBuilder(accountId, "accountName", Account.AccountStatus.ACTIVE).build());
    headers.put(RestUtils.InternalKeys.TARGET_CONTAINER_KEY,
        new ContainerBuilder(containerId, "containerName", Container.ContainerStatus.ACTIVE, "", accountId).build());
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data, null);
  }
}
