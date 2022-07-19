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
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.json.JSONObject;
import org.junit.Assert;
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
  public void testInitEmptyStorageUsage() {
    JSONStringStorageQuotaSource quotaSource =
        new JSONStringStorageQuotaSource(new HashMap<>(), new InMemAccountService(false, false));
    StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, quotaSource, (StorageUsageRefresher) null);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);
    assertEquals(Collections.EMPTY_MAP, enforcer.getStorageUsages());
  }

  /**
   * Test to initialize the storage usage with non-empty map.
   */
  @Test
  public void testInitStorageUsage() throws Exception {
    Map<String, Map<String, Long>> containerUsage = TestUtils.makeStorageMap(10, 10, 1000, 100);
    InMemAccountService accountService = new InMemAccountService(false, false);
    Map<QuotaResource, Long> expectedStorageUsages = new HashMap<>();
    // Account and container id's base is 1, not 0
    for (int i = 1; i <= 10; i++) {
      QuotaResourceType resourceType =
          i <= containerUsage.size() / 2 ? QuotaResourceType.CONTAINER : QuotaResourceType.ACCOUNT;
      AccountBuilder accountBuilder =
          new AccountBuilder((short) i, String.valueOf(i), Account.AccountStatus.ACTIVE, resourceType);
      for (int j = 1; j <= 10; j++) {
        accountBuilder.addOrUpdateContainer(
            new ContainerBuilder((short) j, String.valueOf(j), Container.ContainerStatus.ACTIVE, "",
                (short) i).build());
      }
      accountService.updateAccounts(Collections.singleton(accountBuilder.build()));
      if (resourceType == QuotaResourceType.ACCOUNT) {
        expectedStorageUsages.put(QuotaResource.fromAccountId((short) i),
            containerUsage.get(String.valueOf(i)).values().stream().mapToLong(Long::longValue).sum());
      } else {
        for (Map.Entry<String, Long> containerEntry : containerUsage.get(String.valueOf(i)).entrySet()) {
          expectedStorageUsages.put(QuotaResource.fromContainerId((short) i, Short.valueOf(containerEntry.getKey())),
              containerEntry.getValue());
        }
      }
    }

    StorageQuotaEnforcer enforcer =
        new StorageQuotaEnforcer(config, new JSONStringStorageQuotaSource(new HashMap<>(), accountService),
            (StorageUsageRefresher) null);
    enforcer.initStorageUsage(containerUsage);
    assertEquals(expectedStorageUsages, enforcer.getStorageUsages());
  }

  /**
   * Test when storage usage updates.
   */
  @Test
  public void testStorageUsageRefresherListener() throws Exception {
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> containerUsage = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    InMemAccountService accountService = new InMemAccountService(false, false);
    Map<QuotaResource, Long> expectedStorageUsages = new HashMap<>();
    // Account and container id's base is 1, not 0
    for (int i = 1; i <= initNumAccounts; i++) {
      QuotaResourceType resourceType =
          i <= containerUsage.size() / 2 ? QuotaResourceType.CONTAINER : QuotaResourceType.ACCOUNT;
      AccountBuilder accountBuilder =
          new AccountBuilder((short) i, String.valueOf(i), Account.AccountStatus.ACTIVE, resourceType);
      for (int j = 1; j <= 10; j++) {
        accountBuilder.addOrUpdateContainer(
            new ContainerBuilder((short) j, String.valueOf(j), Container.ContainerStatus.ACTIVE, "",
                (short) i).build());
      }
      accountService.updateAccounts(Collections.singleton(accountBuilder.build()));
      if (resourceType == QuotaResourceType.ACCOUNT) {
        expectedStorageUsages.put(QuotaResource.fromAccountId((short) i),
            containerUsage.get(String.valueOf(i)).values().stream().mapToLong(Long::longValue).sum());
      } else {
        for (Map.Entry<String, Long> containerEntry : containerUsage.get(String.valueOf(i)).entrySet()) {
          expectedStorageUsages.put(QuotaResource.fromContainerId((short) i, Short.valueOf(containerEntry.getKey())),
              containerEntry.getValue());
        }
      }
    }
    StorageQuotaEnforcer enforcer =
        new StorageQuotaEnforcer(config, new JSONStringStorageQuotaSource(new HashMap<>(), accountService),
            (StorageUsageRefresher) null);
    enforcer.initStorageUsage(containerUsage);

    StorageUsageRefresher.Listener listener = enforcer.getUsageRefresherListener();
    int numUpdates = 10;
    for (int i = 1; i <= numUpdates; i++) {
      if (i % 2 == 0) {
        // add new storage usage
        Map<String, Map<String, Long>> additionalUsage = TestUtils.makeStorageMap(1, 10, 10000, 1000);
        short accountId = (short) (initNumAccounts + i);
        containerUsage.put(String.valueOf(accountId), additionalUsage.remove("1"));
        QuotaResourceType resourceType = i % 4 == 0 ? QuotaResourceType.CONTAINER : QuotaResourceType.ACCOUNT;
        AccountBuilder accountBuilder =
            new AccountBuilder(accountId, String.valueOf(accountId), Account.AccountStatus.ACTIVE, resourceType);
        for (int j = 1; j <= 10; j++) {
          accountBuilder.addOrUpdateContainer(
              new ContainerBuilder((short) j, String.valueOf(j), Container.ContainerStatus.ACTIVE, "",
                  (short) accountId).build());
        }
        accountService.updateAccounts(Collections.singleton(accountBuilder.build()));
        if (resourceType == QuotaResourceType.ACCOUNT) {
          expectedStorageUsages.put(QuotaResource.fromAccountId(accountId),
              containerUsage.get(String.valueOf(accountId)).values().stream().mapToLong(Long::longValue).sum());
        } else {
          for (Map.Entry<String, Long> containerEntry : containerUsage.get(String.valueOf(accountId)).entrySet()) {
            expectedStorageUsages.put(QuotaResource.fromContainerId(accountId, Short.valueOf(containerEntry.getKey())),
                containerEntry.getValue());
          }
        }
      } else {
        // change existing storage usage
        Random random = new Random();
        int accountId = random.nextInt(initNumAccounts) + 1;
        int containerId = random.nextInt(10) + 1;
        long newValue = random.nextLong();
        long oldValue = containerUsage.get(String.valueOf(accountId)).get(String.valueOf(containerId));
        containerUsage.get(String.valueOf(accountId)).put(String.valueOf(containerId), newValue);
        if (accountService.getAccountById((short) accountId).getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
          QuotaResource resource = QuotaResource.fromAccountId((short) accountId);
          expectedStorageUsages.put(resource, expectedStorageUsages.get(resource) - oldValue + newValue);
        } else {
          expectedStorageUsages.put(QuotaResource.fromContainerId((short) accountId, (short) containerId), newValue);
        }
      }
      listener.onNewContainerStorageUsage(containerUsage);
      assertEquals(expectedStorageUsages, enforcer.getStorageUsages());
    }
  }

  /**
   * Test {@link StorageQuotaEnforcer#getQuotaAndUsage} and {@link StorageQuotaEnforcer#charge} methods.
   * @throws Exception
   */
  @Test
  public void testGetQuotaAndUsageAndCharge() throws Exception {
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> containerUsage = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    InMemAccountService accountService = new InMemAccountService(false, false);
    Map<String, Long> storageQuota = new HashMap<>();
    // Account and container id's base is 1, not 0
    for (int i = 1; i <= initNumAccounts; i++) {
      QuotaResourceType resourceType =
          i <= containerUsage.size() / 2 ? QuotaResourceType.CONTAINER : QuotaResourceType.ACCOUNT;
      AccountBuilder accountBuilder =
          new AccountBuilder((short) i, String.valueOf(i), Account.AccountStatus.ACTIVE, resourceType);
      for (int j = 1; j <= 10; j++) {
        accountBuilder.addOrUpdateContainer(
            new ContainerBuilder((short) j, String.valueOf(j), Container.ContainerStatus.ACTIVE, "",
                (short) i).build());
      }
      accountService.updateAccounts(Collections.singleton(accountBuilder.build()));
      if (resourceType == QuotaResourceType.ACCOUNT) {
        storageQuota.put(QuotaResource.fromAccount(accountService.getAccountById((short) i)).getResourceId(),
            containerUsage.get(String.valueOf(i)).values().stream().mapToLong(Long::longValue).sum());
      } else {
        accountService.getAccountById((short) i)
            .getAllContainers()
            .forEach(c -> storageQuota.put(QuotaResource.fromContainer(c).getResourceId(),
                containerUsage.get(String.valueOf(c.getParentAccountId())).get(String.valueOf(c.getId()))));
      }
    }
    JSONStringStorageQuotaSource quotaSource = new JSONStringStorageQuotaSource(storageQuota, accountService);
    StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, quotaSource, (StorageUsageRefresher) null);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);

    for (Map.Entry<String, Map<String, Long>> accountEntry : containerUsage.entrySet()) {
      short accountId = Short.valueOf(accountEntry.getKey());
      if (accountService.getAccountById(accountId).getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
        long quota = (long) (quotaSource.getQuota(QuotaResource.fromAccount(accountService.getAccountById(accountId)),
            QuotaName.STORAGE_IN_GB).getQuotaValue()) * BYTES_IN_GB;
        RestRequest restRequest = createRestRequest(accountService, accountId, (short) 1);
        Pair<Long, Long> quotaAndUsage = enforcer.getQuotaAndUsage(restRequest);
        assertEquals("Account id: " + accountEntry.getKey(), quota, quotaAndUsage.getFirst().longValue());
        assertEquals(0L, quotaAndUsage.getSecond().longValue());

        quotaAndUsage = enforcer.charge(restRequest, quota / 2);
        assertEquals(quota, quotaAndUsage.getFirst().longValue());
        assertEquals(quota / 2, quotaAndUsage.getSecond().longValue());

        quotaAndUsage = enforcer.charge(restRequest, quota);
        assertEquals(quota, quotaAndUsage.getFirst().longValue());
        assertEquals(quota / 2 + quota, quotaAndUsage.getSecond().longValue());
      } else {
        for (Map.Entry<String, Long> containerEntry : accountEntry.getValue().entrySet()) {
          short containerId = Short.valueOf(containerEntry.getKey());
          long quota = containerEntry.getValue() * BYTES_IN_GB;
          RestRequest restRequest = createRestRequest(accountService, accountId, containerId);
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
    }

    // Now create a restRequest that doesn't carry account and container
    RestRequest restRequest = createRestRequest();
    Pair<Long, Long> quotaAndUsage = enforcer.getQuotaAndUsage(restRequest);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());
    quotaAndUsage = enforcer.charge(restRequest, 100L);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());

    Account account = new AccountBuilder((short) 1000, String.valueOf(1000), Account.AccountStatus.ACTIVE,
        QuotaResourceType.CONTAINER).addOrUpdateContainer(
        new ContainerBuilder((short) 10000, String.valueOf(10000), Container.ContainerStatus.ACTIVE, "",
            (short) 1000).build()).build();
    accountService.updateAccounts(Collections.singleton(account));
    restRequest = createRestRequest(accountService, (short) 1000, (short) 10000);
    quotaAndUsage = enforcer.getQuotaAndUsage(restRequest);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());
    quotaAndUsage = enforcer.charge(restRequest, 100L);
    assertEquals(-1L, quotaAndUsage.getFirst().longValue());
    assertEquals(0L, quotaAndUsage.getSecond().longValue());
  }

  @Test
  public void testRecommendBasedOnQuotaAndUsage() {
    for (int i = 0; i < 4; i++) {
      boolean shouldThrottle = i % 2 == 0;
      boolean shouldRejectWithoutQuota = i / 2 == 0;
      // The pair of these two boolean should be
      // [(true, true), (false, true), (true, false), (false, false)]
      Properties properties = new Properties();
      properties.setProperty(StorageQuotaConfig.SHOULD_THROTTLE, String.valueOf(shouldThrottle));
      properties.setProperty(StorageQuotaConfig.SHOULD_REJECT_REQUEST_WITHOUT_QUOTA,
          String.valueOf(shouldRejectWithoutQuota));

      StorageQuotaConfig config = new StorageQuotaConfig(new VerifiableProperties(properties));

      JSONStringStorageQuotaSource quotaSource =
          new JSONStringStorageQuotaSource(new HashMap<>(), new InMemAccountService(false, false));
      StorageQuotaEnforcer enforcer = new StorageQuotaEnforcer(config, quotaSource, (StorageUsageRefresher) null);
      enforcer.initStorageUsage(Collections.EMPTY_MAP);

      // Test some cases
      // Case 1: no quota, only when both boolean values are true, we would reject
      // Case 2: usage is less than quota, shouldn't reject and the percentage should be right.
      // Case 3: usage is large then quota, shouldThrottle is true to reject

      // Case 1:
      QuotaRecommendation rec = enforcer.recommendBasedOnQuotaAndUsage(new Pair<>(-1L, 100L));
      if (shouldThrottle && shouldRejectWithoutQuota) {
        Assert.assertEquals(QuotaAction.REJECT, rec.getQuotaAction());
      } else {
        Assert.assertEquals(QuotaAction.ALLOW, rec.getQuotaAction());
      }
      Assert.assertEquals(0.0, rec.getQuotaUsagePercentage(), 0.0);

      // Case 2:
      rec = enforcer.recommendBasedOnQuotaAndUsage(new Pair<>(100L, 50L));
      Assert.assertEquals(QuotaAction.ALLOW, rec.getQuotaAction());
      Assert.assertEquals(50.0, rec.getQuotaUsagePercentage(), 0.0);

      // Case 3:
      rec = enforcer.recommendBasedOnQuotaAndUsage(new Pair<>(100L, 200L));
      if (shouldThrottle) {
        Assert.assertEquals(QuotaAction.REJECT, rec.getQuotaAction());
      } else {
        Assert.assertEquals(QuotaAction.ALLOW, rec.getQuotaAction());
      }
      Assert.assertEquals(100.0, rec.getQuotaUsagePercentage(), 0.0);
    }
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
   * @param accountService The {@link AccountService}.
   * @param accountId the account id.
   * @param containerId the container id.
   * @return a {@link MockRestRequest} with account and container headers.
   * @throws Exception
   */
  private RestRequest createRestRequest(AccountService accountService, short accountId, short containerId)
      throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, "/");
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, accountService.getAccountById(accountId));
    headers.put(RestUtils.InternalKeys.TARGET_CONTAINER_KEY,
        accountService.getAccountById(accountId).getContainerById(containerId));
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data, null);
  }
}
