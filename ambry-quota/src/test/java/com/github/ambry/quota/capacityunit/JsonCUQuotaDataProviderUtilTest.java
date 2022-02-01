/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.capacityunit.CapacityUnit;
import com.github.ambry.quota.capacityunit.JsonCUQuotaDataProviderUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


public class JsonCUQuotaDataProviderUtilTest {
  private static final String DEFAULT_CU_QUOTA_IN_JSON =
      "{\n" + "    \"101\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
          + "            \"wcu\": 1024000000\n" + "        },\n" + "        \"2\": {\n"
          + "            \"rcu\": 258438456,\n" + "            \"wcu\": 258438456\n" + "        }\n" + "    },\n"
          + "    \"102\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
          + "            \"wcu\": 1024000000\n" + "        }\n" + "    },\n" + "    \"103\": {\n"
          + "        \"rcu\": 10737418240,\n" + "        \"wcu\": 10737418240\n" + "    }\n" + "}";
  private static final String DEFAULT_FRONTEND_CAPACITY_JSON =
      "{\n" + "      \"wcu\": 1024,\n" + "      \"rcu\": 1024\n" + "  }";
  private static final String EMPTY_JSON = "{}";

  /**
   * Create {@link Account} object with specified quota and accountId.
   * @param mapOrQuota quota of the account.
   * @param accountId id of the account.
   * @return Account object.
   */
  private static Account createAccountForQuota(JsonCUQuotaDataProviderUtil.MapOrQuota mapOrQuota, String accountId) {
    AccountBuilder accountBuilder = new AccountBuilder();
    accountBuilder.id(Short.parseShort(accountId));
    accountBuilder.name(accountId);
    List<Container> containers = new ArrayList<>();
    if (!mapOrQuota.isQuota()) {
      for (String containerId : mapOrQuota.getContainerQuotas().keySet()) {
        containers.add(createContainer(containerId));
      }
    }
    accountBuilder.containers(containers);
    accountBuilder.status(Account.AccountStatus.ACTIVE);
    if (mapOrQuota.isQuota()) {
      accountBuilder.quotaResourceType(QuotaResourceType.ACCOUNT);
    } else {
      accountBuilder.quotaResourceType(QuotaResourceType.CONTAINER);
    }
    return accountBuilder.build();
  }

  /**
   * Create a {@link Container} with the specified containerId.
   * @param containerId id of the container.
   * @return Container object.
   */
  private static Container createContainer(String containerId) {
    ContainerBuilder containerBuilder = new ContainerBuilder();
    containerBuilder.setId(Short.parseShort(containerId));
    containerBuilder.setName(containerId);
    containerBuilder.setStatus(Container.ContainerStatus.ACTIVE);
    return containerBuilder.build();
  }

  @Test
  public void testGetCUQuotasFromJson() throws IOException, AccountServiceException {
    InMemAccountService accountService = new InMemAccountService(false, false);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, JsonCUQuotaDataProviderUtil.MapOrQuota> testQuotas = objectMapper.readValue(DEFAULT_CU_QUOTA_IN_JSON,
        new TypeReference<Map<String, JsonCUQuotaDataProviderUtil.MapOrQuota>>() {
        });
    for (String s : testQuotas.keySet()) {
      accountService.updateAccounts(Collections.singletonList(createAccountForQuota(testQuotas.get(s), s)));
    }
    Map<String, CapacityUnit> quotas =
        JsonCUQuotaDataProviderUtil.getCUQuotasFromJson(DEFAULT_CU_QUOTA_IN_JSON, accountService);

    Assert.assertEquals(4, quotas.size());
    Assert.assertEquals(1024000000, (long) quotas.get("101_1").getRcu());
    Assert.assertEquals(1024000000, (long) quotas.get("101_1").getWcu());
    Assert.assertEquals(258438456, (long) quotas.get("101_2").getRcu());
    Assert.assertEquals(258438456, (long) quotas.get("101_2").getWcu());
    Assert.assertEquals(1024000000, (long) quotas.get("102_1").getRcu());
    Assert.assertEquals(1024000000, (long) quotas.get("102_1").getWcu());
    Assert.assertEquals(10737418240L, (long) quotas.get("103").getRcu());
    Assert.assertEquals(10737418240L, (long) quotas.get("103").getWcu());
  }

  @Test
  public void testGetCUQuotasFromJsonForEmptyJsonString() throws IOException, AccountServiceException {
    InMemAccountService accountService = new InMemAccountService(false, false);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, JsonCUQuotaDataProviderUtil.MapOrQuota> testQuotas =
        objectMapper.readValue(EMPTY_JSON, new TypeReference<Map<String, JsonCUQuotaDataProviderUtil.MapOrQuota>>() {
        });
    for (String s : testQuotas.keySet()) {
      accountService.updateAccounts(Collections.singletonList(createAccountForQuota(testQuotas.get(s), s)));
    }
    Map<String, CapacityUnit> quotas = JsonCUQuotaDataProviderUtil.getCUQuotasFromJson(EMPTY_JSON, accountService);

    Assert.assertEquals(0, quotas.size());
  }

  @Test
  public void testGetFeCUCapacityFromJson() throws IOException {
    CapacityUnit feCapacityUnit = JsonCUQuotaDataProviderUtil.getFeCUCapacityFromJson(DEFAULT_FRONTEND_CAPACITY_JSON);
    Assert.assertEquals(1024, (long) feCapacityUnit.getRcu());
    Assert.assertEquals(1024, (long) feCapacityUnit.getWcu());
  }

  @Test
  public void testGetFeCUCapacityFromJsonForEmptyJson() throws IOException {
    CapacityUnit feCapacityUnit = JsonCUQuotaDataProviderUtil.getFeCUCapacityFromJson(EMPTY_JSON);
    Assert.assertEquals(0, (long) feCapacityUnit.getRcu());
    Assert.assertEquals(0, (long) feCapacityUnit.getWcu());
  }
}
