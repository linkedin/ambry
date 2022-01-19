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
package com.github.ambry.quota;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.capacityunit.JsonBackedCUQuotaSource;
import com.github.ambry.quota.capacityunit.JsonBackedCUQuotaSourceFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class JsonBackedCUQuotaSourceTest {
  private static final String DEFAULT_CU_QUOTA_IN_JSON =
      "{\n" + "    \"101\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
          + "            \"wcu\": 1024000000\n" + "        },\n" + "        \"2\": {\n"
          + "            \"rcu\": 258438456,\n" + "            \"wcu\": 258438456\n" + "        }\n" + "    },\n"
          + "    \"102\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
          + "            \"wcu\": 1024000000\n" + "        }\n" + "    },\n" + "    \"103\": {\n"
          + "        \"rcu\": 10737418240,\n" + "        \"wcu\": 10737418240\n" + "    }\n" + "}";
  private static final String DEFAULT_FRONTEND_CAPACITY_JSON =
      "{\n" + "      \"wcu\": 1024,\n" + "      \"rcu\": 1024\n" + "  }";
  private static JsonBackedCUQuotaSource quotaSource;
  private static Map<String, JsonBackedCUQuotaSource.MapOrQuota> testQuotas;

  /**
   * Create {@link Account} object with specified quota and accountId.
   * @param mapOrQuota quota of the account.
   * @param accountId id of the account.
   * @return Account object.
   */
  private static Account createAccountForQuota(JsonBackedCUQuotaSource.MapOrQuota mapOrQuota, String accountId) {
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

  /**
   * Create the {@link JsonBackedCUQuotaSource} for test.
   * @return JsonBackedCUQuotaSource object.
   * @throws IOException
   * @throws AccountServiceException
   */
  @Before
  public void setup() throws IOException, AccountServiceException {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON, DEFAULT_CU_QUOTA_IN_JSON);
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON, DEFAULT_FRONTEND_CAPACITY_JSON);
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    InMemAccountService accountService = new InMemAccountService(false, false);
    ObjectMapper objectMapper = new ObjectMapper();
    testQuotas = objectMapper.readValue(quotaConfig.resourceCUQuotaInJson,
        new TypeReference<Map<String, JsonBackedCUQuotaSource.MapOrQuota>>() {
        });
    for (String s : testQuotas.keySet()) {
      accountService.updateAccounts(Collections.singletonList(createAccountForQuota(testQuotas.get(s), s)));
    }
    JsonBackedCUQuotaSourceFactory
        jsonBackedCUQuotaSourceFactory = new JsonBackedCUQuotaSourceFactory(quotaConfig, accountService);
    quotaSource = (JsonBackedCUQuotaSource) jsonBackedCUQuotaSourceFactory.getQuotaSource();
  }

  @Test
  public void testCreation() throws QuotaException {
    Assert.assertEquals(4, quotaSource.getAllQuota().size());
    Assert.assertEquals(1024000000, (long) quotaSource.getQuota(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(1024000000, (long) quotaSource.getQuota(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(258438456, (long) quotaSource.getQuota(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(258438456, (long) quotaSource.getQuota(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(1024000000, (long) quotaSource.getQuota(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(1024000000, (long) quotaSource.getQuota(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    try {
        quotaSource.getQuota(new QuotaResource("101", QuotaResourceType.ACCOUNT), QuotaName.WRITE_CAPACITY_UNIT);
        Assert.fail("If quota is not present, it should throw an exception");
    } catch (QuotaException quotaException) {
    }
    try {
      quotaSource.getQuota(new QuotaResource("102", QuotaResourceType.ACCOUNT), QuotaName.WRITE_CAPACITY_UNIT);
      Assert.fail("If quota is not present, it should throw an exception");
    } catch (QuotaException quotaException) {
    }
    Assert.assertEquals(10737418240L,
        (long) quotaSource.getQuota(new QuotaResource("103", QuotaResourceType.ACCOUNT), QuotaName.READ_CAPACITY_UNIT)
            .getQuotaValue());
    Assert.assertEquals(10737418240L,
        (long) quotaSource.getQuota(new QuotaResource("103", QuotaResourceType.ACCOUNT), QuotaName.WRITE_CAPACITY_UNIT)
            .getQuotaValue());

    Assert.assertEquals(0, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);

    Assert.assertEquals(4, quotaSource.getAllQuotaUsage().size());
    Assert.assertEquals(0, (long) quotaSource.getUsage(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) quotaSource.getUsage(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) quotaSource.getUsage(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) quotaSource.getUsage(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) quotaSource.getUsage(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) quotaSource.getUsage(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT));
  }

  @Test
  public void testChargeForValidResource() throws Exception {
    Account account = createAccountForQuota(testQuotas.get("102"), "102");
    Container container = new ArrayList<>(account.getAllContainers()).get(0);
    quotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT, 102400000);
    Assert.assertEquals(10.0,
        quotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT),
        0.01);
    quotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT, 102400000);
    Assert.assertEquals(10.0,
        quotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(10.0, quotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT),
        0.01);
  }

  @Test
  public void testChargeForNonExistentResource() throws Exception {
    Account account =
        createAccountForQuota(new JsonBackedCUQuotaSource.MapOrQuota(new JsonBackedCUQuotaSource.CUQuota(10, 10)), "106");
    Container container = createContainer("1");
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.READ_CAPACITY_UNIT),
        0.01);
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.WRITE_CAPACITY_UNIT),
        0.01);

    quotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT, 10.0);
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.READ_CAPACITY_UNIT),
        0.01);
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.WRITE_CAPACITY_UNIT),
        0.01);

    quotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT, 10.0);
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.READ_CAPACITY_UNIT),
        0.01);
    Assert.assertEquals(0, quotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.WRITE_CAPACITY_UNIT),
        0.01);
  }

  @Test
  public void testIsQuotaExceedAllowed() throws Exception {
    Assert.assertEquals(0, quotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);

    quotaSource.chargeSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT, 819);
    Assert.assertEquals(79.98, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, quotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    quotaSource.chargeSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT, 819);
    Assert.assertEquals(79.98, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(79.98, quotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    quotaSource.chargeSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT, 1);
    Assert.assertEquals(80.07, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(79.98, quotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    quotaSource.chargeSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT, 1);
    Assert.assertEquals(80.07, quotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(80.07, quotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);
  }
}
