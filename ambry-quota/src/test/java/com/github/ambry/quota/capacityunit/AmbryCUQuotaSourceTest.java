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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class AmbryCUQuotaSourceTest {
  private static final String DEFAULT_CU_QUOTA_IN_JSON =
      "{\n" + "    \"101\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
          + "            \"wcu\": 1024000000\n" + "        },\n" + "        \"2\": {\n"
          + "            \"rcu\": 258438456,\n" + "            \"wcu\": 258438456\n" + "        }\n" + "    },\n"
          + "    \"102\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
          + "            \"wcu\": 1024000000\n" + "        }\n" + "    },\n" + "    \"103\": {\n"
          + "        \"rcu\": 10737418240,\n" + "        \"wcu\": 10737418240\n" + "    }\n" + "}";
  private static final String DEFAULT_FRONTEND_CAPACITY_JSON =
      "{\n" + "      \"wcu\": 1024,\n" + "      \"rcu\": 1024\n" + "  }";
  private static AmbryCUQuotaSource ambryCUQuotaSource;
  private static Map<String, JsonCUQuotaDataProviderUtil.MapOrQuota> testQuotas;
  private static InMemAccountService inMemAccountService;
  private static QuotaConfig quotaConfig;
  private static RouterConfig routerConfig;

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

  /**
   * Create test setup by creating the {@link AmbryCUQuotaSource} object and updating account service.
   * @throws IOException
   * @throws AccountServiceException
   */
  @Before
  public void setup() throws IOException, AccountServiceException {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON, DEFAULT_CU_QUOTA_IN_JSON);
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON, DEFAULT_FRONTEND_CAPACITY_JSON);
    quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    inMemAccountService = new InMemAccountService(false, true);
    ObjectMapper objectMapper = new ObjectMapper();
    testQuotas = objectMapper.readValue(quotaConfig.resourceCUQuotaInJson,
        new TypeReference<Map<String, JsonCUQuotaDataProviderUtil.MapOrQuota>>() {
        });
    for (String s : testQuotas.keySet()) {
      inMemAccountService.updateAccounts(Collections.singletonList(createAccountForQuota(testQuotas.get(s), s)));
    }
    routerConfig = QuotaTestUtils.getDefaultRouterConfig();
    ambryCUQuotaSource = (AmbryCUQuotaSource) new AmbryCUQuotaSourceFactory(quotaConfig, inMemAccountService,
        new QuotaMetrics(new MetricRegistry()), routerConfig).getQuotaSource();
    ambryCUQuotaSource.init();
  }

  @After
  public void cleanup() throws InterruptedException {
    ambryCUQuotaSource.shutdown();
    inMemAccountService.close();
  }

  @Test
  public void testInit() throws Exception {
    QuotaSource quotaSource =
        new AmbryCUQuotaSourceFactory(new QuotaConfig(new VerifiableProperties(new Properties())), inMemAccountService,
            new QuotaMetrics(new MetricRegistry()), QuotaTestUtils.getDefaultRouterConfig()).getQuotaSource();
    Assert.assertFalse(quotaSource.isReady());
    quotaSource.init();
    Assert.assertTrue(quotaSource.isReady());
    quotaSource.shutdown();
    Assert.assertFalse(quotaSource.isReady());
  }

  @Test
  public void testCreation() throws QuotaException {
    Assert.assertEquals(4, ambryCUQuotaSource.getAllQuota().size());
    Assert.assertEquals(1024000000,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
            QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(1024000000,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
            QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(258438456,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
            QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(258438456,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
            QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(1024000000,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
            QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(1024000000,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
            QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    try {
      ambryCUQuotaSource.getQuota(new QuotaResource("101", QuotaResourceType.ACCOUNT), QuotaName.WRITE_CAPACITY_UNIT);
      Assert.fail("If quota is not present, it should throw an exception");
    } catch (QuotaException quotaException) {
    }
    try {
      ambryCUQuotaSource.getQuota(new QuotaResource("102", QuotaResourceType.ACCOUNT), QuotaName.WRITE_CAPACITY_UNIT);
      Assert.fail("If quota is not present, it should throw an exception");
    } catch (QuotaException quotaException) {
    }
    Assert.assertEquals(10737418240L,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("103", QuotaResourceType.ACCOUNT),
            QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
    Assert.assertEquals(10737418240L,
        (long) ambryCUQuotaSource.getQuota(new QuotaResource("103", QuotaResourceType.ACCOUNT),
            QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());

    Assert.assertEquals(0, ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);

    Assert.assertEquals(4, ambryCUQuotaSource.getAllQuotaUsage().size());
    Assert.assertEquals(0, (long) ambryCUQuotaSource.getUsage(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) ambryCUQuotaSource.getUsage(new QuotaResource("101_1", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) ambryCUQuotaSource.getUsage(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) ambryCUQuotaSource.getUsage(new QuotaResource("101_2", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) ambryCUQuotaSource.getUsage(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
        QuotaName.READ_CAPACITY_UNIT));
    Assert.assertEquals(0, (long) ambryCUQuotaSource.getUsage(new QuotaResource("102_1", QuotaResourceType.CONTAINER),
        QuotaName.WRITE_CAPACITY_UNIT));
  }

  @Test
  public void testChargeForValidResource() throws Exception {
    Account account = inMemAccountService.getAccountById((short) 102);
    Container container = new ArrayList<>(account.getAllContainers()).get(0);
    ambryCUQuotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT, 102400000);
    Assert.assertEquals(10.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT), 0.01);
    ambryCUQuotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT, 102400000);
    Assert.assertEquals(10.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(10.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT), 0.01);
  }

  @Test
  public void testChargeForNonExistentResource() throws Exception {
    Account account =
        createAccountForQuota(new JsonCUQuotaDataProviderUtil.MapOrQuota(new CapacityUnit(10, 10)), "106");
    Container container = createContainer("1");
    verifyFailsWithQuotaException(
        () -> ambryCUQuotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.READ_CAPACITY_UNIT),
        "Get usage for non existent resource");
    verifyFailsWithQuotaException(
        () -> ambryCUQuotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.WRITE_CAPACITY_UNIT),
        "Get usage for non existent resource");

    verifyFailsWithQuotaException(() -> {
      ambryCUQuotaSource.chargeUsage(QuotaResource.fromAccount(account), QuotaName.READ_CAPACITY_UNIT, 10.0);
      return null;
    }, "Charge usage for non existent resource");
    verifyFailsWithQuotaException(
        () -> ambryCUQuotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.READ_CAPACITY_UNIT),
        "Get usage for non existent resource");
    verifyFailsWithQuotaException(
        () -> ambryCUQuotaSource.getUsage(QuotaResource.fromAccount(account), QuotaName.WRITE_CAPACITY_UNIT),
        "Get usage for non existent resource");

    verifyFailsWithQuotaException(() -> {
      ambryCUQuotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT, 10.0);
      return null;
    }, "Charge usage for non existent resource");
    verifyFailsWithQuotaException(
        () -> ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT),
        "Get usage for non existent resource");
    verifyFailsWithQuotaException(
        () -> ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT),
        "Get usage for non existent resource");
  }

  @Test
  public void testChargeAndGetSystemResource() throws Exception {
    Assert.assertEquals(0, ambryCUQuotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);

    ambryCUQuotaSource.chargeSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT, 819);
    Assert.assertEquals(79.98 * routerConfig.routerGetRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0, ambryCUQuotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    ambryCUQuotaSource.chargeSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT, 819);
    Assert.assertEquals(79.98 * routerConfig.routerGetRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(79.98 * routerConfig.routerPutRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    ambryCUQuotaSource.chargeSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT, 1);
    Assert.assertEquals(80.07 * routerConfig.routerGetRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.1);
    Assert.assertEquals(79.98 * routerConfig.routerPutRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    ambryCUQuotaSource.chargeSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT, 1);
    Assert.assertEquals(80.07 * routerConfig.routerGetRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.READ_CAPACITY_UNIT), 0.1);
    Assert.assertEquals(80.07 * routerConfig.routerPutRequestParallelism,
        ambryCUQuotaSource.getSystemResourceUsage(QuotaName.WRITE_CAPACITY_UNIT), 0.1);
  }

  @Test
  public void testQuotaRefresh() throws QuotaException, InterruptedException {
    // 1. Charge quota for an account.
    Account account = inMemAccountService.getAccountById((short) 102);
    Container container = new ArrayList<>(account.getAllContainers()).get(0);
    ambryCUQuotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT, 102400000);
    ambryCUQuotaSource.chargeUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT, 102400000);

    // 2. Verify that usage reflects the charge.
    Assert.assertEquals(10.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(10.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT), 0.01);

    // 3. Wait for (slightly longer than) aggregation window time period.
    Thread.sleep((quotaConfig.cuQuotaAggregationWindowInSecs + 1) * 1000);

    // 4. Verify that the usage is reset automatically.
    Assert.assertEquals(0.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.READ_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(0.0,
        ambryCUQuotaSource.getUsage(QuotaResource.fromContainer(container), QuotaName.WRITE_CAPACITY_UNIT), 0.01);
    Assert.assertEquals(4, ambryCUQuotaSource.getAllQuotaUsage().size());
  }

  @Test
  public void testUpdateNewQuotaResources() throws AccountServiceException {
    // 1. Register account update consumer.
    inMemAccountService.addAccountUpdateConsumer(ambryCUQuotaSource::updateNewQuotaResources);

    // 2. Create an account with account quota, and another account with container quotas.
    Account accountWithAccountQuota =
        createAccountForQuota(new JsonCUQuotaDataProviderUtil.MapOrQuota(new CapacityUnit()), "106");
    Map<String, CapacityUnit> containerQuotas = new HashMap<String, CapacityUnit>() {{
      put("1", new CapacityUnit());
      put("2", new CapacityUnit());
    }};
    Account accountWithContainerQuota =
        createAccountForQuota(new JsonCUQuotaDataProviderUtil.MapOrQuota(containerQuotas), "107");

    // 3. Verify that the quota sources doesn't already contain the new accounts.
    Map<String, CapacityUnit> allQuotas = ambryCUQuotaSource.getAllQuota();
    Assert.assertFalse(allQuotas.containsKey("106"));
    Assert.assertFalse(allQuotas.containsKey("107_1"));
    Assert.assertFalse(allQuotas.containsKey("107_2"));

    // 4. Update the account service with the new accounts.
    inMemAccountService.updateAccounts(Arrays.asList(accountWithAccountQuota, accountWithContainerQuota));

    // 5. Ensure that the quota source contains the newly created accounts
    allQuotas = ambryCUQuotaSource.getAllQuota();
    Assert.assertTrue(allQuotas.containsKey("106"));
    Assert.assertTrue(allQuotas.containsKey("107_1"));
    Assert.assertTrue(allQuotas.containsKey("107_2"));
    Map<String, CapacityUnit> allUsages = ambryCUQuotaSource.getAllQuotaUsage();
    Assert.assertTrue(allUsages.containsKey("106"));
    Assert.assertTrue(allUsages.containsKey("107_1"));
    Assert.assertTrue(allUsages.containsKey("107_2"));
  }

  /**
   * Verify that the specified {@link Callable} fails with {@link QuotaException}.
   * @param callable {@link Callable} that is expected to fail.
   * @param operationDesc description of the operation performed by callable.
   * @param <T> the result of the specified {@link Callable}.
   * @throws Exception in case any exception other than {@link QuotaException} is thrown.
   */
  private <T> void verifyFailsWithQuotaException(Callable<T> callable, String operationDesc) throws Exception {
    try {
      callable.call();
      Assert.fail(operationDesc + " should have failed with QuotaException");
    } catch (QuotaException quotaException) {
    }
  }
}
