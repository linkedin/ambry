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
import com.github.ambry.quota.capacityunit.JsonCUQuotaSource;
import com.github.ambry.quota.capacityunit.JsonCUQuotaSourceFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;


public class JsonCUQuotaSourceTest {
  private static final String DEFAULT_CU_QUOTA_IN_JSON = "{\n" + "    \"101\": {\n" + "        \"1\": {\n"
      + "            \"rcu\": 1024000000,\n" + "            \"wcu\": 1024000000\n" + "        },\n"
      + "        \"2\": {\n" + "            \"rcu\": 258438456,\n" + "            \"wcu\": 258438456\n"
      + "        }\n" + "    },\n" + "    \"102\": {\n" + "        \"1\": {\n" + "            \"rcu\": 1024000000,\n"
      + "            \"wcu\": 1024000000\n" + "        }\n" + "    },\n" + "    \"103\": {\n"
      + "        \"rcu\": 10737418240,\n" + "        \"wcu\": 10737418240\n" + "    }\n" + "}";
  private static final String DEFAULT_FRONTEND_CAPACITY_JSON = "{\n" + "      \"wcu\": 1024,\n" + "      \"rcu\": 1024\n"
      + "  }";

  @Test
  public void testCreation() throws IOException, AccountServiceException {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON, DEFAULT_CU_QUOTA_IN_JSON);
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON, DEFAULT_FRONTEND_CAPACITY_JSON);
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    QuotaSource quotaSource = createTestJsonCUQuotaSource(quotaConfig);
    quotaSource.isReady();
  }

  private static QuotaSource createTestJsonCUQuotaSource(QuotaConfig quotaConfig) throws IOException,
                                                                                        AccountServiceException {
    InMemAccountService accountService = new InMemAccountService(false, false);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, JsonCUQuotaSource.MapOrQuota> tempQuotas =
        objectMapper.readValue(quotaConfig.resourceCUQuotaInJson, new TypeReference<Map<String, JsonCUQuotaSource.MapOrQuota>>() {
        });
    for(String s : tempQuotas.keySet()) {
      accountService.updateAccounts(Collections.singletonList(createAccountForQuota(tempQuotas.get(s), s)));
    }
    JsonCUQuotaSourceFactory jsonCUQuotaSourceFactory = new JsonCUQuotaSourceFactory(quotaConfig, accountService);
    return jsonCUQuotaSourceFactory.getQuotaSource();
  }

  private static Account createAccountForQuota(JsonCUQuotaSource.MapOrQuota mapOrQuota, String accountId) {
    AccountBuilder accountBuilder = new AccountBuilder();
    accountBuilder.id(Short.parseShort(accountId));
    accountBuilder.name(accountId);
    List<Container> containers = new ArrayList<>();
    if(!mapOrQuota.isQuota()) {
      for(String containerId : mapOrQuota.getContainerQuotas().keySet()) {
        ContainerBuilder containerBuilder = new ContainerBuilder();
        containerBuilder.setId(Short.parseShort(containerId));
        containerBuilder.setName(containerId);
        containerBuilder.setStatus(Container.ContainerStatus.ACTIVE);
        containers.add(containerBuilder.build());
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
}
