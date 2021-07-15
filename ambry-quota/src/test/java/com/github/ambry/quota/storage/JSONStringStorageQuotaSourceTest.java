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
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * The unit test class for {@link JSONStringStorageQuotaSource}.
 */
public class JSONStringStorageQuotaSourceTest {

  @Test
  public void testJSONStringStorageQuotaSource() throws Exception {
    // Trick to create a string literal without escape.
    String json =
        "{`10`: {`1`: 1000, `2`: 3000}, `20`: {`4`: 2000, `5`: 1000}, `30`: 4000, `40`: 5000, `50`: {`6`: 6000}}".replace(
            "`", "\"");
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.STORAGE_QUOTA_IN_JSON, json);
    StorageQuotaConfig config = new StorageQuotaConfig(new VerifiableProperties(properties));

    // Setting up accounts and account service
    InMemAccountService accountService = new InMemAccountService(false, false);
    Account account = new AccountBuilder((short) 10, "10", Account.AccountStatus.ACTIVE,
        QuotaResourceType.CONTAINER).addOrUpdateContainer(
        new ContainerBuilder((short) 1, "1", Container.ContainerStatus.ACTIVE, "", (short) 10).build())
        .addOrUpdateContainer(
            new ContainerBuilder((short) 2, "2", Container.ContainerStatus.ACTIVE, "", (short) 10).build())
        .build();
    accountService.updateAccounts(Collections.singleton(account));

    account = new AccountBuilder((short) 20, "20", Account.AccountStatus.ACTIVE,
        QuotaResourceType.CONTAINER).addOrUpdateContainer(
        new ContainerBuilder((short) 4, "4", Container.ContainerStatus.ACTIVE, "", (short) 20).build())
        .addOrUpdateContainer(
            new ContainerBuilder((short) 5, "5", Container.ContainerStatus.ACTIVE, "", (short) 20).build())
        .build();
    accountService.updateAccounts(Collections.singleton(account));

    account = new AccountBuilder((short) 30, "30", Account.AccountStatus.ACTIVE,
        QuotaResourceType.ACCOUNT).addOrUpdateContainer(
        new ContainerBuilder((short) 4, "4", Container.ContainerStatus.ACTIVE, "", (short) 30).build()).build();
    accountService.updateAccounts(Collections.singleton(account));

    account = new AccountBuilder((short) 40, "40", Account.AccountStatus.ACTIVE,
        QuotaResourceType.ACCOUNT).addOrUpdateContainer(
        new ContainerBuilder((short) 4, "4", Container.ContainerStatus.ACTIVE, "", (short) 40).build()).build();
    accountService.updateAccounts(Collections.singleton(account));

    account = new AccountBuilder((short) 50, "50", Account.AccountStatus.ACTIVE,
        QuotaResourceType.CONTAINER).addOrUpdateContainer(
        new ContainerBuilder((short) 6, "6", Container.ContainerStatus.ACTIVE, "", (short) 50).build()).build();
    accountService.updateAccounts(Collections.singleton(account));

    JSONStringStorageQuotaSource source = new JSONStringStorageQuotaSource(config, accountService);
    QuotaResourceType resourceType = QuotaResourceType.CONTAINER;
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
    quota = source.getQuota(new QuotaResource("30", QuotaResourceType.ACCOUNT), QuotaName.STORAGE_IN_GB);
    assertEquals(4000L, (long) quota.getQuotaValue());
    quota = source.getQuota(new QuotaResource("40", QuotaResourceType.ACCOUNT), QuotaName.STORAGE_IN_GB);
    assertEquals(5000L, (long) quota.getQuotaValue());
    quota = source.getQuota(new QuotaResource("50_6", resourceType), QuotaName.STORAGE_IN_GB);
    assertEquals(6000L, (long) quota.getQuotaValue());
  }
}
