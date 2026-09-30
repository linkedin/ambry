/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.quota.QuotaResourceType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class AccountInfoMapTest {
  private final Map<String, MigrationConfig> migrationConfigs;

  public AccountInfoMapTest(Map<String, MigrationConfig> migrationConfigs) {
    this.migrationConfigs = migrationConfigs;
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{null}, {Collections.emptyMap()},
        {Collections.singletonMap("DC-1", new MigrationConfig())}});
  }

  @Test
  public void testRepeatedContainerReplayPreservesSerializableMetadata() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Account original = new AccountBuilder((short) 1, "account", Account.AccountStatus.ACTIVE)
        .snapshotVersion(7)
        .lastModifiedTime(1234)
        .aclInheritedByContainer(true)
        .quotaResourceType(QuotaResourceType.CONTAINER)
        .rampControl(new RampControl(true))
        .migrationConfig(new MigrationConfig())
        .migrationConfigs(migrationConfigs)
        .build();
    Container container =
        new ContainerBuilder((short) 1, "container", Container.ContainerStatus.ACTIVE, "description", original.getId())
            .build();
    Account expected = new AccountBuilder(original).addOrUpdateContainer(container).build();
    AccountInfoMap infoMap = new AccountInfoMap(Collections.singletonList(original));
    infoMap.addOrUpdateContainers(Collections.nCopies(100_000, container));
    Account replayed = infoMap.getAccountById(original.getId());
    assertSame("Both indexes should reference the rebuilt account", replayed,
        infoMap.getAccountByName(original.getName()));
    assertEquals("Replayed container should match", container, replayed.getContainerById(container.getId()));
    assertEquals("Migration configs should match", migrationConfigs, replayed.getMigrationConfigs());

    Collection<Account> expectedAccounts = Collections.singletonList(expected);
    byte[] response = AccountCollectionSerde.serializeAccountsInJson(infoMap.getAccounts(), false);
    assertEquals(mapper.readTree(AccountCollectionSerde.serializeAccountsInJson(expectedAccounts, false)),
        mapper.readTree(response));
    assertEquals(expectedAccounts,
        AccountCollectionSerde.accountsFromInputStreamInJson(new ByteArrayInputStream(response)));
    assertEquals(mapper.readTree(AccountCollectionSerde.serializeAccountsInJsonNoContainers(expected)),
        mapper.readTree(AccountCollectionSerde.serializeAccountsInJsonNoContainers(replayed)));
    assertEquals(expectedAccounts,
        BackupFileManager.deserializeAccounts(BackupFileManager.serializeAccounts(infoMap.getAccounts()).array()));
  }
}
