/*
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.InMemoryAccountConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class InMemoryAccountServiceTest {
  private InMemoryAccountServiceFactory accountServiceFactory;

  @Before
  public void init() {
    Properties cfg = new Properties();
    cfg.setProperty(InMemoryAccountConfig.FILE_PATH, "src/test/accounts.json");
    VerifiableProperties verifiableProperties = new VerifiableProperties(cfg);
    accountServiceFactory = new InMemoryAccountServiceFactory(verifiableProperties, new MetricRegistry());
  }

  @Test
  public void getAccountFromJSON() {
    AccountService accountService = accountServiceFactory.getAccountService();
    Account account = accountService.getAccountById((short) 1234);
    assertEquals("Did not get the expected account", "s3tests", account.getName());
    assertEquals(2, accountService.getAllAccounts().size());
  }

  @Test
  public void invalidJSON() {
    Properties cfg = new Properties();
    cfg.setProperty(InMemoryAccountConfig.FILE_PATH, "nowhere.json");
    VerifiableProperties verifiableProperties = new VerifiableProperties(cfg);
    InMemoryAccountServiceFactory factoryWithBadFile =
        new InMemoryAccountServiceFactory(verifiableProperties, new MetricRegistry());
    AccountService accountService = factoryWithBadFile.getAccountService();

    assertEquals(1, accountService.getAllAccounts().size());
  }

  @Test
  public void getUnknownAccount() {
    AccountService accountService = accountServiceFactory.getAccountService();
    Account account = accountService.getAccountById((short) Account.UNKNOWN_ACCOUNT_ID);
    assertEquals("Did not get the expected account", Account.UNKNOWN_ACCOUNT_NAME, account.getName());
  }

  @Test
  public void getAccountNotFound() {
    AccountService accountService = accountServiceFactory.getAccountService();
    Account account = accountService.getAccountById((short) 4321);
    assertNull("Found an unexpected account", account);
  }

  @Test
  public void addAccount() throws Exception {
    ContainerBuilder containerA =
        new ContainerBuilder((short) 1, "containerA", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ContainerBuilder containerB =
        new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 2345, "test", Account.AccountStatus.ACTIVE, true, 0, containers,
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
    assertEquals(accountsToUpdate.get(0), accountService.getAccountById((short) 2345));
    assertEquals(3, accountService.getAllAccounts().size());
  }

  @Test(expected = AccountServiceException.class)
  public void addAccountConflictDuplicateInRequest() throws AccountServiceException {
    ContainerBuilder containerA =
        new ContainerBuilder((short) 1, "containerA", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ContainerBuilder containerB =
        new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 2345, "s3tests", Account.AccountStatus.ACTIVE, true, 0, containers,
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));
    accountsToUpdate.add(new Account((short) 2345, "s3tests", Account.AccountStatus.ACTIVE, true, 0, containers,
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
  }

  @Test(expected = AccountServiceException.class)
  public void addAccountConflictAlreadyExists() throws AccountServiceException {
    ContainerBuilder containerA =
        new ContainerBuilder((short) 1, "containerA", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ContainerBuilder containerB =
        new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 2345, "s3tests", Account.AccountStatus.ACTIVE, true, 0, containers,
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
  }

  @Test(expected = AccountServiceException.class)
  public void addAccountWithTooManyContainers() throws AccountServiceException {
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 3000, "toomany", Account.AccountStatus.ACTIVE, true, 0,
        Collections.emptyList(),
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
    ArrayList<Container> containers = new ArrayList<>();
    // add Short.MAX_VALUE + 1 containers
    for (int i = 0; i <= Short.MAX_VALUE; i++) {
      containers.add(new ContainerBuilder((short) i, "container" + i, Container.ContainerStatus.ACTIVE, "test", (short) 3000).build());
    }
    accountService.updateContainers(accountsToUpdate.get(0).getName(), containers);
  }

  @Test(expected = AccountServiceException.class)
  public void addAccountWithIncrementalContainerAdd() throws AccountServiceException {
    ArrayList<Container> containers = new ArrayList<>();
    // add Short.MAX_VALUE -1 containers
    for (int i = 0; i < Short.MAX_VALUE; i++) {
      containers.add(new ContainerBuilder((short) i, "container" + i, Container.ContainerStatus.ACTIVE, "test", (short) 3000).build());
    }
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 3000, "toomany", Account.AccountStatus.ACTIVE, true, 0, containers,
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
    // add two new containers to trigger overflow
    containers = new ArrayList<>();
    containers.add(new ContainerBuilder((short)-1, "container-large-id-1", Container.ContainerStatus.ACTIVE, "test", (short) 3000).build());
    containers.add(new ContainerBuilder((short)-1, "container-large-id-2", Container.ContainerStatus.ACTIVE, "test", (short) 3000).build());
    accountService.updateContainers(accountsToUpdate.get(0).getName(), containers);
  }


  @Test(expected = AccountServiceException.class)
  public void addContainerConflictAlreadyExists() throws AccountServiceException {
    ContainerBuilder containerA =
        new ContainerBuilder((short) 1, "s3testbucket", Container.ContainerStatus.ACTIVE, "test", (short) 1234);
    ContainerBuilder containerB =
        new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 1234);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 1234, "s3tests", Account.AccountStatus.ACTIVE, true, 0, containers,
        Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
  }

  @Test
  public void updateAccountMigrationConfigs() throws Exception {
    // Create an account with migrationConfigs
    Map<String, MigrationConfig> migrationConfigs = new HashMap<>();
    migrationConfigs.put("DC-1", new MigrationConfig());
    migrationConfigs.put("DC-2",
        new MigrationConfig(true, new MigrationConfig.WriteRamp(), new MigrationConfig.ReadRamp(),
            new MigrationConfig.ListRamp()));
    Account account =
        new AccountBuilder((short) 5000, "migrationTest", Account.AccountStatus.ACTIVE).migrationConfigs(
            migrationConfigs).build();

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(Collections.singletonList(account));
    Account stored = accountService.getAccountById((short) 5000);
    assertEquals("migrationConfigs should be set after initial create", migrationConfigs,
        stored.getMigrationConfigs());

    // Update the account with different migrationConfigs
    Map<String, MigrationConfig> updatedConfigs = new HashMap<>();
    updatedConfigs.put("DC-3",
        new MigrationConfig(true, new MigrationConfig.WriteRamp(false, 75.0, 0.0, 0.0, false),
            new MigrationConfig.ReadRamp(), new MigrationConfig.ListRamp()));
    Account updatedAccount = new AccountBuilder(stored).migrationConfigs(updatedConfigs).build();
    accountService.updateAccounts(Collections.singletonList(updatedAccount));

    Account afterUpdate = accountService.getAccountById((short) 5000);
    assertEquals("migrationConfigs should reflect the update", updatedConfigs, afterUpdate.getMigrationConfigs());
    assertFalse("Old DC-1 key should no longer be present", afterUpdate.getMigrationConfigs().containsKey("DC-1"));
    assertTrue("New DC-3 key should be present", afterUpdate.getMigrationConfigs().containsKey("DC-3"));
  }

  @Test
  public void updateAccountClearMigrationConfigs() throws Exception {
    // Create an account with migrationConfigs
    Map<String, MigrationConfig> migrationConfigs = new HashMap<>();
    migrationConfigs.put("DC-1", new MigrationConfig());
    Account account =
        new AccountBuilder((short) 5001, "migrationClearTest", Account.AccountStatus.ACTIVE).migrationConfigs(
            migrationConfigs).build();

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(Collections.singletonList(account));
    assertNotNull("migrationConfigs should be set",
        accountService.getAccountById((short) 5001).getMigrationConfigs());

    // Update to clear migrationConfigs
    Account cleared =
        new AccountBuilder(accountService.getAccountById((short) 5001)).migrationConfigs(null).build();
    accountService.updateAccounts(Collections.singletonList(cleared));
    assertNull("migrationConfigs should be null after clearing",
        accountService.getAccountById((short) 5001).getMigrationConfigs());
  }
}
