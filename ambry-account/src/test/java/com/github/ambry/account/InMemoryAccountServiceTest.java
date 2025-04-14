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
    InMemoryAccountServiceFactory factoryWithBadFile = new InMemoryAccountServiceFactory(verifiableProperties, new MetricRegistry());
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
    ContainerBuilder containerA = new ContainerBuilder((short) 1, "containerA", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ContainerBuilder containerB = new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 2345, "test", Account.AccountStatus.ACTIVE, true, 0, containers, Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
    assertEquals(accountsToUpdate.get(0), accountService.getAccountById((short) 2345));
    assertEquals(3, accountService.getAllAccounts().size());
  }

  @Test(expected = AccountServiceException.class)
  public void addAccountConflict() throws AccountServiceException {
    ContainerBuilder containerA = new ContainerBuilder((short) 1, "containerA", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ContainerBuilder containerB = new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 2345);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 2345, "s3tests", Account.AccountStatus.ACTIVE, true, 0, containers, Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
  }

  @Test(expected = AccountServiceException.class)
  public void addContainerConflict() throws AccountServiceException {
    ContainerBuilder containerA = new ContainerBuilder((short) 1, "s3testbucket", Container.ContainerStatus.ACTIVE, "test", (short) 1234);
    ContainerBuilder containerB = new ContainerBuilder((short) 2, "containerB", Container.ContainerStatus.ACTIVE, "test", (short) 1234);
    ArrayList<Container> containers = new ArrayList<>();
    containers.add(containerA.build());
    containers.add(containerB.build());
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account((short) 1234, "s3tests", Account.AccountStatus.ACTIVE, true, 0, containers, Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE));

    AccountService accountService = accountServiceFactory.getAccountService();
    accountService.updateAccounts(accountsToUpdate);
  }
}
