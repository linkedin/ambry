/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceErrorCode;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


/**
 * Utils for testing account-related classes.
 */
public class AccountTestUtils {
  private static final Random random = TestUtils.RANDOM;

  /**
   * Assert a collection of {@link Account}s exist in the {@link AccountService}.
   * @param accounts The collection of {@link Account}s to assert their existence.
   * @param expectedAccountCount The expected number of {@link Account}s in the {@link AccountService}.
   * @param accountService The {@link AccountService} to assert {@link Account}s existence.
   */
  public static void assertAccountsInAccountService(Collection<Account> accounts, int expectedAccountCount,
      AccountService accountService) {
    assertEquals("Wrong number of accounts in accountService", expectedAccountCount,
        accountService.getAllAccounts().size());
    for (Account account : accounts) {
      assertAccountInAccountService(account, accountService);
    }
  }

  /**
   * Asserts that an {@link Account} exists in the {@link AccountService}.
   * @param account The {@link Account} to assert existence.
   * @param accountService The {@link AccountService} to assert {@link Account} existence.
   */
  public static void assertAccountInAccountService(Account account, AccountService accountService) {
    Account accountFoundById = accountService.getAccountById(account.getId());
    Account accountFoundByName = accountService.getAccountByName(account.getName());
    assertEquals("Account got by name from accountService does not match account to assert.", account,
        accountFoundByName);
    assertEquals("Account got by id from accountService does not match the account to assert", account,
        accountFoundById);
    assertEquals("The number of containers in the account is wrong.", accountFoundById.getAllContainers().size(),
        account.getAllContainers().size());
    for (Container container : account.getAllContainers()) {
      assertContainerInAccountService(container, accountService);
    }
  }

  /**
   * Assert that a {@link Container} exists in the {@link AccountService}.
   * @param container The {@link Container} to assert.
   * @param accountService The {@link AccountService} to assert {@link Container} existence.
   */
  public static void assertContainerInAccountService(Container container, AccountService accountService) {
    Container containerFoundById =
        accountService.getAccountById(container.getParentAccountId()).getContainerById(container.getId());
    Container containerFoundByName =
        accountService.getAccountById(container.getParentAccountId()).getContainerByName(container.getName());
    assertEquals("Container got by id from accountService/account does not match container got by name.",
        containerFoundById, containerFoundByName);
    assertEquals("Container got by id from accountService/account does not match the container to assert",
        containerFoundById, container);
  }

  /**
   * Updates a collection of {@link Account}s and verifies that an {@link AccountServiceException} with the expected
   * error code is thrown.
   * @param accounts the accounts to update.
   * @param expectedErrorCode the {@link AccountServiceErrorCode} expected.
   * @param accountService The {@link AccountService} to update.
   */
  public static void assertUpdateAccountsFails(Collection<Account> accounts, AccountServiceErrorCode expectedErrorCode,
      AccountService accountService) {
    try {
      accountService.updateAccounts(accounts);
      fail("Expected update to fail");
    } catch (AccountServiceException ase) {
      assertEquals("Unexpected error code", expectedErrorCode, ase.getErrorCode());
    }
  }

  /**
   * Randomly generates a collection of {@link Account}s, which do not have the same id or name. The {@link Container}s
   * of the same {@link Account} also do not have the same id or name.
   * @param idToRefAccountMap A map from id to {@link Account} to populate with the generated {@link Account}s.
   * @param idToRefContainerMap A map from name to {@link Account} to populate with the generated {@link Account}s.
   * @param accountIdSet A set of ids that could not be used to generate {@link Account}s.
   * @param accountCount The number of {@link Account}s to generate.
   * @param containerCountPerAccount The number of {@link Container}s per {@link Account} to generate.
   */
  public static void generateRefAccounts(Map<Short, Account> idToRefAccountMap,
      Map<Short, Map<Short, Container>> idToRefContainerMap, Set<Short> accountIdSet, int accountCount,
      int containerCountPerAccount) {
    idToRefAccountMap.clear();
    idToRefContainerMap.clear();
    for (int i = 0; i < accountCount; i++) {
      short accountId = Utils.getRandomShort(random);
      if (!accountIdSet.add(accountId)) {
        i--;
        continue;
      }
      String accountName = UUID.randomUUID().toString();
      Account.AccountStatus accountStatus =
          random.nextBoolean() ? Account.AccountStatus.ACTIVE : Account.AccountStatus.INACTIVE;
      List<Container> containers = new ArrayList<>();
      List<ContainerBuilder> containerBuilders = generateContainerBuilders(containerCountPerAccount, accountId);

      containers.addAll(containerBuilders.stream().map(ContainerBuilder::build).collect(Collectors.toList()));
      Map<Short, Container> idToContainers =
          containers.stream().collect(Collectors.toMap(Container::getId, Function.identity()));
      Account account = new AccountBuilder(accountId, accountName, accountStatus).containers(containers).build();
      assertEquals("Wrong number of generated containers for the account", containerCountPerAccount,
          account.getAllContainers().size());
      idToRefAccountMap.put(accountId, account);
      idToRefContainerMap.put(accountId, idToContainers);
    }
    assertEquals("Wrong number of generated accounts", accountCount, idToRefAccountMap.size());
  }

  /**
   * Generate {@link ContainerBuilder}s for specified {@code accountId}.
   * @param numContainers number of {@link ContainerBuilder}s to generate.
   * @param accountId accountId for container.
   * @return {@link List} of {@link ContainerBuilder}s.
   */
  public static List<ContainerBuilder> generateContainerBuilders(int numContainers, short accountId) {
    List<ContainerBuilder> containerBuilders = new ArrayList<>();
    Set<Short> containerIdSet = new HashSet<>();
    for (int j = 0; j < numContainers; j++) {
      short containerId = Utils.getRandomShort(random);
      if (!containerIdSet.add(containerId)) {
        j--;
        continue;
      }
      String containerName = UUID.randomUUID().toString();
      Container.ContainerStatus containerStatus =
          random.nextBoolean() ? Container.ContainerStatus.ACTIVE : Container.ContainerStatus.INACTIVE;
      String containerDescription = UUID.randomUUID().toString();
      boolean containerCaching = random.nextBoolean();
      boolean containerEncryption = random.nextBoolean();
      boolean containerPreviousEncryption = containerEncryption || random.nextBoolean();
      boolean mediaScanDisabled = random.nextBoolean();
      String replicationPolicy = TestUtils.getRandomString(10);
      boolean ttlRequired = random.nextBoolean();
      ContainerBuilder containerBuilder =
          new ContainerBuilder(containerId, containerName, containerStatus, containerDescription,
              accountId).setEncrypted(containerEncryption)
              .setPreviouslyEncrypted(containerPreviousEncryption)
              .setCacheable(containerCaching)
              .setMediaScanDisabled(mediaScanDisabled)
              .setReplicationPolicy(replicationPolicy)
              .setTtlRequired(ttlRequired);
      containerBuilders.add(containerBuilder);
    }
    return containerBuilders;
  }
}

