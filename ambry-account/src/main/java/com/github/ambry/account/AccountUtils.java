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
package com.github.ambry.account;

import com.github.ambry.server.StatsSnapshot;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utils for Account-related operations.
 */
public class AccountUtils {
  private static final Logger logger = LoggerFactory.getLogger(AccountUtils.class);

  /**
   * Checks if there are duplicate accountId or accountName in the given collection of {@link Account}s.
   *
   * @param accounts A collection of {@link Account}s to check duplication.
   * @return {@code true} if there are duplicated accounts in id or name in the given collection of accounts,
   *                      {@code false} otherwise.
   */
  static boolean hasDuplicateAccountIdOrName(Collection<Account> accounts) {
    if (accounts == null) {
      return false;
    }
    Set<Short> idSet = new HashSet<>();
    Set<String> nameSet = new HashSet<>();
    boolean res = false;
    for (Account account : accounts) {
      if (!idSet.add(account.getId()) || !nameSet.add(account.getName())) {
        logger.debug("Accounts to update have conflicting id or name. Conflicting accountId={} accountName={}",
            account.getId(), account.getName());
        res = true;
        break;
      }
    }
    return res;
  }

  /**
   * Returns {@link Set} of deprecated {@link Container}s ready for deletion from {@link AccountService}.
   * @param accountService {@link AccountService} object.
   * @param containerDeletionRetentionDays Number of days upto which deprecated containers can be marked as ACTIVE.
   * @return {@link Set} of deprecated {@link Container}s.
   */
  public static Set<Container> getDeprecatedContainers(AccountService accountService,
      long containerDeletionRetentionDays) {
    Set<Container> deprecatedContainers = new HashSet<>();
    accountService.getContainersByStatus(Container.ContainerStatus.DELETE_IN_PROGRESS).forEach((container) -> {
      if (container.getDeleteTriggerTime() + TimeUnit.DAYS.toMillis(containerDeletionRetentionDays)
          <= System.currentTimeMillis()) {
        deprecatedContainers.add(container);
      }
    });
    deprecatedContainers.addAll(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE));
    return deprecatedContainers;
  }

  /**
   * Compares and logs differences (if any) in Accounts.
   * @param accountsInPrimary accounts in primary collection.
   * @param accountsInSecondary accounts in secondary collection.
   * @return count of accounts mismatching.
   */
  public static int compareAccounts(Collection<Account> accountsInPrimary, Collection<Account> accountsInSecondary) {
    int mismatchCount = 0;
    Map<String, Account> secondaryAccountMap = new HashMap<>();
    accountsInSecondary.forEach(account -> secondaryAccountMap.put(account.getName(), account));

    Set<Account> accountsMissingInSecondary = accountsInPrimary.stream()
        .filter(account -> !secondaryAccountMap.containsKey(account.getName()))
        .collect(Collectors.toSet());

    Set<Account> accountsDifferentInSecondary = new HashSet<>(accountsInPrimary);
    accountsDifferentInSecondary.removeAll(accountsInSecondary);
    accountsDifferentInSecondary.removeAll(accountsMissingInSecondary);

    if (!accountsMissingInSecondary.isEmpty() || !accountsDifferentInSecondary.isEmpty()) {

      StringBuilder accountsInfo = new StringBuilder();

      if (!accountsMissingInSecondary.isEmpty()) {
        accountsInfo.append("[");
        for (Account account : accountsMissingInSecondary) {
          accountsInfo.append(AccountCollectionSerde.accountToJsonNoContainers(account).toString()).append(",");
        }
        accountsInfo.append("]");
        logger.warn("Accounts found in primary and absent in secondary = {}", accountsInfo.toString());
        mismatchCount += accountsMissingInSecondary.size();
      }

      if (!accountsDifferentInSecondary.isEmpty()) {
        accountsInfo.setLength(0);
        accountsInfo.append("[");
        for (Account account : accountsDifferentInSecondary) {
          accountsInfo.append("{Account = ")
              .append(account.toString())
              .append(", primary = ")
              .append(AccountCollectionSerde.accountToJsonNoContainers(account).toString())
              .append(", secondary = ")
              .append(AccountCollectionSerde.accountToJsonNoContainers(secondaryAccountMap.get(account.getName()))
                  .toString());

          Set<Container> containersMissingInSecondary = account.getAllContainers()
              .stream()
              .filter(container -> secondaryAccountMap.get(account.getName()).getContainerByName(container.getName())
                  == null)
              .collect(Collectors.toSet());

          Set<Container> containersDifferentInSecondary = new HashSet<>(account.getAllContainers());
          containersDifferentInSecondary.removeAll(secondaryAccountMap.get(account.getName()).getAllContainers());
          containersDifferentInSecondary.removeAll(containersMissingInSecondary);

          if (!containersMissingInSecondary.isEmpty()) {
            accountsInfo.append(", Containers missing in secondary: [");
            for (Container container : containersMissingInSecondary) {
              accountsInfo.append(container.toJson().toString()).append(",");
            }
            accountsInfo.append("]");
            mismatchCount += containersMissingInSecondary.size();
          }

          if (!containersDifferentInSecondary.isEmpty()) {
            accountsInfo.append(", Containers different in secondary: [");
            for (Container container : containersDifferentInSecondary) {
              accountsInfo.append("{container = ")
                  .append(container.toString())
                  .append(", primary = ")
                  .append(container.toJson().toString())
                  .append(", secondary = ")
                  .append(secondaryAccountMap.get(account.getName())
                      .getContainerByName(container.getName())
                      .toJson()
                      .toString())
                  .append("},");
            }
            accountsInfo.append("]");
            mismatchCount += containersDifferentInSecondary.size();
          }
          accountsInfo.append("}");
        }
        accountsInfo.append("]");

        logger.warn("Accounts mismatch in primary and secondary = {}", accountsInfo.toString());
      }
    }
    return mismatchCount;
  }

  /**
   * Selects {@link Container}s to be marked as INACTIVE. Check the valid data size of each DELETE_IN_PROGRESS container
   * from {@link StatsSnapshot} and select the ones with zero data size to be marked as INACTIVE.
   * @return {@link Set} of inactive {@link Container} candidates.
   */
  public static Set<Container> selectInactiveContainerCandidates(StatsSnapshot statsSnapshot,
      Set<Container> deleteInProgressContainerSet) {
    Set<Container> inactiveContainerCandidateSet = new HashSet<>();
    if (statsSnapshot != null) {
      Map<String, Set<String>> nonEmptyContainersByAccount = new HashMap<>();
      searchNonEmptyContainers(nonEmptyContainersByAccount, statsSnapshot, null);
      for (Container container : deleteInProgressContainerSet) {
        String containerIdToString = "C[" + container.getId() + "]";
        String accountIdToString = "A[" + container.getParentAccountId() + "]";
        if (nonEmptyContainersByAccount.containsKey(accountIdToString) && nonEmptyContainersByAccount.get(
            accountIdToString).contains(containerIdToString)) {
          logger.debug("Container {} has not been compacted yet", container);
        } else {
          logger.info("Container {} has been compacted already", container);
          inactiveContainerCandidateSet.add(container);
        }
      }
    }
    return inactiveContainerCandidateSet;
  }

  /**
   * Gets valid data size {@link Container}s. The qualified {@link Container}s' raw valid data size should be larger than zero.
   * @param nonEmptyContainersByAccount it holds a mapping of {@link Account}s to {@link Container}s which raw valid data size larger than zero.
   * @param statsSnapshot the {@link StatsSnapshot} generated from cluster wide aggregation.
   * @param keyName the key of subMap for each level of {@link StatsSnapshot}.
   */
  public static void searchNonEmptyContainers(Map<String, Set<String>> nonEmptyContainersByAccount,
      StatsSnapshot statsSnapshot, String keyName) {
    if (statsSnapshot.getSubMap() != null && !statsSnapshot.getSubMap().isEmpty()) {
      for (Map.Entry<String, StatsSnapshot> entry : statsSnapshot.getSubMap().entrySet()) {
        if (entry.getKey().startsWith("C") && entry.getValue().getValue() > 0) {
          Objects.requireNonNull(keyName,
              "keyName should not be null since every container will have it's corresponding accountId");
          nonEmptyContainersByAccount.getOrDefault(keyName, new HashSet<>()).add(entry.getKey());
        } else if (entry.getKey().startsWith("A")) {
          nonEmptyContainersByAccount.putIfAbsent(entry.getKey(), new HashSet<>());
          searchNonEmptyContainers(nonEmptyContainersByAccount, entry.getValue(), entry.getKey());
        }
      }
    }
  }
}
