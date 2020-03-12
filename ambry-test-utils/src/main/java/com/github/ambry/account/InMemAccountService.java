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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;


/**
 * Implementation of {@link AccountService} for test. This implementation synchronizes on all methods.
 */
public class InMemAccountService implements AccountService {
  /**
   * An account defined specifically for the blobs put without specifying target account and container. In the
   * pre-containerization world, a put-blob request does not carry any information which account/container to store
   * the blob. These blobs are assigned to this account if their service ID does not match a valid account, because the
   * target account information is unknown.
   */
  public static final Account UNKNOWN_ACCOUNT =
      new Account(Account.UNKNOWN_ACCOUNT_ID, Account.UNKNOWN_ACCOUNT_NAME, Account.AccountStatus.ACTIVE,
          Account.SNAPSHOT_VERSION_DEFAULT_VALUE,
          Arrays.asList(Container.UNKNOWN_CONTAINER, Container.DEFAULT_PUBLIC_CONTAINER,
              Container.DEFAULT_PRIVATE_CONTAINER));
  private boolean shouldUpdateSucceed = true;
  private final boolean shouldReturnOnlyUnknown;
  private final boolean notifyConsumers;
  private final Map<Short, Account> idToAccountMap = new HashMap<>();
  private final Map<String, Account> nameToAccountMap = new HashMap<>();
  private final Set<Consumer<Collection<Account>>> accountUpdateConsumers = new HashSet<>();

  /**
   * Constructor.
   * @param shouldReturnOnlyUnknown {@code true} if always returns {@link InMemAccountService#UNKNOWN_ACCOUNT} when queried
   *                                                     by account name; {@code false} to do the actual query.
   * @param notifyConsumers {@code true} if consumers should be notified of account changes. {@code false} otherwise.
   */
  public InMemAccountService(boolean shouldReturnOnlyUnknown, boolean notifyConsumers) {
    this.shouldReturnOnlyUnknown = shouldReturnOnlyUnknown;
    this.notifyConsumers = notifyConsumers;
  }

  @Override
  public synchronized Account getAccountById(short accountId) {
    return shouldReturnOnlyUnknown ? UNKNOWN_ACCOUNT : idToAccountMap.get(accountId);
  }

  @Override
  public synchronized Account getAccountByName(String accountName) {
    return shouldReturnOnlyUnknown ? UNKNOWN_ACCOUNT : nameToAccountMap.get(accountName);
  }

  @Override
  public synchronized boolean updateAccounts(Collection<Account> accounts) {
    if (!shouldUpdateSucceed) {
      return false;
    }
    for (Account account : accounts) {
      idToAccountMap.put(account.getId(), account);
      nameToAccountMap.put(account.getName(), account);
    }
    if (notifyConsumers) {
      for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
        accountUpdateConsumer.accept(accounts);
      }
    }
    return true;
  }

  @Override
  public synchronized boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to subscribe cannot be null");
    return accountUpdateConsumers.add(accountUpdateConsumer);
  }

  @Override
  public synchronized boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to unsubscribe cannot be null");
    return accountUpdateConsumers.remove(accountUpdateConsumer);
  }

  @Override
  public synchronized Collection<Account> getAllAccounts() {
    return idToAccountMap.values();
  }

  @Override
  public void close() {
    // no op
  }

  /**
   * Clears all the accounts in this {@code AccountService}.
   */
  public synchronized void clear() {
    Collection<Account> allAccounts = idToAccountMap.values();
    idToAccountMap.clear();
    nameToAccountMap.clear();
    if (notifyConsumers) {
      for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
        accountUpdateConsumer.accept(allAccounts);
      }
    }
  }

  /**
   * Creates and adds an {@link Account} to this {@link AccountService}. The account will contain one container
   * with {@link Container#DEFAULT_PUBLIC_CONTAINER_ID}, one with {@link Container#DEFAULT_PRIVATE_CONTAINER_ID} and
   * one other random {@link Container}.
   * @return the {@link Account} that was created and added.
   */
  public synchronized Account createAndAddRandomAccount() {
    Account account = generateRandomAccount();
    updateAccounts(Collections.singletonList(account));
    return account;
  }

  /**
   * Generates an {@link Account} but does not add it to this {@link AccountService}. The account will contain one
   * container with {@link Container#DEFAULT_PUBLIC_CONTAINER_ID}, one with
   * {@link Container#DEFAULT_PRIVATE_CONTAINER_ID} and one other random {@link Container}.
   * @return the {@link Account} that was created.
   */
  public synchronized Account generateRandomAccount() {
    short refAccountId;
    String refAccountName;
    do {
      refAccountId = Utils.getRandomShort(TestUtils.RANDOM);
      refAccountName = TestUtils.getRandomString(10);
    } while (idToAccountMap.containsKey(refAccountId) || nameToAccountMap.containsKey(refAccountName));
    Account.AccountStatus refAccountStatus = Account.AccountStatus.ACTIVE;
    Container randomContainer = getRandomContainer(refAccountId);
    Container publicContainer =
        new ContainerBuilder(Container.DEFAULT_PUBLIC_CONTAINER).setParentAccountId(refAccountId).build();
    Container privateContainer =
        new ContainerBuilder(Container.DEFAULT_PRIVATE_CONTAINER).setParentAccountId(refAccountId).build();
    return new AccountBuilder(refAccountId, refAccountName, refAccountStatus).addOrUpdateContainer(publicContainer)
        .addOrUpdateContainer(privateContainer)
        .addOrUpdateContainer(randomContainer)
        .build();
  }

  /**
   * @param shouldUpdateSucceed the new value of {@code shouldUpdateSucceed}, which will be returned by subsequent
   *                            {@link #updateAccounts(Collection)} calls.
   */
  public void setShouldUpdateSucceed(boolean shouldUpdateSucceed) {
    this.shouldUpdateSucceed = shouldUpdateSucceed;
  }

  /**
   * Adds {@code replicationPolicy} to {@code container}.
   * @param container the {@link Container} to add the replication policy to.
   * @param replicationPolicy the replication policy to add to {@code container}.
   * @return the new container object with the appropriate replication policy if the operation suceeded. {@code null}
   * otherwise
   */
  public Container addReplicationPolicyToContainer(Container container, String replicationPolicy) {
    Container newContainer = new ContainerBuilder(container).setReplicationPolicy(replicationPolicy).build();
    Account account = getAccountById(container.getParentAccountId());
    Account newAccount = new AccountBuilder(account).addOrUpdateContainer(newContainer).build();
    return updateAccounts(Collections.singleton(newAccount)) ? newContainer : null;
  }

  /**
   * Creates and returns a random {@link Container} for {@code accountId}.
   * @param accountId the account id for the container
   * @return returns a random {@link Container} for {@code accountId}
   */
  private Container getRandomContainer(short accountId) {
    // adding +2 so that the ID is not 0 or 1
    short refContainerId = (short) (TestUtils.RANDOM.nextInt(Short.MAX_VALUE - 1) + 2);
    String refContainerName = TestUtils.getRandomString(10);
    Container.ContainerStatus refContainerStatus = Container.ContainerStatus.ACTIVE;
    String refContainerDescription = TestUtils.getRandomString(10);
    boolean refContainerEncryption = TestUtils.RANDOM.nextBoolean();
    boolean refContainerPreviousEncryption = refContainerEncryption || TestUtils.RANDOM.nextBoolean();
    boolean refContainerCaching = TestUtils.RANDOM.nextBoolean();
    boolean refContainerMediaScanDisabled = TestUtils.RANDOM.nextBoolean();
    boolean refContainerBackupEnabled = TestUtils.RANDOM.nextBoolean();
    return new ContainerBuilder(refContainerId, refContainerName, refContainerStatus, refContainerDescription,
        accountId).setEncrypted(refContainerEncryption)
        .setPreviouslyEncrypted(refContainerPreviousEncryption)
        .setCacheable(refContainerCaching)
        .setMediaScanDisabled(refContainerMediaScanDisabled)
        .setTtlRequired(false)
        .setSecurePathRequired(false)
        .setBackupEnabled(refContainerBackupEnabled)
        .build();
  }
}
