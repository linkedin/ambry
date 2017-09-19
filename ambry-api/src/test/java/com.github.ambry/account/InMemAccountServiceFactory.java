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

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.IOException;
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
 * Factory to create {@link InMemAccountService}. The account services returned are static variables.
 */
public class InMemAccountServiceFactory implements AccountServiceFactory {
  private static final InMemAccountService ONLY_UNKNOWN = new InMemAccountService(true);
  private static final InMemAccountService ANY_ACCOUNT = new InMemAccountService(false);

  private final boolean returnOnlyUnknown;

  /**
   * Constructor. If the properties contains a field "in.mem.account.service.only.unknown" set to {@code true}, an
   * {@link InMemAccountService} that only returns {@link Account#UNKNOWN_ACCOUNT} is returned. Otherwise a fully
   * functional service is returned. These account services are also static (singleton) so the same instance of these
   * services is returned no matter how many times {@link #getAccountService()} is called or different instances of
   * {@link InMemAccountServiceFactory} are created.
   * @param verifiableProperties The properties to get a {@link InMemAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry will be discarded
   * @param notifier will be discarded
   */
  public InMemAccountServiceFactory(VerifiableProperties verifiableProperties, Object metricRegistry, Object notifier) {
    returnOnlyUnknown = verifiableProperties.getBoolean("in.mem.account.service.only.unknown", false);
  }

  public InMemAccountServiceFactory(boolean returnOnlyUnknown) {
    this.returnOnlyUnknown = returnOnlyUnknown;
  }

  @Override
  public InMemAccountService getAccountService() {
    return returnOnlyUnknown ? ONLY_UNKNOWN : ANY_ACCOUNT;
  }

  /**
   * Implementation of {@link AccountService} for test. This implementation synchronizes on all methods.
   */
  public static class InMemAccountService implements AccountService {
    private final boolean shouldReturnOnlyUnknownAccount;
    private final Map<Short, Account> idToAccountMap = new HashMap<>();
    private final Map<String, Account> nameToAccountMap = new HashMap<>();
    private final Set<Consumer<Collection<Account>>> accountUpdateConsumers = new HashSet<>();

    /**
     * Constructor.
     * @param shouldReturnOnlyUnknownAccount {@code true} if always returns {@link Account#UNKNOWN_ACCOUNT} when queried
     *                                                     by account name; {@code false} to do the actual query.
     */
    private InMemAccountService(boolean shouldReturnOnlyUnknownAccount) {
      this.shouldReturnOnlyUnknownAccount = shouldReturnOnlyUnknownAccount;
    }

    @Override
    public synchronized Account getAccountById(short accountId) {
      return shouldReturnOnlyUnknownAccount ? Account.UNKNOWN_ACCOUNT : idToAccountMap.get(accountId);
    }

    @Override
    public synchronized Account getAccountByName(String accountName) {
      return shouldReturnOnlyUnknownAccount ? Account.UNKNOWN_ACCOUNT : nameToAccountMap.get(accountName);
    }

    @Override
    public synchronized boolean updateAccounts(Collection<Account> accounts) {
      for (Account account : accounts) {
        idToAccountMap.put(account.getId(), account);
        nameToAccountMap.put(account.getName(), account);
      }
      for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
        accountUpdateConsumer.accept(accounts);
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
    public void close() throws IOException {
      // no op
    }

    /**
     * Clears all the accounts in this {@code AccountService}.
     */
    public synchronized void clear() {
      Collection<Account> allAccounts = idToAccountMap.values();
      idToAccountMap.clear();
      nameToAccountMap.clear();
      for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
        accountUpdateConsumer.accept(allAccounts);
      }
    }

    /**
     * Creates and adds an {@link Account} to this {@link AccountService}. The account will contain one container
     * with {@link Container#DEFAULT_PUBLIC_CONTAINER_ID}, one with {@link Container#DEFAULT_PRIVATE_CONTAINER_ID} and
     * one other random {@link Container}.
     * @return the {@link Account} that was created and added.
     */
    public synchronized Account createAndAddRandomAccount() {
      short refAccountId;
      String refAccountName;
      do {
        refAccountId = Utils.getRandomShort(TestUtils.RANDOM);
        refAccountName = UtilsTest.getRandomString(10);
      } while (idToAccountMap.containsKey(refAccountId) || nameToAccountMap.containsKey(refAccountName));
      Account.AccountStatus refAccountStatus = Account.AccountStatus.ACTIVE;
      Container randomContainer = getRandomContainer(refAccountId);
      Container publicContainer = getDefaultContainer(refAccountId, false);
      Container privateContainer = getDefaultContainer(refAccountId, true);
      Account account = new AccountBuilder(refAccountId, refAccountName, refAccountStatus,
          Arrays.asList(publicContainer, privateContainer, randomContainer)).build();
      updateAccounts(Collections.singletonList(account));
      return account;
    }

    /**
     * Creates and returns default public/private container for {@code accountId}.
     * @param accountId the account id for the container
     * @param isPrivate {@code true} if private container is required. {@code false} for public.
     * @return default public/private container for {@code accountId}.
     */
    private Container getDefaultContainer(short accountId, boolean isPrivate) {
      Container container;
      if (isPrivate) {
        container =
            new ContainerBuilder(Container.DEFAULT_PRIVATE_CONTAINER_ID, Container.DEFAULT_PRIVATE_CONTAINER_NAME,
                Container.DEFAULT_PRIVATE_CONTAINER_STATUS, Container.DEFAULT_PRIVATE_CONTAINER_DESCRIPTION,
                Container.DEFAULT_PRIVATE_CONTAINER_IS_PRIVATE_SETTING, accountId).build();
      } else {
        container = new ContainerBuilder(Container.DEFAULT_PUBLIC_CONTAINER_ID, Container.DEFAULT_PUBLIC_CONTAINER_NAME,
            Container.DEFAULT_PUBLIC_CONTAINER_STATUS, Container.DEFAULT_PUBLIC_CONTAINER_DESCRIPTION,
            Container.DEFAULT_PUBLIC_CONTAINER_IS_PRIVATE_SETTING, accountId).build();
      }
      return container;
    }

    /**
     * Creates and returns a random {@link Container} for {@code accountId}.
     * @param accountId the account id for the container
     * @return returns a random {@link Container} for {@code accountId}
     */
    private Container getRandomContainer(short accountId) {
      short refContainerId = Utils.getRandomShort(TestUtils.RANDOM);
      String refContainerName = UtilsTest.getRandomString(10);
      Container.ContainerStatus refContainerStatus = Container.ContainerStatus.ACTIVE;
      String refContainerDescription = UtilsTest.getRandomString(10);
      boolean refContainerPrivacy = TestUtils.RANDOM.nextBoolean();
      return new ContainerBuilder(refContainerId, refContainerName, refContainerStatus, refContainerDescription,
          refContainerPrivacy, accountId).build();
    }
  }
}

