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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;


/**
 * An implementation of {@link AccountService} that always has a single entry which is the unknown account. Any
 * queries by account name to this account service will unconditionally return the unknown account. This
 * account service is in memory, and does not talk to any persistent storage service.
 */
class InMemoryUnknownAccountService implements AccountService {
  static final Account UNKNOWN_ACCOUNT =
      new Account(Account.UNKNOWN_ACCOUNT_ID, Account.UNKNOWN_ACCOUNT_NAME, Account.AccountStatus.ACTIVE,
          Account.SNAPSHOT_VERSION_DEFAULT_VALUE,
          Arrays.asList(Container.UNKNOWN_CONTAINER, Container.DEFAULT_PUBLIC_CONTAINER,
              Container.DEFAULT_PRIVATE_CONTAINER));
  private static final Collection<Account> accounts =
      Collections.unmodifiableCollection(Collections.singletonList(UNKNOWN_ACCOUNT));
  private volatile boolean isOpen = true;

  @Override
  public Account getAccountById(short accountId) {
    checkOpen();
    return accountId == Account.UNKNOWN_ACCOUNT_ID ? UNKNOWN_ACCOUNT : null;
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to subscribe cannot be null");
    return true;
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to unsubscribe cannot be null");
    return true;
  }

  @Override
  public Account getAccountByName(String accountName) {
    checkOpen();
    Objects.requireNonNull(accountName, "accountName cannot be null.");
    return UNKNOWN_ACCOUNT;
  }

  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    checkOpen();
    Objects.requireNonNull(accounts, "accounts cannot be null");
    return false;
  }

  @Override
  public Collection<Account> getAllAccounts() {
    checkOpen();
    return accounts;
  }

  @Override
  public void close() {
    isOpen = false;
  }

  /**
   * Checks if the service is open.
   */
  private void checkOpen() {
    if (!isOpen) {
      throw new IllegalStateException("AccountService is closed.");
    }
  }
}
