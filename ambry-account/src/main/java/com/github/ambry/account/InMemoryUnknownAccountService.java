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
  public static final short NAMED_BLOB_ACCOUNT_ID = 101;
  public static final String NAMED_BLOB_ACCOUNT_NAME = "named-blob-sandbox";
  static final Account UNKNOWN_ACCOUNT =
      new Account(Account.UNKNOWN_ACCOUNT_ID, Account.UNKNOWN_ACCOUNT_NAME, Account.AccountStatus.ACTIVE,
          Account.ACL_INHERITED_BY_CONTAINER_DEFAULT_VALUE, Account.SNAPSHOT_VERSION_DEFAULT_VALUE,
          Arrays.asList(Container.UNKNOWN_CONTAINER, Container.DEFAULT_PUBLIC_CONTAINER,
              Container.DEFAULT_PRIVATE_CONTAINER), Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE);

  // Create a hardcoded Account "named-blob-account" which will be used for s3 prototype tests
  static final Account NAMED_BLOB_ACCOUNT =
      new Account(NAMED_BLOB_ACCOUNT_ID, NAMED_BLOB_ACCOUNT_NAME, Account.AccountStatus.ACTIVE,
          Account.ACL_INHERITED_BY_CONTAINER_DEFAULT_VALUE, Account.SNAPSHOT_VERSION_DEFAULT_VALUE,
          Arrays.asList(Container.DEFAULT_NAMED_BLOB_CONTAINER, Container.NAMED_BLOB_HNS_CONTAINER),
          Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE);

  private static final Collection<Account> accounts =
      Collections.unmodifiableCollection(Arrays.asList(UNKNOWN_ACCOUNT, NAMED_BLOB_ACCOUNT));
  private volatile boolean isOpen = true;

  @Override
  public Account getAccountById(short accountId) {
    checkOpen();
    if (accountId == Account.UNKNOWN_ACCOUNT_ID) {
      return UNKNOWN_ACCOUNT;
    } else if (accountId == NAMED_BLOB_ACCOUNT_ID) {
      return NAMED_BLOB_ACCOUNT;
    }
    return null;
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
    if (accountName.equals(NAMED_BLOB_ACCOUNT_NAME)) {
      return NAMED_BLOB_ACCOUNT;
    } else {
      return UNKNOWN_ACCOUNT;
    }
  }

  @Override
  public void updateAccounts(Collection<Account> accounts) {
    checkOpen();
    Objects.requireNonNull(accounts, "accounts cannot be null");
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
