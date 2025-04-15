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

import com.github.ambry.config.AccountServiceConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.AccountUtils.*;


/**
 * Account service which holds account data in-memory.
 *
 * This implementation of the account service is meant primarily for local testing and deployment.
 * It is not thread-safe.
 */
public final class InMemoryAccountService extends AbstractAccountService {

  static Account UNKNOWN_ACCOUNT =
      new Account(Account.UNKNOWN_ACCOUNT_ID, Account.UNKNOWN_ACCOUNT_NAME, Account.AccountStatus.ACTIVE,
          Account.ACL_INHERITED_BY_CONTAINER_DEFAULT_VALUE, Account.SNAPSHOT_VERSION_DEFAULT_VALUE,
          Arrays.asList(Container.UNKNOWN_CONTAINER, Container.DEFAULT_PUBLIC_CONTAINER,
              Container.DEFAULT_PRIVATE_CONTAINER), Account.QUOTA_RESOURCE_TYPE_DEFAULT_VALUE);
  private static final Logger logger = LoggerFactory.getLogger(InMemoryAccountService.class);

  /** Location of the account file which this service uses as it's source of accounts.*/
  private final Path accountFile;

  /**
   * Constructs an {@code InMemoryAccountService}. {@code #init} method MUST be called after construction.
   *
   * @param accountServiceMetrics Metrics instance to log metrics to.
   */
  public InMemoryAccountService(Path accountFile, AccountServiceMetrics accountServiceMetrics,
      AccountServiceConfig accountConfig) {
    super(accountConfig, accountServiceMetrics, null);
    this.accountFile = accountFile;
  }

  /** Guards against calling {@link #init} multiple times.*/
  private final AtomicBoolean initialized = new AtomicBoolean();

  /**
   * Initializes this instance. Does all work which requires handing out references to this instance itself which can't
   * be done during normal instance construction without letting a reference to an incomplete constructed instance escape.
   */
  public void init() {
    if (!initialized.getAndSet(true)) {
      processAccountJsonFile();

      // add the default unknown account
      ArrayList<Account> accounts = new ArrayList<>();
      accounts.add(UNKNOWN_ACCOUNT);

      try {
        updateAccounts(accounts);
      } catch (AccountServiceException e) {
        logger.warn("Unable to add default unknown account", e);
      }
    }
  }

  @Override
  protected void checkOpen() {
    if (!initialized.get()) {
      throw new IllegalStateException("AccountService is closed.");
    }
  }

  @Override
  protected void onAccountChangeMessage(String topic, String message) {
    // We're good.
  }

  /**
   * Adds accounts
   *
   * @param accounts The collection of {@link Account}s to update. Cannot be {@code null}.
   */
  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    if (hasDuplicateAccountIdOrName(accounts)) {
      accountServiceMetrics.updateAccountErrorCount.inc();
      throw new AccountServiceException("Duplicate account id or name exist in the accounts to update",
          AccountServiceErrorCode.ResourceConflict);
    }

    if (accountInfoMapRef.get().hasConflictingAccount(accounts, true)) {
      throw new AccountServiceException("Input accounts conflict with the accounts in local cache",
          AccountServiceErrorCode.ResourceConflict);
    }

    for (Account account : accounts) {
      if (accountInfoMapRef.get().hasConflictingContainer(account.getAllContainers(), account.getId(), true)) {
        throw new AccountServiceException("Containers in account " + account.getId() + " conflict with local cache",
            AccountServiceErrorCode.ResourceConflict);
      }
    }

    accountInfoMapRef.get().addOrUpdateAccounts(accounts);
  }

  @Override
  public void close() throws IOException {
    // Nothing to actually close here.
  }

  /**
   * Loads initial accounts into memory via a JSON file.
   */
  private void processAccountJsonFile() {
    if (accountFile == null) {
      return;
    }

    try {
      if (!Files.exists(accountFile)) {
        logger.warn("JSON account file '{}' does not exist", accountFile.toAbsolutePath());
        return;
      }
      String accountsJsonString = new String(Files.readAllBytes(accountFile), StandardCharsets.UTF_8);
      AccountInfoMap newAccountInfoMap = new AccountInfoMap(accountsJsonString);
      accountInfoMapRef.set(newAccountInfoMap);
    } catch (Exception e) {
      logger.error("Failed updating accounts", e);
    }
  }
}
