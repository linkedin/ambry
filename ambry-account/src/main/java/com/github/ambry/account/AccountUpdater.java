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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.HelixNotifier;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.github.ambry.account.AccountUtils.*;


/**
 * A component that is used to create/update {@link Account}s to {@code ZooKeeper} server. Underneath it
 * employs an {@link AccountService} to do the actual write operation, and a {@link Notifier} to publish
 * message for account metadata change once the write operation is completed.
 */
class AccountUpdater {
  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 5000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  private final AccountService accountService;
  private final Properties helixConfigProps = new Properties();
  private final VerifiableProperties vHelixConfigProps;
  private final HelixPropertyStoreConfig storeConfig;
  private final Notifier notifier;

  /**
   * Constructor.
   * @param zkServer The {@code ZooKeeper} server address to connect.
   * @param storePath The path for {@link org.apache.helix.store.HelixPropertyStore}, which will be used as the
   *                  root path for both {@link HelixAccountService} and {@link HelixNotifier}.
   * @param zkConnectionTimeoutMs The timeout in millisecond to connect to the {@code ZooKeeper} server.
   * @param zkSessionTimeoutMs The timeout in millisecond for a session to the {@code ZooKeeper} server.
   */
  private AccountUpdater(String zkServer, String storePath, int zkConnectionTimeoutMs, int zkSessionTimeoutMs) {
    helixConfigProps.setProperty(
        HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms",
        String.valueOf(zkConnectionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms",
        String.valueOf(zkSessionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connect.string",
        zkServer);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path", storePath);
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    notifier = new HelixNotifier(storeConfig);
    accountService =
        new HelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier).getAccountService();
  }

  /**
   * Create or update a collection of {@link Account}s in {@code ZooKeeper}. The {@link Account}s are specified
   * in a file that contains a {@link JSONArray}, with each element be the json format of the {@link Account}s
   * to create or update.
   * @param accountJsonFilePath The path of the file that contains a json array of the {@link Account}s to update.
   * @param zkServer The address of the {@code ZooKeeper} server.
   * @param storePath The path for {@link org.apache.helix.store.HelixPropertyStore}, which will be used as the
   *                  root path for both {@link HelixAccountService} and {@link HelixNotifier}.
   * @param zkConnectionTimeoutMs The timeout in millisecond to connect to the {@code ZooKeeper} server.
   * @param zkSessionTimeoutMs The timeout in millisecond for a session to the {@code ZooKeeper} server.
   * @return The number of accounts that have been successfully created or updated. {@code -1} indicates that
   *         the operation is not successful.
   * @throws Exception Any exception during the operation.
   */
  static int createOrUpdate(String accountJsonFilePath, String zkServer, String storePath,
      Integer zkConnectionTimeoutMs, Integer zkSessionTimeoutMs) throws Exception {
    int zkClientConnectionTimeoutMs =
        zkConnectionTimeoutMs == null ? ZK_CLIENT_CONNECTION_TIMEOUT_MS : zkConnectionTimeoutMs;
    int zkClientSessionTimeoutMs = zkSessionTimeoutMs == null ? ZK_CLIENT_SESSION_TIMEOUT_MS : zkSessionTimeoutMs;
    JSONArray accountArray = new JSONArray(Utils.readStringFromFile(accountJsonFilePath));
    Collection<Account> accounts = new ArrayList<>();
    for (int i = 0; i < accountArray.length(); i++) {
      JSONObject accountJson = accountArray.getJSONObject(i);
      accounts.add(Account.fromJson(accountJson));
    }
    int res = -1;
    if (!hasDuplicateAccountIdOrName(accounts)) {
      AccountUpdater accountUpdater =
          new AccountUpdater(zkServer, storePath, zkClientConnectionTimeoutMs, zkClientSessionTimeoutMs);
      if (accountUpdater.update(accounts)) {
        res = accountArray.length();
      }
    } else {
      throw new IllegalArgumentException("Duplicate id or name exists in the accounts to update");
    }
    return res;
  }

  /**
   * Calls {@link AccountService#updateAccounts(Collection)} to create/update {@link Account}s.
   * @param accounts The {@link Account}s to create/update through the {@link #accountService}.
   * @return {@code true} if the operation is successful, {@code false} otherwise.
   */
  private boolean update(Collection<Account> accounts) {
    return accountService.updateAccounts(accounts);
  }
}
