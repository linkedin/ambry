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
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static com.github.ambry.account.AccountUtils.*;


/**
 * <p>
 *   This is a command-line tool can be used to create/update a number of {@link Account}s on {@code ZooKeeper} node.
 *   It employs an {@link AccountService} component to perform the operations, and it follows the same policy defined
 *   by {@link AccountService} to resolve conflict or exception. When a create/update operation is successful, it
 *   will publish the message through a {@link com.github.ambry.commons.Notifier}, so that all the entities that
 *   are listening to the metadata change can get notified.
 * </p>
 * <p>
 *   This tool requires three mandatory options:<ul>
 *     <li> the path to a json file that contains a {@link org.json.JSONArray} of {@link Account}s in json to create or update;
 *     <li> the address of the {@code ZooKeeper} server to store account metadata; and
 *     <li> the path on the {@code ZooKeeper} that will be used as the root path for account metadata storage and notifications.
 *   </ul>
 * </p>
 * <p>
 *   A sample usage of the tool is:
 *   <code>
 *     java -Dlog4j.configuration=file:../config/log4j.properties -cp ambry.jar com.github.ambry.account.AccountUpdateTool
 *     --zkServer localhost:2181 --storePath /ambry/test/helixPropertyStore --accountJsonPath ./refAccounts.json
 *   </code>
 * </p>
 * <p>
 *   The file for the accounts to create/update contains a {@link org.json.JSONArray} of account in its json form. Invalid
 *   records will fail the operation. For example the file is in the following format:
 *   <pre>
 *     [
 *      {
 *        "accountId": 1,
 *        "accountName": "account1",
 *        "containers": [
 *          {
 *            "parentAccountId": 1,
 *            "containerName": "container1",
 *            "description": "This is the first container of account1",
 *            "isPrivate": false,
 *            "containerId": 1,
 *            "version": 1,
 *            "status": "ACTIVE"
 *          }
 *        ],
 *        "version": 1,
 *        "status": "ACTIVE"
 *      },
 *      {
 *        "accountId": 2,
 *        "accountName": "account1",
 *        "containers": [
 *          {
 *            "parentAccountId": 2,
 *            "containerName": "container1",
 *            "description": "This is the first container of ",
 *            "isPrivate": true,
 *            "containerId": 1,
 *            "version": 1,
 *            "status": "INACTIVE"
 *          }
 *        ],
 *        "version": 1,
 *        "status": "ACTIVE"
 *      }
 *     ]
 *   </pre>
 * </p>
 */
public class AccountUpdateTool {
  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 5000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  private static final String DEFAULT_LOCAL_BACKUP_DIR = "/tmp/account-update-tool-backups";

  private final AccountService accountService;

  /**
   * @param args takes in three mandatory arguments: the path of the json file for the accounts to create/update,
   *             the address of the {@code ZooKeeper} server, and the root path for the
   *             {@link org.apache.helix.store.HelixPropertyStore} that will be used for storing account metadata and
   *             notifications.
   *
   *             Also takes in an optional argument that specifies the timeout in millisecond to connect to the
   *             {@code ZooKeeper} server (default value is 5000), and the timeout in millisecond to keep a session
   *             to the {@code ZooKeeper} (default value is 20000).
   */
  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> accountJsonPathOpt = parser.accepts("accountJsonPath",
        "The path to the account json file. The json file must be in the form of a json array, with each"
            + "entry to be an account in its json form.")
        .withRequiredArg()
        .describedAs("account_json_path")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> accountsToEditOpt = parser.accepts("accountsToEdit",
        "The names of the accounts to edit, separated by commas. "
            + "The script will open an editor where you can edit the json for these accounts.")
        .withRequiredArg()
        .describedAs("accounts_to_edit")
        .withValuesSeparatedBy(",");

    ArgumentAcceptingOptionSpec<String> zkServerOpt =
        parser.accepts("zkServer", "The address of ZooKeeper server. This option is required.")
            .withRequiredArg()
            .describedAs("zk_server")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> storePathOpt = parser.accepts("storePath",
        "The root path of helix property store in the ZooKeeper. "
            + "Must start with /, and must not end with /. It is recommended to make root path in the form of "
            + "/ambry/<clustername>/helixPropertyStore. This option is required.")
        .withRequiredArg()
        .describedAs("helix_store_path")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<Integer> zkConnectionTimeoutMsOpt = parser.accepts("zkConnectionTimeout",
        "Optional timeout in millisecond for connecting to the ZooKeeper server. This option is not required, "
            + "and the default value is 5000.")
        .withRequiredArg()
        .describedAs("zk_connection_timeout")
        .ofType(Integer.class)
        .defaultsTo(ZK_CLIENT_CONNECTION_TIMEOUT_MS);

    ArgumentAcceptingOptionSpec<Integer> zkSessionTimeoutMsOpt = parser.accepts("zkSessionTimeout",
        "Optional timeout in millisecond for session to the ZooKeeper server. This option is not required, "
            + "and the default value is 20000.")
        .withRequiredArg()
        .describedAs("zk_session_timeout")
        .ofType(Integer.class)
        .defaultsTo(ZK_CLIENT_SESSION_TIMEOUT_MS);

    ArgumentAcceptingOptionSpec<Short> containerJsonVersionOpt = parser.accepts("containerSchemaVersion",
        "Optional override for the container JSON version to write in when doing an account update.")
        .withRequiredArg()
        .describedAs("container_json_version")
        .ofType(Short.class)
        .defaultsTo(Container.getCurrentJsonVersion());

    ArgumentAcceptingOptionSpec<String> backupDirOpt =
        parser.accepts("backupDir", "Optional local backup directory path. Defaults to " + DEFAULT_LOCAL_BACKUP_DIR)
            .withRequiredArg()
            .describedAs("backup_dir")
            .ofType(String.class)
            .defaultsTo(DEFAULT_LOCAL_BACKUP_DIR);

    parser.accepts("ignoreSnapshotVersion", "Ignore the snapshot version conflict. Defaults to false");
    parser.accepts("help", "print this help message.");
    parser.accepts("h", "print this help message.");

    OptionSet options = parser.parse(args);
    if (options.has("help") || options.has("h")) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }
    ToolUtils.ensureOrExit(Arrays.asList(zkServerOpt, storePathOpt), options, parser);
    String storePath = options.valueOf(storePathOpt);
    String zkServer = options.valueOf(zkServerOpt);
    String backupDir = options.valueOf(backupDirOpt);
    Integer zkConnectionTimeoutMs = options.valueOf(zkConnectionTimeoutMsOpt);
    Integer zkSessionTimeoutMs = options.valueOf(zkSessionTimeoutMsOpt);
    Short containerJsonVersion = options.valueOf(containerJsonVersionOpt);
    String accountJsonPath = options.valueOf(accountJsonPathOpt);
    List<String> accountsToEdit = options.valuesOf(accountsToEditOpt);
    boolean ignoreSnapshotVersion = options.has("ignoreSnapshotVersion");
    if (!((accountJsonPath == null) ^ (accountsToEdit.isEmpty()))) {
      System.err.println("Must provide exactly one of --accountJsonPath or --accountsToEdit");
      parser.printHelpOn(System.err);
      System.exit(1);
    }
    try (AccountService accountService = getHelixAccountService(zkServer, storePath, backupDir, zkConnectionTimeoutMs,
        zkSessionTimeoutMs)) {
      AccountUpdateTool accountUpdateTool = new AccountUpdateTool(accountService, containerJsonVersion);
      if (accountJsonPath != null) {
        accountUpdateTool.updateAccountsFromFile(accountJsonPath, ignoreSnapshotVersion);
      } else {
        accountUpdateTool.editAccounts(accountsToEdit, ignoreSnapshotVersion);
      }
    } catch (Exception e) {
      System.err.println("Updating accounts failed with exception: " + e);
      e.printStackTrace();
    }
  }

  /**
   * Constructor.
   * @param accountService The {@link AccountService} to use.
   * @param containerJsonVersion The {@link Container} JSON version to write in.
   * @throws Exception
   */
  AccountUpdateTool(AccountService accountService, short containerJsonVersion) {
    Container.setCurrentJsonVersion(containerJsonVersion);
    this.accountService = accountService;
  }

  /**
   * Edit accounts in a text editor and upload them to zookeeper.
   * @param accountNames the name of the accounts to edit.
   * @param ignoreSnapshotVersion if {@code true}, don't verify the snapshot version.
   * @throws IOException
   * @throws InterruptedException
   */
  void editAccounts(Collection<String> accountNames, boolean ignoreSnapshotVersion)
      throws IOException, InterruptedException {
    Path accountJsonPath = Files.createTempFile("account-update-", ".json");
    JSONArray accountsToEdit = new JSONArray();
    accountNames.stream()
        .map(accountName -> Optional.ofNullable(accountService.getAccountByName(accountName))
            .orElseThrow(() -> new IllegalArgumentException("Could not find account: " + accountName))
            .toJson(false))
        .forEach(accountsToEdit::put);
    try (BufferedWriter writer = Files.newBufferedWriter(accountJsonPath)) {
      accountsToEdit.write(writer, 2, 0);
    }
    ToolUtils.editFile(accountJsonPath);
    System.out.println("The following account metadata will be uploaded:");
    try (Stream<String> lines = Files.lines(accountJsonPath)) {
      lines.forEach(System.out::println);
    }
    if (ToolUtils.yesNoPrompt("Do you want to update these accounts?")) {
      updateAccountsFromFile(accountJsonPath.toAbsolutePath().toString(), ignoreSnapshotVersion);
    } else {
      System.out.println("Not updating any accounts");
    }
    Files.delete(accountJsonPath);
  }

  /**
   * Update accounts from a file containing a json array of account metadata.
   * @param accountJsonPath the path to the file containing the account metadata to upload.
   * @param ignoreSnapshotVersion if {@code true}, don't verify the snapshot version.
   * @throws IOException
   */
  void updateAccountsFromFile(String accountJsonPath, boolean ignoreSnapshotVersion) throws IOException {
    long startTime = System.currentTimeMillis();
    Collection<Account> accountsToUpdate = getAccountsFromJson(accountJsonPath);
    if (!hasDuplicateAccountIdOrName(accountsToUpdate)) {
      if (ignoreSnapshotVersion) {
        Collection<Account> allAccounts = accountService.getAllAccounts();
        // Update the snapshot version to resolve conflict
        Map<Short, Account> existingAccountsMap = new HashMap<>();
        for (Account account : allAccounts) {
          existingAccountsMap.put(account.getId(), account);
        }
        Collection<Account> newAccounts = new ArrayList<>(accountsToUpdate.size());
        // resolve the snapshot conflict.
        for (Account account : accountsToUpdate) {
          Account accountInMap = existingAccountsMap.get(account.getId());
          if (accountInMap != null && accountInMap.getSnapshotVersion() != account.getSnapshotVersion()) {
            newAccounts.add(new AccountBuilder(account).snapshotVersion(accountInMap.getSnapshotVersion()).build());
          } else {
            newAccounts.add(account);
          }
        }
        accountsToUpdate = newAccounts;
      }
      if (accountService.updateAccounts(accountsToUpdate)) {
        System.out.println(
            accountsToUpdate.size() + " account(s) successfully created or updated, took " + (System.currentTimeMillis()
                - startTime) + " ms");
      } else {
        throw new RuntimeException("Updating accounts failed. See log for details.");
      }
    } else {
      throw new IllegalArgumentException("Duplicate id or name exists in the accounts to update");
    }
  }

  /**
   * Method to create instances of {@link HelixAccountService}.
   * @param zkServer The {@code ZooKeeper} server address to connect.
   * @param storePath The root path on the {@code ZooKeeper} for account data.
   * @param backupDir The path to the local backup directory.
   * @param zkConnectionTimeoutMs The connection timeout in millisecond for connecting {@code ZooKeeper} server.
   * @param zkSessionTimeoutMs The session timeout in millisecond for connecting {@code ZooKeeper} server.
   * @return the {@link HelixAccountService}.
   */
  static AccountService getHelixAccountService(String zkServer, String storePath, String backupDir,
      int zkConnectionTimeoutMs, int zkSessionTimeoutMs) {
    Properties helixConfigProps = new Properties();
    helixConfigProps.setProperty(
        HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms",
        String.valueOf(zkConnectionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms",
        String.valueOf(zkSessionTimeoutMs));
    helixConfigProps.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY, zkServer);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path", storePath);
    helixConfigProps.setProperty(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY, backupDir);
    VerifiableProperties vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    return new HelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry()).getAccountService();
  }

  /**
   * Gets a collection of {@link Account}s from a json file.
   * @param accountJsonPath The path to the json file.
   * @return The collection of {@link Account}s parsed from the given json file.
   * @throws IOException
   * @throws JSONException
   */
  private static Collection<Account> getAccountsFromJson(String accountJsonPath) throws IOException, JSONException {
    JSONArray accountArray = new JSONArray(Utils.readStringFromFile(accountJsonPath));
    Collection<Account> accounts = new ArrayList<>();
    for (int i = 0; i < accountArray.length(); i++) {
      JSONObject accountJson = accountArray.getJSONObject(i);
      accounts.add(Account.fromJson(accountJson));
    }
    return accounts;
  }
}
