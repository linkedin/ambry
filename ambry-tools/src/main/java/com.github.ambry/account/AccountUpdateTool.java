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
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
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

  /**
   * @param args takes in three mandatory arguments: the path of the json file for the accounts to create/update,
   *             the address of the {@code ZooKeeper} server, and the root path for the {@link org.apache.helix.store.HelixPropertyStore}
   *             that will be used for storing account metadata and notifications.
   *
   *             Also takes in an optional argument that specifies the timeout in millisecond to connect to the
   *             {@code ZooKeeper} server (default value is 5000), and the timeout in millisecond to keep a session
   *             to the {@code ZooKeeper} (default value is 20000).
   */
  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> accountJsonFilePathOpt = parser.accepts("accountJsonPath",
        "The path to the account json file. The json file must be in the form of a json array, with each"
            + "entry to be an account in its json form.")
        .withRequiredArg()
        .describedAs("account_json_file_path")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> zkServerOpt =
        parser.accepts("zkServer", "The address of ZooKeeper server. This option is required.")
            .withRequiredArg()
            .describedAs("zk_server")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> storePathOpt = parser.accepts("storePath",
        "The root path of helix property store in the ZooKeeper. Must start with /, and must not end "
            + "with /. It is recommended to make root path in the form of /ambry/<clustername>/helixPropertyStore. This option is required.")
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

    parser.accepts("help", "print this help message.");

    parser.accepts("h", "print this help message.");

    OptionSet options = parser.parse(args);
    if (options.has("help") || options.has("h")) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }
    String accountJsonFilePath = options.valueOf(accountJsonFilePathOpt);
    String storePath = options.valueOf(storePathOpt);
    String zkServer = options.valueOf(zkServerOpt);
    Integer zkConnectionTimeoutMs = options.valueOf(zkConnectionTimeoutMsOpt);
    Integer zkSessionTimeoutMs = options.valueOf(zkSessionTimeoutMsOpt);
    Short containerJsonVersion = options.valueOf(containerJsonVersionOpt);
    ArrayList<OptionSpec> listOpt = new ArrayList<>();
    listOpt.add(accountJsonFilePathOpt);
    listOpt.add(zkServerOpt);
    ToolUtils.ensureOrExit(listOpt, options, parser);
    try {
      updateAccount(accountJsonFilePath, zkServer, storePath, zkConnectionTimeoutMs, zkSessionTimeoutMs,
          containerJsonVersion);
    } catch (Exception e) {
      System.err.println("Updating accounts failed with exception: " + e);
      e.printStackTrace();
    }
  }

  /**
   * Performs the updating accounts operation.
   * @param accountJsonFilePath The path to the json file.
   * @param zkServer The {@code ZooKeeper} server address to connect.
   * @param storePath The root path on the {@code ZooKeeper} for account data.
   * @param zkConnectionTimeoutMs The connection timeout in millisecond for connecting {@code ZooKeeper} server.
   * @param zkSessionTimeoutMs The session timeout in millisecond for connecting {@code ZooKeeper} server.
   * @param containerJsonVersion The {@link Container} JSON version to write in.
   * @throws Exception
   */
  static void updateAccount(String accountJsonFilePath, String zkServer, String storePath, int zkConnectionTimeoutMs,
      int zkSessionTimeoutMs, short containerJsonVersion) throws Exception {
    Container.setCurrentJsonVersion(containerJsonVersion);
    long startTime = System.currentTimeMillis();
    Collection<Account> accountsToUpdate = getAccountsFromJson(accountJsonFilePath);
    if (!hasDuplicateAccountIdOrName(accountsToUpdate)) {
      AccountService accountService =
          getHelixAccountService(zkServer, storePath, zkConnectionTimeoutMs, zkSessionTimeoutMs);
      if (accountService.updateAccounts(accountsToUpdate)) {
        System.out.println(accountsToUpdate.size() + " accounts have been successfully created or updated, took " + (
            System.currentTimeMillis() - startTime) + " ms");
      } else {
        throw new Exception("Updating accounts failed with unknown reason.");
      }
    } else {
      throw new IllegalArgumentException("Duplicate id or name exists in the accounts to update");
    }
  }

  /**
   * Constructor.
   * @param zkServer The {@code ZooKeeper} server address to connect.
   * @param storePath The path for {@link org.apache.helix.store.HelixPropertyStore}, which will be used as the
   *                  root path for both {@link HelixAccountService} and {@link HelixNotifier}.
   * @param zkConnectionTimeoutMs The timeout in millisecond to connect to the {@code ZooKeeper} server.
   * @param zkSessionTimeoutMs The timeout in millisecond for a session to the {@code ZooKeeper} server.
   */
  private static AccountService getHelixAccountService(String zkServer, String storePath, int zkConnectionTimeoutMs,
      int zkSessionTimeoutMs) {
    Properties helixConfigProps = new Properties();
    helixConfigProps.setProperty(
        HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms",
        String.valueOf(zkConnectionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms",
        String.valueOf(zkSessionTimeoutMs));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connect.string",
        zkServer);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path", storePath);
    VerifiableProperties vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    HelixPropertyStoreConfig storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    Notifier notifier = new HelixNotifier(storeConfig);
    return new HelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier).getAccountService();
  }

  /**
   * Gets a collection of {@link Account}s from a json file.
   * @param accountJsonFilePath The path to the json file.
   * @return The collection of {@link Account}s parsed from the given json file.
   * @throws IOException
   * @throws JSONException
   */
  private static Collection<Account> getAccountsFromJson(String accountJsonFilePath) throws IOException, JSONException {
    JSONArray accountArray = new JSONArray(Utils.readStringFromFile(accountJsonFilePath));
    Collection<Account> accounts = new ArrayList<>();
    for (int i = 0; i < accountArray.length(); i++) {
      JSONObject accountJson = accountArray.getJSONObject(i);
      accounts.add(Account.fromJson(accountJson));
    }
    return accounts;
  }
}
