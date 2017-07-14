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

import com.github.ambry.tools.util.ToolUtils;
import java.util.ArrayList;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


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
    long startTime = System.currentTimeMillis();
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
        .ofType(Integer.class);

    ArgumentAcceptingOptionSpec<Integer> zkSessionTimeoutMsOpt = parser.accepts("zkSessionTimeout",
        "Optional timeout in millisecond for session to the ZooKeeper server. This option is not required, "
            + "and the default value is 20000.")
        .withRequiredArg()
        .describedAs("zk_session_timeout")
        .ofType(Integer.class);

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
    ArrayList<OptionSpec> listOpt = new ArrayList<>();
    listOpt.add(accountJsonFilePathOpt);
    listOpt.add(zkServerOpt);
    try {
      ToolUtils.ensureOrExit(listOpt, options, parser);
      int numOfAccountsSuccessfullyUpdated =
          AccountUpdater.createOrUpdate(accountJsonFilePath, zkServer, storePath, zkConnectionTimeoutMs,
              zkSessionTimeoutMs);
      if (numOfAccountsSuccessfullyUpdated == -1) {
        throw new Exception("Update failed with unknown reason.");
      } else {
        System.out.println(
            numOfAccountsSuccessfullyUpdated + " accounts have bee successfully created or updated, took " + (
                System.currentTimeMillis() - startTime) + " ms");
      }
    } catch (Exception e) {
      System.err.println("Operation is failed with exception: " + e);
    }
  }
}
