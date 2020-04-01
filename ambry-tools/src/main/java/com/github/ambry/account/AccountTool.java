/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * <p>
 *   This is a command-line tool can be used to perform a number of {@link Account} related operations.
 *   By change the action parameter you can do
 *   <ul>
 *     <li>list: list all the available versions in account service</li>
 *     <li>view: print out the {@link Account} metadata for a specific version</li>
 *     <li>update: overwrite {@link Account} metadata in the account service by creating a new version</li>
 *     <li>rollback: rollback to a previous version by fetch the data out and creating a new version</li>
 *   </ul>
 *   When a update/rollback operation is successful, it will publish the message through a {@link com.github.ambry.commons.Notifier},
 *   so that all the entities that are listening to the metadata change can get notified.
 * </p>
 * <p>
 *   This tool requires three mandatory options:<ul>
 *     <li> the address of the {@code ZooKeeper} server to store account metadata; and
 *     <li> the path on the {@code ZooKeeper} that will be used as the root path for account metadata storage and notifications.
 *     <li> the path to a json file that contains a object of "zkInfo", the value of an array of {@link com.github.ambry.clustermap.ClusterMapUtils.DcZkInfo}</li>
 *     <li> the dc name</li>
 *     <li> the cluster name that {@link com.github.ambry.clustermap.HelixClusterManager} would recognize</li>
 *   </ul>
 * </p>
 * <p>
 *   A sample usage of the tool is:
 *   <code>
 *     java -Dlog4j.configuration=file:./log4j.properties -cp ambry.jar com.github.ambry.account.AccountTool
 *     --zkServer localhost:2181 --storePath /ambry/test/helixPropertyStore --zkLayoutPath ./zklayout.json --clustername cluster1 --dcname dc1
 *   </code>
 * </p>
 * <p>
 *   The file for dc zkinfo looks like this
 *   <pre>
 *     {
 *     "zkInfo": [
 *         {
 *             "datacenter": "dc1",
 *             "id": 1,
 *             "zkConnectStr": "zk-dc1.example.com:2181"
 *         }
 *     ]
 *    }
 *   </pre>
 *   The dcname parameter should be one of the "datacenter" in the list.
 * </p>
 *
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
public class AccountTool {
  private final MetricRegistry registry;
  private final BackupFileManager backupFileManager;
  private final VerifiableProperties verifiableProperties;
  private final HelixPropertyStoreConfig storeConfig;
  private final HelixAccountServiceConfig accountServiceConfig;
  private final HelixPropertyStore<ZNRecord> helixStore;

  private RouterStore routerStore = null;
  private Router router = null;
  private ClusterMap clusterMap = null;
  private HelixAccountService accountService = null;

  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 5000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  private static final String DEFAULT_HOSTNAME = "localhost";

  private String clusterName = null;
  private String hostname = null;
  private String dcName = null;
  private String clusterMapDcsZkConnectString = null;

  private String hardwareLayout;
  private String partitionLayout;

  private enum Action {
    /**
     * List all the versions available in the account service.
     */
    LIST,

    /**
     * View the whole account metadata at a specific version.
     */
    VIEW,

    /**
     * Rollback the account metadata to a previous version.
     */
    ROLLBACK,

    /**
     * Update the account metadata by overwriting it with the content provided by a file.
     */
    UPDATE
  }

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> actionOpt =
        parser.accepts("action", "The choice of the action only includes list,view,rollback,update")
            .withRequiredArg()
            .describedAs("action")
            .ofType(String.class);

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

    ArgumentAcceptingOptionSpec<String> zkLayoutPathOpt = parser.accepts("zkLayoutPath",
        "The path to the json file containing zookeeper connect info. This should be of the following form: \n{\n"
            + "  \"zkInfo\" : [\n" + "     {\n" + "       \"datacenter\":\"dc1\",\n"
            + "       \"zkConnectStr\":\"abc.example.com:2199\",\n" + "     },\n" + "     {\n"
            + "       \"datacenter\":\"dc2\",\n" + "       \"zkConnectStr\":\"def.example.com:2300\",\n" + "     },\n"
            + "     {\n" + "       \"datacenter\":\"dc3\",\n" + "       \"zkConnectStr\":\"ghi.example.com:2400\",\n"
            + "     }\n" + "  ]\n" + "}").
        withRequiredArg().
        describedAs("zk_connect_info_path").
        ofType(String.class);

    ArgumentAcceptingOptionSpec<String> clusterNameOpt =
        parser.accepts("clustername", "Cluster name of current machine.")
            .withRequiredArg()
            .describedAs("cluster name")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> hostnameOpt = parser.accepts("hostname",
        "Optional hostname of current machine. The option is not required and will defaulted to localhost")
        .withRequiredArg()
        .describedAs("hostname")
        .ofType(String.class)
        .defaultsTo(DEFAULT_HOSTNAME);

    ArgumentAcceptingOptionSpec<String> dcnameOpt = parser.accepts("dcname",
        "Optional dc name of current machine. The option is not required and will defaulted to \"dc\"")
        .withRequiredArg()
        .describedAs("dc name")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<Integer> versionOpt = parser.accepts("version",
        "Required when the action is rollback or view. When the action is rollback, this tool would rollback the account metadata"
            + " to the given version and create a new version on top of the latest version. When the action is view, this tool will print out"
            + " the account metadata for the given version")
        .withRequiredArg()
        .describedAs("version")
        .ofType(Integer.class);

    ArgumentAcceptingOptionSpec<String> accountJsonPathOpt = parser.accepts("accountJsonPath",
        "The path to the account json file. The json file must be in the form of a json array, with each"
            + "entry to be an account in its json form.")
        .withRequiredArg()
        .describedAs("account_json_path")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt = parser.accepts("hardwareLayout",
        "The hardware layout json file. This is optional if you provide a zklayout file.")
        .withRequiredArg()
        .describedAs("hardware_layout")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> partitionLayoutOpt = parser.accepts("partitionLayout",
        "The partition layout json file. This is optional if you provide a zklayout file.")
        .withRequiredArg()
        .describedAs("partition_layout")
        .ofType(String.class);

    parser.accepts("help", "print this help message.");
    parser.accepts("h", "print this help message.");

    OptionSet options = parser.parse(args);
    if (options.has("help") || options.has("h")) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    ToolUtils.ensureOrExit(
        Arrays.asList(actionOpt, zkServerOpt, storePathOpt, zkLayoutPathOpt, dcnameOpt, clusterNameOpt), options,
        parser);
    String action = options.valueOf(actionOpt);
    Action act = null;
    try {
      act = Enum.valueOf(Action.class, action.toUpperCase());
    } catch (Exception e) {
      System.out.println("Invalid action: " + action);
      parser.printHelpOn(System.out);
      System.exit(0);
    }
    String zkServer = options.valueOf(zkServerOpt);
    String storePath = options.valueOf(storePathOpt);
    String hostname = options.valueOf(hostnameOpt);
    String dcname = options.valueOf(dcnameOpt);
    Integer zkConnectionTimeoutMs = options.valueOf(zkConnectionTimeoutMsOpt);
    Integer zkSessionTimeoutMs = options.valueOf(zkSessionTimeoutMsOpt);
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    String clusterMapDcsZkConnectString = Utils.readStringFromFile(zkLayoutPath);
    String clusterName = options.valueOf(clusterNameOpt);
    String hardwareLayout = options.valueOf(hardwareLayoutOpt);
    String partitionLayout = options.valueOf(partitionLayoutOpt);

    if ((zkLayoutPath != null && clusterName == null) || (zkLayoutPath == null && clusterName != null)) {
      System.out.println("You have to provide clustername and zklayout at the same time");
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    if (zkLayoutPath == null) {
      if ((hardwareLayout != null && partitionLayout == null) || (hardwareLayout == null && partitionLayout != null)) {
        System.out.println("You have to provide hardwareLayout and partitionLayout at the same time");
        parser.printHelpOn(System.out);
        System.exit(0);
      }
    }

    int version = 0;
    if (act == Action.VIEW || act == Action.ROLLBACK) {
      ToolUtils.ensureOrExit(Arrays.asList(versionOpt), options, parser);
      version = options.valueOf(versionOpt);
    }

    String accountJsonPath = null;
    if (act == Action.UPDATE) {
      ToolUtils.ensureOrExit(Arrays.asList(accountJsonPathOpt), options, parser);
      accountJsonPath = options.valueOf(accountJsonPathOpt);
    }

    AccountTool accountTool =
        new AccountTool(zkServer, storePath, zkConnectionTimeoutMs, zkSessionTimeoutMs, hostname, dcname, clusterName,
            clusterMapDcsZkConnectString, hardwareLayout, partitionLayout);
    try {
      boolean succeeded = false;
      switch (act) {
        case LIST:
          List<Integer> versions = accountTool.listVersions();
          if (versions != null) {
            System.out.println("The versions are " + versions);
          } else {
            System.out.println("Some error occurs.");
          }
          break;
        case VIEW:
          Collection<String> accounts = accountTool.viewAccountMetadata(version);
          if (accounts != null) {
            System.out.println("The account metadata at version " + version + " are " + accounts);
          } else {
            System.out.println("Some error occurs");
          }
          break;
        case ROLLBACK:
          succeeded = accountTool.rollback(version);
          if (succeeded) {
            System.out.println("Successfully rollback account metadata to version " + version);
          } else {
            System.out.println("Fail to rollback account metadata to version " + version);
          }
          break;
        case UPDATE:
          succeeded = accountTool.updateFromFile(accountJsonPath);
          if (succeeded) {
            System.out.println("Successfully update account metadata from file " + accountJsonPath);
          } else {
            System.out.println("Fail to update account metadata from file " + accountJsonPath);
          }
          break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      accountTool.close();
      System.exit(0);
    }
  }

  /**
   * Constructor to create {@link AccountTool}.
   * @param zkServer The address to zookeeper server.
   * @param storePath The root path {@link HelixPropertyStore}.
   * @param zkConnectionTimeoutMs The connection timeout to {@link HelixPropertyStore}.
   * @param zkSessionTimeoutMs The session timeout to {@link HelixPropertyStore}.
   * @param hostname The hostname of the current host.
   * @param dcname The dc name of the current host.
   * @param clusterName The cluster name of the current host. This will be used to figure out the hardware layout.
   * @param clusterMapDcsZkConnectString The zk info in different dc in json format.
   * @param hardwareLayout The filepath to hardware layout. This is optional if clusterMapDcsZkConnectString is not empty.
   * @param partitionLayout The filepath to partition layout. This is optional if clusterMapDcsZkConnectString is not empty.
   * @throws Exception Any unexpected exception.
   */
  public AccountTool(String zkServer, String storePath, int zkConnectionTimeoutMs, int zkSessionTimeoutMs,
      String hostname, String dcname, String clusterName, String clusterMapDcsZkConnectString, String hardwareLayout,
      String partitionLayout) throws Exception {
    this.hostname = hostname;
    this.dcName = dcname;
    this.clusterName = clusterName;
    this.clusterMapDcsZkConnectString = clusterMapDcsZkConnectString;
    this.hardwareLayout = hardwareLayout;
    this.partitionLayout = partitionLayout;
    verifiableProperties = getVerifiableProperties(zkServer, storePath, zkConnectionTimeoutMs, zkSessionTimeoutMs);
    accountServiceConfig = new HelixAccountServiceConfig(verifiableProperties);
    storeConfig = new HelixPropertyStoreConfig(verifiableProperties);
    registry = new MetricRegistry();
    backupFileManager = new BackupFileManager(new AccountServiceMetrics(registry), accountServiceConfig);
    helixStore = CommonUtils.createHelixPropertyStore(accountServiceConfig.zkClientConnectString, storeConfig, null);
    routerStore = getRouterStore();
  }

  /**
   * Return a list of available version numbers in the account service. Or null whenever there is an error.
   * @return List of version number in the account service.
   */
  public List<Integer> listVersions() {
    return routerStore.getAllVersions();
  }

  /**
   * Fetch the {@link Account} metadata at given version and return their json format as a collection of String.
   * Or null whenever there is an error.
   * @param version The version to fetch the {@link Account} metadata.
   * @return A collection of json string of {@link Account} metadata.
   */
  public Collection<String> viewAccountMetadata(int version) {
    Map<String, String> accountMetadata = routerStore.fetchAccountMetadataAtVersion(version);
    if (accountMetadata != null) {
      return accountMetadata.values();
    } else {
      return null;
    }
  }

  /**
   * Overwrite the current {@link Account} metadata with the given {@link Collection} and publish the new {@link Account}
   * metadata if the update succeeds.
   * @param accounts The new {@link Account} metadata.
   * @return True if the update succeeds.
   */
  private boolean updateAccounts(Collection<Account> accounts) {
    Collection<Account> existingAccounts = accountService.getAllAccounts();
    Map<Short, Account> existingAccountsMap = new HashMap<>();
    for (Account account : existingAccounts) {
      existingAccountsMap.put(account.getId(), account);
    }
    Collection<Account> newAccounts = new ArrayList<>(accounts.size());
    // resolve the snapshot conflict.
    for (Account account : accounts) {
      Account accountInMap = existingAccountsMap.get(account.getId());
      if (accountInMap != null && accountInMap.getSnapshotVersion() != account.getSnapshotVersion()) {
        newAccounts.add(new AccountBuilder(account).snapshotVersion(accountInMap.getSnapshotVersion()).build());
      } else {
        newAccounts.add(account);
      }
    }
    return accountService.updateAccountsWithAccountMetadataStore(newAccounts, routerStore);
  }

  /**
   * Overwrite the {@link Account} metadata in the account service with the one in the given file. It reads the {@link Account}
   * metadata from the given file, as an array of json object, and overwrites the current {@link Account} metadata with it,
   * by creating a new version on top of the latest version. For instance, if the current version is 10, and you update from
   * a file, then a new version 11 will be created with teh same content in the file.
   * It will publish the new {@link Account} metadata to all the listener if the rollback operation succeeds.
   * @param accountJsonPath The file to read the {@link Account} metadata from.
   * @return True if the update operation succeeds.
   * @throws Exception Andy unexpected error.
   */
  public boolean updateFromFile(String accountJsonPath) throws Exception {
    return updateAccounts(getAccountsFromJson(accountJsonPath));
  }

  /**
   * Roll the {@link Account} metadata back to a previous version. It fetches the {@link Account} metadata at the given
   * version and overwrites the current {@link Account} metadata with it, by creating a new version on top of the latest
   * version. For instance, if the current version is 10 and you rollback to version 8, then a new version 11 will be created
   * with the same content of version 8.
   * It will publish the new {@link Account} metadata to all the listener if the rollback operation succeeds.
   * @param version The version to roll back to.
   * @return True if the rollback operation succeeds.
   * @throws Exception Andy unexpected error.
   */
  public boolean rollback(int version) throws Exception {
    Collection<String> accountJsons = viewAccountMetadata(version);
    Collection<Account> accounts = new ArrayList<>();
    for (String accountJson : accountJsons) {
      accounts.add(Account.fromJson(new JSONObject(accountJson)));
    }
    return updateAccounts(accounts);
  }

  /**
   * Close and stop all the running components.
   * @throws Exception Any unexpected error.
   */
  public void close() throws Exception {
    if (accountService != null) {
      accountService.close();
    }
    if (clusterMap != null) {
      clusterMap.close();
    }
    if (helixStore != null) {
      helixStore.stop();
    }
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

  /**
   * Setting proper properties for all the components to be created.
   * @param zkServer The address to zookeeper server.
   * @param storePath The root path for {@link HelixPropertyStore}.
   * @param zkConnectionTimeoutMs The connection timeout for {@link HelixPropertyStore}.
   * @param zkSessionTimeoutMs The session timeout for {@link HelixPropertyStore}.
   * @return {@link VerifiableProperties}.
   */
  private VerifiableProperties getVerifiableProperties(String zkServer, String storePath, int zkConnectionTimeoutMs,
      int zkSessionTimeoutMs) {
    Properties properties = new Properties();
    // for creating HelixAccountService
    properties.setProperty(HelixPropertyStoreConfig.HELIX_ZK_CLIENT_CONNECTION_TIMEOUT_MS,
        String.valueOf(zkConnectionTimeoutMs));
    properties.setProperty(HelixPropertyStoreConfig.HELIX_ZK_CLIENT_SESSION_TIMEOUT_MS,
        String.valueOf(zkSessionTimeoutMs));
    properties.setProperty(HelixPropertyStoreConfig.HELIX_ROOT_PATH, storePath);
    properties.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY, zkServer);

    // for creating NonBlockingRouter
    if (clusterMapDcsZkConnectString != null) {
      properties.setProperty("clustermap.clusteragents.factory", "com.github.ambry.clustermap.HelixClusterAgentsFactory");
      properties.setProperty("clustermap.dcs.zk.connect.strings", clusterMapDcsZkConnectString);
      properties.setProperty("clustermap.cluster.name", clusterName);
    } else {
      properties.setProperty("clustermap.clusteragents.factory", "com.github.ambry.clustermap.StaticClusterAgentsFactory");
    }
    properties.setProperty("clustermap.host.name", hostname);
    properties.setProperty("clustermap.datacenter.name", dcName);
    properties.setProperty("router.hostname", hostname);
    properties.setProperty("router.datacenter.name", dcName);
    properties.setProperty("kms.default.container.key",
        "B375A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");
    return new VerifiableProperties(properties);
  }

  /**
   * Create a {@link RouterStore}. This router store will overwrite the account metadata whene updating
   * the account metadata.
   * @return {@link RouterStore}.
   * @throws Exception Any unexpected exception.
   */
  private RouterStore getRouterStore() throws Exception {
    return new RouterStore(new AccountServiceMetrics(registry), backupFileManager, helixStore,
        new AtomicReference<>(getRouter()), true, 100);
  }

  /**
   * Create a {@link Router} for {@link RouterStore}.
   * @return The {@link Router}.
   * @throws Exception Any unexpected exception.
   */
  private Router getRouter() throws Exception {
    // Create a HelixAccountService for the router.
    AccountServiceFactory accountServiceFactory =
        Utils.getObj("com.github.ambry.account.HelixAccountServiceFactory", verifiableProperties, registry);
    accountService = (HelixAccountService) accountServiceFactory.getAccountService();

    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterAgentsFactory clusterAgentsFactory =
        Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig, hardwareLayout,
            partitionLayout);
    clusterMap = clusterAgentsFactory.getClusterMap();
    SSLFactory sslFactory = getSSLFactoryIfRequired();
    // Create a NonBlockingRouter.
    RouterFactory routerFactory =
        Utils.getObj("com.github.ambry.router.NonBlockingRouterFactory", verifiableProperties, clusterMap,
            new LoggingNotificationSystem(), sslFactory, accountService);
    router = routerFactory.getRouter();
    return router;
  }

  /**
   * Return a {@link SSLFactory} if the ssl is required when connecting to ambry-server.
   * @return {@link SSLFactory}.
   * @throws Exception Any unexpected exception
   */
  private SSLFactory getSSLFactoryIfRequired() throws Exception {
    boolean sslRequired = new NettyConfig(verifiableProperties).nettyServerEnableSSL
        || new ClusterMapConfig(verifiableProperties).clusterMapSslEnabledDatacenters.length() > 0;
    return sslRequired ? SSLFactory.getNewInstance(new SSLConfig(verifiableProperties)) : null;
  }
}
