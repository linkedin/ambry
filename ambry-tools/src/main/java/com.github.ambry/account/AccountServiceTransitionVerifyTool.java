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
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONObject;


/**
 * This is a tool to verify that {@link Account} metadata stored in the new zookeeper znode (and ambry-server) is the same as
 * the one in the old zookeeper znode.
 *
 * <p>
 *   This tool requires two mandatory options:
 *   <ul>
 *     <li> the address of the {@code ZooKeeper} server to store account metadata; </li>
 *     <li> the path on the {@code ZooKeeper} that will be used as the root path for the {@link Account} metadata storage and notification;</li>
 *   </ul>
 * </p>
 *
 * <p>
 *   A sample usage of the tool is:
 *   <code>
 *     java -Dlog4j.configuration=file:../config/log4j.properties -cp ambry.jar com.github.ambry.account.AccountServiceTransitionVerifyTool
 *     --zkServer localhost:2818 --storePath /ambry/test/helixPropertyStore --zkLayoutPath ./zkLayout.json
 *   </code>
 *   The command will show you the result of the comparison. And if the {@link Account} metadata are not the same from these two storage,
 *   it will also print out the different part.
 * </p>
 */
public class AccountServiceTransitionVerifyTool {
  private final MetricRegistry registry;
  private final BackupFileManager backupFileManager;
  private final VerifiableProperties verifiableProperties;
  private final HelixPropertyStoreConfig storeConfig;
  private final HelixAccountServiceConfig accountServiceConfig;
  private final HelixPropertyStore<ZNRecord> helixStore;

  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 5000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  private static final String DEFAULT_HOSTNAME = "localhost";
  private static final String DEFAULT_DCNAME = "dc";

  private String clusterName = null;
  private String hostname = null;
  private String dcName = null;
  private String clusterMapDcsZkConnectString = null;

  private Router router = null;
  private AccountService accountService = null;
  private ClusterMap clusterMap = null;

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

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

    ArgumentAcceptingOptionSpec<String> clusterNameOpt = parser.accepts("clustername",
        "Cluster name of current machine.")
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
        .ofType(String.class)
        .defaultsTo(DEFAULT_DCNAME);

    parser.accepts("help", "print this help message.");
    parser.accepts("h", "print this help message.");

    OptionSet options = parser.parse(args);
    if (options.has("help") || options.has("h")) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    ToolUtils.ensureOrExit(Arrays.asList(zkServerOpt, storePathOpt, zkLayoutPathOpt, clusterNameOpt), options, parser);
    String zkServer = options.valueOf(zkServerOpt);
    String storePath = options.valueOf(storePathOpt);
    String hostname = options.valueOf(hostnameOpt);
    String dcname = options.valueOf(dcnameOpt);
    Integer zkConnectionTimeoutMs = options.valueOf(zkConnectionTimeoutMsOpt);
    Integer zkSessionTimeoutMs = options.valueOf(zkSessionTimeoutMsOpt);
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    String clusterMapDcsZkConnectString = Utils.readStringFromFile(zkLayoutPath);
    String clusterName = options.valueOf(clusterNameOpt);

    AccountServiceTransitionVerifyTool verifyTool = new AccountServiceTransitionVerifyTool(zkServer, storePath, zkConnectionTimeoutMs,
        zkSessionTimeoutMs, hostname, dcname, clusterMapDcsZkConnectString, clusterName);
    if (verifyTool.fetchAndCompareLegacyWithRouter()) {
      System.out.println("The legacy account map and the router account map are the same");
    } else {
      System.out.println("The legacy account map and the router account map are the **NOT** same, something is wrong");
    }
    verifyTool.close();
  }

  /**
   * Constructor to create {@link AccountServiceTransitionVerifyTool}.
   * @param zkServer The address to zookeeper server.
   * @param storePath The root path {@link HelixPropertyStore}.
   * @param zkConnectionTimeoutMs The connection timeout to {@link HelixPropertyStore}.
   * @param zkSessionTimeoutMs The session timeout to {@link HelixPropertyStore}.
   * @param hostname The hostname of this machine.
   * @param dcName The DCName of this machine.
   * @param clusterMapDcsZkConnectString The DC zookeeper connection map in json format.
   * @param clusterName The clusterName in HelixClusterMap.
   * @throws Exception Any unexpected exception.
   */
  public AccountServiceTransitionVerifyTool(String zkServer, String storePath, int zkConnectionTimeoutMs, int zkSessionTimeoutMs,
      String hostname, String dcName, String clusterMapDcsZkConnectString, String clusterName) throws Exception {
    this.hostname = hostname;
    this.dcName = dcName;
    this.clusterMapDcsZkConnectString = clusterMapDcsZkConnectString;
    this.clusterName = clusterName;
    verifiableProperties = getVerifiableProperties(zkServer, storePath, zkConnectionTimeoutMs, zkSessionTimeoutMs);
    accountServiceConfig = new HelixAccountServiceConfig(verifiableProperties);
    storeConfig = new HelixPropertyStoreConfig(verifiableProperties);
    registry = new MetricRegistry();
    backupFileManager = new BackupFileManager(new AccountServiceMetrics(registry), accountServiceConfig);
    helixStore = CommonUtils.createHelixPropertyStore(accountServiceConfig.zkClientConnectString, storeConfig, null);
  }

  /**
   * Fetch the {@link Account}s from the {@link LegacyMetadataStore} and {@link RouterStore} and compare them to see if they equal
   * to each other.
   * @return True if the {@link Account}s from both stores are the same.
   * @throws Exception Any unexpected exception.
   */
  public boolean fetchAndCompareLegacyWithRouter() throws Exception {
    LegacyMetadataStore legacy = getLegacyMetadataStore();
    RouterStore router = getRouterStore();

    Map<String, String> legacyAccountMap = legacy.fetchAccountMetadata();
    Map<String, String> routerAccountMap = router.fetchAccountMetadata();

    if (legacyAccountMap != null && routerAccountMap != null) {
      if (legacyAccountMap.size() != routerAccountMap.size()) {
        System.out.println("Sizes don't match");
        return false;
      }
      for (Map.Entry<String, String> entry: legacyAccountMap.entrySet()) {
        String key = entry.getKey();

        String legacyValue = entry.getValue();
        Account legacyAccount = Account.fromJson(new JSONObject(legacyValue));
        System.out.println("Verify account: " + legacyAccount.getName());

        String routerValue = routerAccountMap.get(key);
        if (routerValue == null) {
          System.out.println("Account " + key + " doesn't exist in the router account map");
          return false;
        }
        Account routerAccount = Account.fromJson(new JSONObject(routerValue));
        if (!routerAccount.equals(legacyAccount)) {
          System.out.println("Account in router map is different than it's in legacy account map");
          System.out.println("Account in router map:");
          System.out.println("" + routerAccount);
          System.out.println("Account in legacy map:");
          System.out.println("" + legacyAccount);
          return false;
        }
      }
      return true;
    } else if (legacyAccountMap == null && routerAccountMap == null) {
      return true;
    } else {
      System.out.println("One of the map is null");
      return false;
    }
  }

  /**
   * Close all the components.
   * @throws Exception
   */
  public void close() throws Exception {
    if (router != null) {
      router.close();
    }
    if (accountService != null) {
      accountService.close();
    }
    if (helixStore != null) {
      helixStore.stop();
    }
    if (clusterMap != null) {
      clusterMap.close();
    }
  }

  /**
   * Setting proper properties for all the components to be created.
   * @param zkServer The address to zookeeper server.
   * @param storePath The root path for {@link HelixPropertyStore}.
   * @param zkConnectionTimeoutMs The connection timeout for {@link HelixPropertyStore}.
   * @param zkSessionTimeoutMs The session timeout for {@link HelixPropertyStore}.
   * @return {@link VerifiableProperties}.
   */
  private VerifiableProperties getVerifiableProperties(String zkServer, String storePath,
      int zkConnectionTimeoutMs, int zkSessionTimeoutMs) {
    Properties properties = new Properties();
    // for creating HelixAccountService
    properties.setProperty(HelixPropertyStoreConfig.HELIX_ZK_CLIENT_CONNECTION_TIMEOUT_MS, String.valueOf(zkConnectionTimeoutMs));
    properties.setProperty(HelixPropertyStoreConfig.HELIX_ZK_CLIENT_SESSION_TIMEOUT_MS, String.valueOf(zkSessionTimeoutMs));
    properties.setProperty(HelixPropertyStoreConfig.HELIX_ROOT_PATH, storePath);
    properties.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY, zkServer);

    // for creating NonBlockingRouter
    properties.setProperty("clustermap.dcs.zk.connect.strings", clusterMapDcsZkConnectString);
    properties.setProperty("clustermap.clusteragents.factory", "com.github.ambry.clustermap.HelixClusterAgentsFactory");
    properties.setProperty("clustermap.cluster.name", clusterName);
    properties.setProperty("clustermap.host.name", hostname);
    properties.setProperty("clustermap.datacenter.name", dcName);
    properties.setProperty("router.hostname", hostname);
    properties.setProperty("router.datacenter.name", dcName);
    properties.setProperty("kms.default.container.key", "B375A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");
    return new VerifiableProperties(properties);
  }

  /**
   * Create a {@link LegacyMetadataStore}.
   * @return {@link LegacyMetadataStore}.
   */
  private LegacyMetadataStore getLegacyMetadataStore() {
    return new LegacyMetadataStore(new AccountServiceMetrics(registry), backupFileManager, helixStore);
  }

  /**
   * Create a {@link RouterStore}.
   * @return {@link RouterStore}.
   * @throws Exception Any unexpected exception.
   */
  private RouterStore getRouterStore() throws Exception {
    return new RouterStore(new AccountServiceMetrics(registry), backupFileManager, helixStore, new AtomicReference<>(getRouter()), false, 100);
  }

  /**
   * Create a {@link Router} for {@link RouterStore}.
   * @return The {@link Router}.
   * @throws Exception Any unexpected exception.
   */
  private Router getRouter() throws Exception {
    // Create a HelixAccountService for the router.
    AccountServiceFactory accountServiceFactory =
        Utils.getObj("com.github.ambry.account.HelixAccountServiceFactory", verifiableProperties,
            registry);
    accountService = accountServiceFactory.getAccountService();

    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    ClusterAgentsFactory clusterAgentsFactory =
        Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
            "", "");
    clusterMap = clusterAgentsFactory.getClusterMap();
    SSLFactory sslFactory = getSSLFactoryIfRequired();
    // Create a NonBlockingRouter.
    RouterFactory routerFactory =
        Utils.getObj("com.github.ambry.router.NonBlockingRouterFactory", verifiableProperties, clusterMap, new LoggingNotificationSystem(),
            sslFactory, accountService);
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
