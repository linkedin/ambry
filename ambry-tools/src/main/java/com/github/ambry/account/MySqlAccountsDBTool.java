package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.mysql.AccountTable;
import com.github.ambry.account.mysql.ContainerTable;
import com.github.ambry.account.mysql.MySqlConfig;
import com.github.ambry.account.mysql.MySqlDataAccessor;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   This is a command-line tool can be used to either:
 *    a) populate list of {@link Account}s and {@link Container}s into MySQL DB from {@code ZooKeeper} store
 *    b) compare list of {@link Account}s and {@link Container}s in MySQL DB with {@code ZooKeeper} store
 *   This can be used to validate that accounts and containers are correctly being updated in MySQL during migration phase and
 *   are in sync with ZK storage.
 *
 *   This tool takes in following parameters:
 *    1. 'propsFile' file which contains DB credentials needed to connect to MySQL url, username, password;
 *    2. 'operation' which tells to the type of operation. Supported operations are a)init, b)compare
 *    3. 'zkServer' - connect string of zookeeper server
 *    4. 'zkStorePath' - path of znode
 *
 *   A sample usage of the tool is:
 *     java -Dlog4j.configuration=file:../config/log4j.properties -cp ambry.jar com.github.ambry.account.MySqlAccountsDBTool
 *     --propsFile mysql.properties --operation init --zkServer zk-ei4-ambry.int.linkedin.com:12913 --storePath /Ambry/Ambry-EI/helixPropertyStore
 *
 *  A propsFile should contain:
 *    db.url=jdbc:mysql://makto-db-006.int.linkedin.com/AccountMetadata
 *    user=AmbryUser-2
 *    password=AmbryAccounts-1
 * </p>
 */

public class MySqlAccountsDBTool {

  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountsDBTool.class);
  private static String PROPSFILE = "propsFile";
  private static String OPERATION = "operation";
  private static String ZKSERVER = "zkServer";
  private static String STOREPATH = "storePath";
  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 5000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  static final String ACCOUNT_METADATA_MAP_KEY = "accountMetadata";
  static final String RELATIVE_ACCOUNT_METADATA_PATH = "/account_metadata/full_data";

  private final MySqlDataAccessor mySqlDataAccessor;
  private final AccountTable accountTable;
  private final ContainerTable containerTable;
  private final HelixPropertyStore<ZNRecord> helixPropertyStore;
  private final String fullZKAccountMetadataPath;

  enum OPERATION_TYPE {
    INIT, COMPARE
  }

  public static void main(String[] args) throws IOException {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> propsFileOpt =
        parser.accepts(PROPSFILE, "Properties file path").withRequiredArg().describedAs(PROPSFILE).ofType(String.class);

    ArgumentAcceptingOptionSpec<String> operationOpt =
        parser.accepts(OPERATION, "Supported operations are 'init' and 'compare'")
            .withRequiredArg()
            .describedAs(OPERATION)
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> zkServerOpt = parser.accepts(ZKSERVER, "The address of ZooKeeper server")
        .withRequiredArg()
        .describedAs(ZKSERVER)
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> storePathOpt = parser.accepts(STOREPATH,
        "The root path of helix property store in the ZooKeeper. "
            + "Must start with /, and must not end with /. It is recommended to make root path in the form of "
            + "/ambry/<clustername>/helixPropertyStore. This option is required if source of storage is zookeeper.")
        .withRequiredArg()
        .describedAs(STOREPATH)
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

    parser.accepts("help", "print this help message.");
    parser.accepts("h", "print this help message.");

    OptionSet options = parser.parse(args);
    if (options.has("help") || options.has("h")) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    // ensure mandatory arguments (propsFile, operation, zk address, zk store path) are present
    ToolUtils.ensureOrExit(Arrays.asList(propsFileOpt, operationOpt, zkServerOpt, storePathOpt), options, parser);
    String propsFilePath = options.valueOf(propsFileOpt);
    String operation = options.valueOf(operationOpt);
    MySqlAccountsDBTool.OPERATION_TYPE operationType = null;
    try {
      operationType = Enum.valueOf(MySqlAccountsDBTool.OPERATION_TYPE.class, operation.toUpperCase());
    } catch (Exception e) {
      System.out.println("Invalid operation: " + operation + ". Supported operations: init, compare");
      parser.printHelpOn(System.out);
      System.exit(0);
    }
    String zkServer = options.valueOf(zkServerOpt);
    String storePath = options.valueOf(storePathOpt);
    Integer zkConnectionTimeoutMs = options.valueOf(zkConnectionTimeoutMsOpt);
    Integer zkSessionTimeoutMs = options.valueOf(zkSessionTimeoutMsOpt);

    try {
      Properties properties = Utils.loadProps(propsFilePath);
      properties.setProperty(HelixPropertyStoreConfig.HELIX_ZK_CLIENT_CONNECTION_TIMEOUT_MS,
          String.valueOf(zkConnectionTimeoutMs));
      properties.setProperty(HelixPropertyStoreConfig.HELIX_ZK_CLIENT_SESSION_TIMEOUT_MS,
          String.valueOf(zkSessionTimeoutMs));
      properties.setProperty(HelixPropertyStoreConfig.HELIX_ROOT_PATH, storePath);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

      MySqlAccountsDBTool mySqlAccountsDBTool = new MySqlAccountsDBTool(verifiableProperties, zkServer);

      if (operationType == OPERATION_TYPE.INIT) {
        mySqlAccountsDBTool.initialize();
      } else if (operationType == OPERATION_TYPE.COMPARE) {
        mySqlAccountsDBTool.compare();
      }
    } catch (Exception e) {
      logger.error("MySQL accounts validation failed", e);
    }
  }

  public MySqlAccountsDBTool(VerifiableProperties verifiableProperties, String zkServer) throws SQLException {

    this.mySqlDataAccessor = new MySqlDataAccessor(new MySqlConfig(verifiableProperties));
    this.accountTable = new AccountTable(mySqlDataAccessor);
    this.containerTable = new ContainerTable(mySqlDataAccessor);
    //Create helix property store
    HelixPropertyStoreConfig helixPropertyStoreConfig = new HelixPropertyStoreConfig(verifiableProperties);
    this.helixPropertyStore = CommonUtils.createHelixPropertyStore(zkServer, helixPropertyStoreConfig, null);

    //store the complete path of metadata in zk for logging
    fullZKAccountMetadataPath = helixPropertyStoreConfig.rootPath + RELATIVE_ACCOUNT_METADATA_PATH;
  }

  private void cleanup() throws SQLException {
    Statement statement = mySqlDataAccessor.getDatabaseConnection().createStatement();
    int numDeleted = statement.executeUpdate("delete from " + ContainerTable.CONTAINER_TABLE);
    logger.info("Deleted {} containers", numDeleted);
    int numDeletedAccounts = statement.executeUpdate("delete from " + AccountTable.ACCOUNT_TABLE);
    logger.info("Deleted {} Accounts", numDeletedAccounts);
  }

  /**
   * Initializes db from zk
   */
  public void initialize() throws SQLException {

    // clean the account and container tables in DB
    cleanup();

    //get the list of accounts from zk in the form of map account id -> account json (as string)
    long startTimeMs = SystemTime.getInstance().milliseconds();
    Map<String, String> accountMap = fetchAccountMetadataFromZK();
    if (accountMap == null) {
      logger.info("Account metadata in ZK is empty");
      return;
    }
    long zkFetchTimeMs = SystemTime.getInstance().milliseconds();
    logger.info("Fetched account metadata from zk path={}, took time={} ms", fullZKAccountMetadataPath,
        zkFetchTimeMs - startTimeMs);

    AccountInfoMap accountInfoMap = new AccountInfoMap(new AccountServiceMetrics(new MetricRegistry()), accountMap);
    for (Account account : accountInfoMap.getAccounts()) {
      accountTable.addAccount(account);
      for (Container container : account.getAllContainers()) {
        containerTable.addContainer(account.getId(), container);
      }
    }

    logger.info("Initialized account metadata in DB from ZK path {}, took time={} ms",
        fullZKAccountMetadataPath, System.currentTimeMillis() - zkFetchTimeMs);
  }

  /**
   * compares db with zk and prints the accounts (IDs) that are different
   */
  public void compare() throws SQLException {

    //get the list of accounts from zk in the form of map account id -> account json (as string)
    long startTimeMs = SystemTime.getInstance().milliseconds();
    Map<String, String> accountMapFromZK = fetchAccountMetadataFromZK();
    if (accountMapFromZK == null) {
      logger.info("Account metadata in ZK is empty");
      return;
    }
    long zkFetchTimeMs = SystemTime.getInstance().milliseconds();
    logger.info("Fetched account metadata from zk path={}, took time={} ms", fullZKAccountMetadataPath,
        zkFetchTimeMs - startTimeMs);
    Set<Account> accountSetFromZK = (accountMapFromZK.values()
        .stream()
        .map(accountString -> Account.fromJson(new JSONObject(accountString)))
        .collect(Collectors.toSet()));

    // get the list of accounts from mysql in the form of map account id -> account json (as string)
    Set<Account> accountSetFromDB = new HashSet<>(accountTable.getNewAccounts(0));

    //Accounts missing (or different) in DB = accounts in ZK - accounts in DB
    accountSetFromZK.removeAll(accountSetFromDB);
    if (accountSetFromZK.size() > 0) {
      logger.info("Accounts different in DB {} ", accountSetFromZK);
    } else {
      logger.info("Accounts in ZK and DB are same");
    }
  }

  Map<String, String> fetchAccountMetadataFromZK() {
    Stat stat = new Stat();
    ZNRecord znRecord = helixPropertyStore.get(RELATIVE_ACCOUNT_METADATA_PATH, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      logger.info("The ZNRecord to read does not exist on path={}", RELATIVE_ACCOUNT_METADATA_PATH);
      return null;
    }
    return znRecord.getMapField(ACCOUNT_METADATA_MAP_KEY);
  }
}
