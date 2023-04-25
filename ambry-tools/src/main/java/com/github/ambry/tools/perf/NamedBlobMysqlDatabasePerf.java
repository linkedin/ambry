/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.named.MySqlNamedBlobDbFactory;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.json.JSONArray;


/**
 * Tool to do performance testing on mysql database with named blob.
 *
 * This tool would insert number of target rows specified in the argument or property file and then do performance testing
 * with several named blob operations.
 *
 * To run this tool.
 * First compile ambry.jar with ./gradelw allJar
 * Then copy target/ambry.jar to a host that has access to mysql database if mysql database has host based permission checking.
 * Then run
 *  > java -cp "*" com.github.ambry.tools.perf.NamedBlobMysqlDatabasePerf
 *  >      --db_username database_username
 *  >      --db_datacenter datacenter_name
 *  >      --db_name database_name
 *  >      --db_host database_host
 *  >      --parallelism 10 // number of connections to create
 *  >      --target_row 10  // number of millions of rows to create before performance test
 *  >      --include_list false // true of false to include list operations in the performance test
 *
 *  Or you can provide a property file to include all the arguments in the above command, for example:
 *  > cat named_blob.props
 *  db_username=database_username
 *  db_password=databse_pasword // yes, you can also provide database password in the prop file
 *  db_datacenter=datacenter_name
 *  db_name=database_name
 *  db_host=database_hostname
 *  parallelism=10
 *  target_rows=10
 *  include_list=false
 *  > java -cp "*" com.github.ambry.tools.perf.NamedBlobMysqlDatabasePerf --props named_blob.props
 */
public class NamedBlobMysqlDatabasePerf {
  public static final short HUGE_LIST_ACCOUNT_ID = 1024;
  public static final short HUGE_LIST_CONTAINER_ID = 8;
  public static final String HUGE_LIST_COMMON_PREFIX = "hugeListCommonPrefix/NamedBlobMysqlDatabasePerf-";
  // 10% of the chance we would use this account id and container id for insert
  public static final float PERCENTAGE_FOR_HUGE_LIST = 0.1f;

  public static final String ACCOUNT_NAME_FORMAT = "ACCOUNT_%d";
  public static final String CONTAINER_NAME_FORMAT = "CONTAINER_%d";

  public static final int NUMBER_ACCOUNT = 100;
  public static final int NUMBER_CONTAINER = 10;

  public static final String TABLE_NAME = "named_blobs_v2";

  public static final PartitionId PARTITION_ID = new MockPartitionId();

  public static final String DB_USERNAME = "db_username";
  public static final String DB_DATACENTER = "db_datacenter";
  public static final String DB_NAME = "db_name";
  public static final String DB_HOST = "db_host";
  public static final String DB_PASSWORD = "db_password";
  public static final String PARALLELISM = "parallelism";
  public static final String TARGET_ROWS = "target_rows";
  public static final String INCLUDE_LIST = "include_list";

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> propsFilepathOpt = parser.accepts("props", "Path to property file \n"
            + "All the argument can be provided through a property file with key=value at each line.\n"
            + "If an parameter both exist in the property file and the command line argument, the value in the command line argument would override the value in the property file")
        .withRequiredArg()
        .describedAs("props_file")
        .ofType(String.class);

    // database connection configuration
    ArgumentAcceptingOptionSpec<String> dbUsernameOpt = parser.accepts(DB_USERNAME, "Username to database")
        .withRequiredArg()
        .describedAs("db_username")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dbDatacenterOpt = parser.accepts(DB_DATACENTER, "Datacenter of the database")
        .withRequiredArg()
        .describedAs("datacenter")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dbNameOpt =
        parser.accepts(DB_NAME, "Database name").withRequiredArg().describedAs("db_name").ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dbHostOpt =
        parser.accepts(DB_HOST, "Database host").withRequiredArg().describedAs("db_host").ofType(String.class);

    ArgumentAcceptingOptionSpec<Integer> parallelismOpt = parser.accepts(PARALLELISM, "Number of thread to execute sql statement")
        .withRequiredArg()
        .describedAs("parallelism")
        .ofType(Integer.class);

    ArgumentAcceptingOptionSpec<Integer> targetMRowsOpt = parser.accepts(TARGET_ROWS,
            "Number of rows to insert so the total database rows would reach this target." + "Notice that this target is in in millions. If the value is 1, this command would make sure database would have 1 million rows.")
        .withRequiredArg()
        .describedAs("target_rows")
        .ofType(Integer.class);

    OptionSpec<Void> includeListTestOpt = parser.accepts(INCLUDE_LIST, "Including list operation in the performance tests.");

    OptionSet options = parser.parse(args);
    Properties props = new Properties();
    if (options.has(propsFilepathOpt)) {
      String propFilepath = options.valueOf(propsFilepathOpt);
      System.out.println("Loading properties from file " + propFilepath);
      try (FileInputStream fis = new FileInputStream(propFilepath)) {
        props.load(fis);
      }
      System.out.println("Getting properties: " + props.stringPropertyNames());
    }
    if (options.has(dbUsernameOpt)) {
      props.setProperty(DB_USERNAME, options.valueOf(dbUsernameOpt));
    }
    if (options.has(dbDatacenterOpt)) {
      props.setProperty(DB_DATACENTER, options.valueOf(dbDatacenterOpt));
    }
    if (options.has(dbNameOpt)) {
      props.setProperty(DB_NAME, options.valueOf(dbNameOpt));
    }
    if (options.has(dbHostOpt)) {
      props.setProperty(DB_HOST, options.valueOf(dbHostOpt));
    }
    if (options.has(parallelismOpt)) {
      props.setProperty(PARALLELISM, String.valueOf(options.valueOf(parallelismOpt)));
    }
    if (options.has(targetMRowsOpt)) {
      props.setProperty(TARGET_ROWS, String.valueOf(options.valueOf(targetMRowsOpt)));
    }
    if (options.has(includeListTestOpt)) {
      props.setProperty(INCLUDE_LIST, "true");
    }
    if (!props.containsKey(INCLUDE_LIST)) {
      props.setProperty(INCLUDE_LIST, "false");
    }

    List<String> requiredArguments = Arrays.asList(DB_USERNAME, DB_DATACENTER, DB_NAME, DB_HOST, PARALLELISM, TARGET_ROWS, INCLUDE_LIST);
    for (String requiredArgument : requiredArguments) {
      if (!props.containsKey(requiredArgument)) {
        System.err.println("Missing " + requiredArgument + "! Please provide it through property file or the command line argument");
        parser.printHelpOn(System.err);
        System.exit(1);
      }
    }

    // Now ask for password if it's not provided in the propFile
    if (!props.containsKey(DB_PASSWORD)) {
      String password = ToolUtils.passwordInput("Please input database password for user " + props.getProperty(DB_USERNAME) + ": ");
      props.setProperty(DB_PASSWORD, password);
    }

    // Now create a mysql named blob data accessor
    Properties newProperties = new Properties();
    String dbUrl = "jdbc:mysql://" + props.getProperty(DB_HOST) + "/" + props.getProperty(DB_NAME) + "?serverTimezone=UTC";
    MySqlUtils.DbEndpoint dbEndpoint =
        new MySqlUtils.DbEndpoint(dbUrl, props.getProperty(DB_DATACENTER), true, props.getProperty(DB_USERNAME), props.getProperty(DB_PASSWORD));
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(dbEndpoint.toJson());
    System.out.println("DB_INFO: " + jsonArray);
    newProperties.setProperty(MySqlNamedBlobDbConfig.DB_INFO, jsonArray.toString());
    newProperties.setProperty(MySqlNamedBlobDbConfig.DB_RELY_ON_NEW_TABLE, "true");
    newProperties.setProperty(MySqlNamedBlobDbConfig.LOCAL_POOL_SIZE, String.valueOf(2 * Integer.valueOf(props.getProperty(PARALLELISM))));
    newProperties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, props.getProperty(DB_DATACENTER));

    int numThreads = Integer.valueOf(props.getProperty(PARALLELISM));
    ScheduledExecutorService executor = Utils.newScheduler(numThreads + 1, "workers-", false);
    MetricRegistry registry = new MetricRegistry();
    // Mock an account service
    AccountService accountService = createInMemoryAccountService();
    MySqlNamedBlobDbFactory factory =
        new MySqlNamedBlobDbFactory(new VerifiableProperties(newProperties), registry, accountService, SystemTime.getInstance());
    DataSource dataSource = factory.buildDataSource(dbEndpoint);
    NamedBlobDb namedBlobDb = factory.getNamedBlobDb();

    // First, fill the database with target number of rows
    long targetRows = Long.valueOf(props.getProperty(TARGET_ROWS)) * 1000000L;
    long existingRows = getNumberOfRowsInDatabase(dataSource);
    if (existingRows >= targetRows) {
      System.out.println("Existing number of rows: " + existingRows + ", more than target number of rows: " + targetRows
          + ", skip filling database rows");
    } else {
      fillDatabase(registry, namedBlobDb, executor, accountService, existingRows, targetRows, numThreads);
    }

    // Then starting doing performance tests
    // For performance testing, we are expecting each database query's average latency to be 2ms. So each worker can achieve
    // 500 QPS. We are trying to achieve 80K QPS for each query, which means we at least need 160 threads.
    boolean includeList = props.getProperty(INCLUDE_LIST).equals("true");
    runPerformanceTest(registry, namedBlobDb, executor, accountService, numThreads, includeList);

    Utils.shutDownExecutorService(executor, 10, TimeUnit.SECONDS);
    namedBlobDb.close();
  }

  /**
   * Create an in memory account service for named blob.
   * @return An {@link AccountService} object.
   * @throws Exception
   */
  private static AccountService createInMemoryAccountService() throws Exception {
    AccountService accountService = new InMemAccountService(false, false);
    List<Account> accounts = new ArrayList<>();
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < NUMBER_ACCOUNT - 1; i++) {
      short accountId = (short) (i + 100);
      String accountName = String.format(ACCOUNT_NAME_FORMAT, accountId);
      int numContainer = random.nextInt(NUMBER_CONTAINER - 2) + 2;
      List<Container> containers = new ArrayList<>();
      for (int j = 0; j < numContainer; j++) {
        short containerId = (short) (j + 2);
        String containerName = String.format(CONTAINER_NAME_FORMAT, containerId);
        containers.add(new ContainerBuilder(containerId, containerName, Container.ContainerStatus.ACTIVE, "", accountId).build());
      }
      Account account = new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(containers).build();
      accounts.add(account);
    }
    // Now add the special account
    //@formatter:off
    accounts.add(
        new AccountBuilder(HUGE_LIST_ACCOUNT_ID, String.format(ACCOUNT_NAME_FORMAT, HUGE_LIST_ACCOUNT_ID), Account.AccountStatus.ACTIVE)
            .containers(
                Collections.singletonList(
                    new ContainerBuilder(HUGE_LIST_CONTAINER_ID, String.format(CONTAINER_NAME_FORMAT, HUGE_LIST_CONTAINER_ID), Container.ContainerStatus.ACTIVE, "", HUGE_LIST_ACCOUNT_ID)
                        .build()))
            .build());
    //@formatter:on
    accountService.updateAccounts(accounts);
    return accountService;
  }

  /**
   * Get total number of rows of the target table.
   * @param datasource Datasource to execute query on.
   * @return Total number of rows
   * @throws Exception
   */
  private static long getNumberOfRowsInDatabase(DataSource datasource) throws Exception {
    String rowQuerySql = "SELECT COUNT(*) as total FROM " + TABLE_NAME;
    long numberOfRows = 0;
    try (Connection connection = datasource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(rowQuerySql)) {
        try (ResultSet result = queryStatement.executeQuery()) {
          while (result.next()) {
            numberOfRows = result.getLong("total");
          }
        }
      }
    }
    return numberOfRows;
  }

  /**
   * Fill database(table) with rows so we can reach the target number of rows.
   * @param registry The {@link MetricRegistry} object.
   * @param namedBlobDb The {@link NamedBlobDb} object.
   * @param executor The {@link ScheduledExecutorService} object.
   * @param accountService The {@link AccountService} object.
   * @param existingRows The number of existing rows.
   * @param targetRows The number of target rows.
   * @param numThreads The number of threads for insert database rows.
   * @throws Exception
   */
  private static void fillDatabase(MetricRegistry registry, NamedBlobDb namedBlobDb, ScheduledExecutorService executor,
      AccountService accountService, long existingRows, long targetRows, int numThreads) throws Exception {
    long remainingRows = targetRows - existingRows;
    System.out.println("Existing number of rows: " + existingRows + ". Target number of rows: " + targetRows
        + ". Number of rows to insert: " + remainingRows);
    AtomicLong trackingRow = new AtomicLong();
    long numberOfInsertPerWorker = remainingRows / numThreads;
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      long num = numberOfInsertPerWorker;
      if (i == numThreads - 1) {
        num = remainingRows - i * numberOfInsertPerWorker;
      }
      futures.add(executor.submit(new RowFillWorker(i, namedBlobDb, accountService, num, trackingRow)));
    }
    AtomicBoolean stop = new AtomicBoolean(false);
    executor.submit(() -> {
      try {
        while (!stop.get()) {
          System.out.println(trackingRow.get() + " rows has been inserted");
          Thread.sleep(1000);
        }
      } catch (Exception e) {
      }
    });
    for (Future<?> future : futures) {
      future.get();
    }
    System.out.println("All the RowFillWorkers are finished");
    stop.set(true);
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobPutTimeInMs");
  }

  /**
   * Print out histogram data for the given metric name.
   * @param registry The {@link MetricRegistry} object.
   * @param metricName The metric name to print out
   */
  private static void printHistogramMetric(MetricRegistry registry, String metricName) {
    Histogram histogram = registry.getHistograms().get(metricName);
    System.out.println("Metric of " + metricName + ": ");
    System.out.println("Count: " + histogram.getCount());
    System.out.println("Median: " + histogram.getSnapshot().getMedian());
    System.out.println("95th: " + histogram.getSnapshot().get95thPercentile());
    System.out.println("99th: " + histogram.getSnapshot().get99thPercentile());
  }

  /**
   * Generate a random {@link NamedBlobRecord}.
   * @param random The {@link Random} object to generate random number.
   * @param accountService The {@link AccountService} object.
   * @param allAccounts All the accounts in the account service.
   * @return A {@link NamedBlobRecord}.
   */
  private static NamedBlobRecord generateRandomNamedBlobRecord(Random random, AccountService accountService,
      List<Account> allAccounts) {
    Account account;
    Container container;
    String blobName;
    if (random.nextFloat() < PERCENTAGE_FOR_HUGE_LIST) {
      account = accountService.getAccountById(HUGE_LIST_ACCOUNT_ID);
      container = account.getContainerById(HUGE_LIST_CONTAINER_ID);
      blobName = HUGE_LIST_COMMON_PREFIX + TestUtils.getRandomString(50);
    } else {
      account = allAccounts.get(random.nextInt(allAccounts.size()));
      List<Container> containers = new ArrayList<>(account.getAllContainers());
      container = containers.get(random.nextInt(containers.size()));
      blobName = TestUtils.getRandomString(50);
    }
    BlobId blobId =
        new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, account.getId(), container.getId(),
            PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId.toString(), -1);
    return record;
  }

  /**
   * Worker class to insert rows into database.
   */
  public static class RowFillWorker implements Runnable {
    private final int id;
    private final NamedBlobDb namedBlobDb;
    private final AccountService accountService;
    private final List<Account> allAccounts;
    private final long numberOfInsert;
    private final AtomicLong trackingRow;

    /**
     * Constructor for this class.
     * @param id The id of this worker.
     * @param namedBlobDb The {@link NamedBlobDb} object.
     * @param accountService The {@link AccountService} object.
     * @param numberOfInsert The insert of rows to insert
     * @param trackingRow The {@link AtomicLong} object to keep track of how many rows are inserted.
     */
    public RowFillWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService, long numberOfInsert,
        AtomicLong trackingRow) {
      this.id = id;
      this.namedBlobDb = namedBlobDb;
      this.accountService = accountService;
      this.numberOfInsert = numberOfInsert;
      this.trackingRow = trackingRow;
      allAccounts = new ArrayList<>(accountService.getAllAccounts());
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 0; l < numberOfInsert; l++) {
          NamedBlobRecord record = generateRandomNamedBlobRecord(random, accountService, allAccounts);
          namedBlobDb.put(record).get();
          trackingRow.incrementAndGet();
        }
        System.out.println("RowFillWorker " + id + " finishes writing " + numberOfInsert + " records");
      } catch (Exception e) {
        System.out.println("RowFillWorker " + id + " has som exception " + e);
      }
    }
  }

  /**
   * Run performance test after target number of rows are filled in the database.
   * @param registry The {@link MetricRegistry} object.
   * @param namedBlobDb The {@link NamedBlobDb} object.
   * @param executor The {@link ScheduledExecutorService} object.
   * @param accountService The {@link AccountService} object.
   * @param numThreads The number of threads to run performance test
   * @param includeList True to include list operation in the performance test
   * @throws Exception
   */
  private static void runPerformanceTest(MetricRegistry registry, NamedBlobDb namedBlobDb,
      ScheduledExecutorService executor, AccountService accountService, int numThreads, boolean includeList)
      throws Exception {
    long numberOfPuts = 1000 * 1000; // 1 million inserts
    System.out.println("Running performance test, number of puts: " + numberOfPuts);
    long numberOfInsertPerWorker = numberOfPuts / numThreads;
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      long num = numberOfInsertPerWorker;
      if (i == numThreads - 1) {
        num = numberOfPuts - i * numberOfInsertPerWorker;
      }
      futures.add(executor.submit(new PerformanceTestWorker(i, namedBlobDb, accountService, num, includeList)));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    System.out.println("All the PerformanceTestWorkers are finished");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobPutTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobGetTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedTtlupdateTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobListTimeInMs");
    printHistogramMetric(registry, "com.github.ambry.named.MySqlNamedBlobDb.NamedBlobDeleteTimeInMs");
  }

  /**
   * This class is the worker class that carries out the performance tests.
   * There are several tests to be done in this worker
   * 1. Put, with isUpsert to be true, otherwise, a put operation would run a select and insert query.
   * 2. Update, update the state for each named blob record
   * 3. Get, with all the named blob records that are just inserted
   * 4. Delete, delete is actually an update as well.
   * 5. List, list a small named blob sets and a huge named blob sets
   */
  public static class PerformanceTestWorker implements Runnable {
    private final int id;
    private final NamedBlobDb namedBlobDb;
    private final AccountService accountService;
    private final List<Account> allAccounts;
    private final long numberOfPuts;
    private final boolean includeList;
    private final List<NamedBlobRecord> allRecords;

    public PerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService, long numberOfPuts,
        boolean includeList) {
      this.id = id;
      this.namedBlobDb = namedBlobDb;
      this.accountService = accountService;
      this.numberOfPuts = numberOfPuts;
      this.includeList = includeList;
      allAccounts = new ArrayList<>(accountService.getAllAccounts());
      allRecords = new ArrayList<>((int) numberOfPuts);
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 0; l < numberOfPuts; l++) {
          NamedBlobRecord record = generateRandomNamedBlobRecord(random, accountService, allAccounts);
          namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
          allRecords.add(record);
        }
        System.out.println("PerformanceTestWorker " + id + " finishes writing " + numberOfPuts + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.updateBlobStateToReady(record).get();
        }
        System.out.println("PerformanceTestWorker " + id + " finishes updating " + numberOfPuts + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        }
        System.out.println("PerformanceTestWorker " + id + " finishes reading " + numberOfPuts + " records");

        if (includeList) {
          int numberOfList = 0;
          for (NamedBlobRecord record : allRecords) {
            if (!record.getAccountName().equals(String.format(ACCOUNT_NAME_FORMAT, HUGE_LIST_ACCOUNT_ID))) {
              namedBlobDb.list(record.getAccountName(), record.getContainerName(), "A", null).get();
              numberOfList++;
              if (numberOfList == 100) {
                break;
              }
            }
          }
          System.out.println("PerformanceTestWorker " + id + " finishes listing for records");

          String accountName = String.format(ACCOUNT_NAME_FORMAT, HUGE_LIST_ACCOUNT_ID);
          String containerName = String.format(CONTAINER_NAME_FORMAT, HUGE_LIST_CONTAINER_ID);
          String token = null;
          for (int i = 0; i < 100; i++) {
            token =
                namedBlobDb.list(accountName, containerName, HUGE_LIST_COMMON_PREFIX, token).get().getNextPageToken();
          }
          System.out.println("PerformanceTestWorker " + id + " finishes listing for huge records");
        }

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        }
        System.out.println("PerformanceTestWorker " + id + " finishes deleting " + numberOfPuts + " records");
      } catch (Exception e) {
        System.out.println("PerformanceTestWorker " + id + " has som exception " + e);
      }
    }
  }
}
