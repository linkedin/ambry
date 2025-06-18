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
import com.github.ambry.config.Config;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.named.MySqlNamedBlobDbFactory;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.json.JSONArray;


/**
 * Tool to do performance testing on mysql database with named blob.
 *
 * This tool would insert number of target rows specified in the property file and then do performance testing
 * with several named blob operations.
 *
 * To run this tool.
 * First compile ambry.jar with ./gradelw allJar
 * Then copy target/ambry.jar to a host that has access to mysql database if mysql database has host based permission checking.
 * You also have to provide a property file to include all the arguments, for example:
 *  > cat named_blob.props
 *  db.user.name=database_username
 *  db.password=databse_pasword
 *  db.datacenter=datacenter_name
 *  db.name=database_name
 *  db.host=database_hostname
 *  parallelism=10
 *  target.rows.in.million=10
 *  num.operations=1000000
 *  test.type=LIST
 *  > java -cp "*" com.github.ambry.tools.perf.NamedBlobMysqlDatabasePerf --props named_blob.props
 */
public class NamedBlobMysqlDatabasePerf {
  public static final String ACCOUNT_NAME_FORMAT = "ACCOUNT_%d";
  public static final String CONTAINER_NAME_FORMAT = "CONTAINER_%d";

  public static final short HUGE_LIST_ACCOUNT_ID = 1024;
  public static final short HUGE_LIST_CONTAINER_ID = 8;
  public static final String HUGE_LIST_ACCOUNT_NAME = String.format(ACCOUNT_NAME_FORMAT, HUGE_LIST_ACCOUNT_ID);
  public static final String HUGE_LIST_CONTAINER_NAME = String.format(CONTAINER_NAME_FORMAT, HUGE_LIST_CONTAINER_ID);
  public static final String HUGE_LIST_COMMON_PREFIX = "hugeListCommonPrefix/NamedBlobMysqlDatabasePerf-";
  public static final String HUGE_LIST_TEMPLATE = "hugeListCommonPrefix/NamedBlobMysqlDatabasePerf-%s/%s";

  public static final int NUMBER_ACCOUNT = 100;
  public static final int NUMBER_CONTAINER = 10;

  public static final String TABLE_NAME = "named_blobs_v2";

  public static final PartitionId PARTITION_ID = new MockPartitionId();

  public static class PerfConfig {
    /**
     * Dataceneter from there this cli is executed
     */
    @Config("db.datacenter")
    public final String dbDatacenter;

    /**
     * The host of database instance (or cluster), including port
     */
    @Config("db.host")
    public final String dbHost;

    /**
     * The name of database
     */
    @Config("db.name")
    public final String dbName;

    /**
     * The user name to access database
     */
    @Config("db.user.name")
    public final String dbUserName;

    /**
     * The password for the given username. If you enable certificate based authentication, this will
     * be ignored, just provide a random string.
     *
     * You can also provide an empty string when certificate based authentication is not enabled,
     * you will be prompted to type password when running this cli.
     */
    @Config("db.password")
    public final String dbPassword;

    /**
     * True to enable certificate based authentication. If this is true, you must also provide ssl configuration
     * including
     * <ul>
     *   <li>{@link SSLConfig#sslKeystoreType}</li>
     *   <li>{@link SSLConfig#sslKeystorePath}</li>
     *   <li>{@link SSLConfig#sslKeystorePassword}</li>
     *   <li>{@link SSLConfig#sslTruststoreType}</li>
     *   <li>{@link SSLConfig#sslTruststorePath}</li>
     *   <li>{@link SSLConfig#sslTruststorePassword}</li>
     * </ul>
     */
    @Config("enable.certificate.based.authentication")
    public final boolean enableCertificateBasedAuthentication;

    /**
     * The number of parallel threads to run the performance test.
     */
    @Config("parallelism")
    public final int parallelism;

    /**
     * The target number of rows in the database, in millions. If there are not enough rows in the database before
     * running the performance test, this tool will fill the database with new rows until it hits the target number.
     */
    @Config("target.rows.in.million")
    public final int targetRowsInMillion;

    /**
     * Enable hard delete when deleting named blobs.
     */
    @Config("enable.hard.delete")
    public final boolean enableHardDelete;

    /**
     * The number of operations to exuected in performance test.
     */
    @Config("num.operations")
    public final int numOperations;

    /**
     * The test type to run. It can be one of the following:
     * <ul>
     *   <li>{@link TestType#CUSTOM}</li>
     *   <li>{@link TestType#LIST}</li>
     *   <li>{@link TestType#READ_WRITE}</li>
     * </ul>
     */
    @Config("test.type")
    public final TestType testType;

    /**
     * Only do write operations in CUSTOM test type when this is true.
     */
    @Config("custom.only.writes")
    public final boolean onlyWrites;

    /**
     * Create a {@link PerfConfig} given a {@link VerifiableProperties} object.
     * @param verifiableProperties
     * @throws Exception
     */
    public PerfConfig(VerifiableProperties verifiableProperties) throws Exception {
      dbDatacenter = verifiableProperties.getString("db.datacenter", "");
      dbHost = verifiableProperties.getString("db.host", "");
      dbName = verifiableProperties.getString("db.name", "");
      dbUserName = verifiableProperties.getString("db.user.name", "");
      dbPassword = verifiableProperties.getString("db.password", "");
      enableCertificateBasedAuthentication =
          verifiableProperties.getBoolean("enable.certificate.based.authentication", false);
      parallelism = verifiableProperties.getInt("parallelism", 0);
      targetRowsInMillion = verifiableProperties.getIntInRange("target.rows.in.million", 0, 0, 100);
      enableHardDelete = verifiableProperties.getBoolean("enable.hard.delete", false);
      numOperations = verifiableProperties.getInt("num.operations", 0);
      testType = verifiableProperties.getEnum("test.type", TestType.class, TestType.READ_WRITE);
      onlyWrites = verifiableProperties.getBoolean("custom.only.writes", false);
      validate();
    }

    /**
     * Validate if the configurations are valid
     * @throws Exception
     */
    public void validate() throws Exception {
      validateNotEmpty(dbDatacenter, "db.datacenter");
      validateNotEmpty(dbHost, "db.host");
      validateNotEmpty(dbName, "db.name");
      validateNotEmpty(dbUserName, "db.user.name");
      validatePositive(parallelism, "parallelism");
      validatePositive(numOperations, "num.operations");
    }

    private void validateNotEmpty(String value, String name) throws Exception {
      if (isStringEmpty(value)) {
        throw new IllegalArgumentException(name + " should not be empty");
      }
    }

    private void validatePositive(int value, String name) throws Exception {
      if (value <= 0) {
        throw new IllegalArgumentException(name + " should be positive");
      }
    }
  }

  /**
   * The type of test to run. This cli tool can be used to run various types of tests. Reach {@Link TestType} is
   * associated with a {@link PerformanceTestWorker} class that implements several static methods.
   * <ul>
   *   <li>getNumberOfExistingRows, which takes in a {@link DataSource} and return long</li>
   *   <li>generateNewNamedBlobRecord, which takes in a {@link Random} and a list of {@link Account}s and return a {@link NamedBlobRecord}</li>
   * </ul>
   */
  public enum TestType {
    READ_WRITE, LIST, CUSTOM;

    /**
     * Associate TestType with {@link PerformanceTestWorker} class.
     */
    static final Map<TestType, Class> workerClassMap = new HashMap<TestType, Class>() {
      {
        put(READ_WRITE, ReadWritePerformanceTestWorker.class);
        put(LIST, ListPerformanceTestWorker.class);
        put(CUSTOM, CustomPerformanceTestWorker.class);
      }
    };

    /**
     * Return the number of existing rows for each {@link TestType}.
     * @param dataSource
     * @return
     * @throws Exception
     */
    public long getNumberOfExistingRows(DataSource dataSource) throws Exception {
      Class<?> clazz = workerClassMap.get(this);
      Method method = clazz.getDeclaredMethod("getNumberOfExistingRows", DataSource.class);
      Object object = method.invoke(null, dataSource);
      if (object instanceof Long) {
        return (Long) object;
      } else {
        throw new IllegalArgumentException("getNumberOfExistingRows should return a long");
      }
    }

    /**
     * Return the newly generated {@link NamedBlobRecord} for each {@link TestType}.
     * @param random
     * @param allAccounts
     * @return
     * @throws Exception
     */
    public NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) throws Exception {
      Class<?> clazz = workerClassMap.get(this);
      Method method = clazz.getDeclaredMethod("generateNewNamedBlobRecord", Random.class, List.class);
      Object object = method.invoke(null, random, allAccounts);
      if (object instanceof NamedBlobRecord) {
        return (NamedBlobRecord) object;
      } else {
        throw new IllegalArgumentException("generateNewNamedBlobRecord should return a NamedBlobRecord");
      }
    }

    /**
     * Get class object associated with each {@link TestType}.
     * @return
     */
    public Class getWorkerClass() {
      return workerClassMap.get(this);
    }
  }

  static boolean isStringEmpty(String value) {
    return value == null || value.isEmpty() || value.trim().isEmpty();
  }

  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    PerfConfig config = new PerfConfig(verifiableProperties);
    Properties newProperties = new Properties();

    String password = config.dbPassword;
    if (!config.enableCertificateBasedAuthentication) {
      // we are going to use password
      if (isStringEmpty(password)) {
        password = ToolUtils.passwordInput("Please input database password for user " + config.dbUserName + ": ");
      }
    } else {
      newProperties.setProperty(MySqlNamedBlobDbConfig.ENABLE_CERTIFICATE_BASED_AUTHENTICATION, "true");
      newProperties.setProperty("ssl.keystore.type", verifiableProperties.getString("ssl.keystore.type"));
      newProperties.setProperty("ssl.keystore.path", verifiableProperties.getString("ssl.keystore.path"));
      newProperties.setProperty("ssl.keystore.password", verifiableProperties.getString("ssl.keystore.password"));
      newProperties.setProperty("ssl.truststore.type", verifiableProperties.getString("ssl.truststore.type"));
      newProperties.setProperty("ssl.truststore.path", verifiableProperties.getString("ssl.truststore.path"));
      newProperties.setProperty("ssl.truststore.password", verifiableProperties.getString("ssl.truststore.password"));
    }

    // Now create a mysql named blob data accessor
    String dbUrl = "jdbc:mysql://" + config.dbHost + "/" + config.dbName + "?serverTimezone=UTC";
    MySqlUtils.DbEndpoint dbEndpoint =
        new MySqlUtils.DbEndpoint(dbUrl, config.dbDatacenter, true, config.dbUserName, password);
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(dbEndpoint.toJson());
    System.out.println("DB_INFO: " + jsonArray);
    newProperties.setProperty(MySqlNamedBlobDbConfig.DB_INFO, jsonArray.toString());
    newProperties.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION,
        String.valueOf(MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION));
    newProperties.setProperty(MySqlNamedBlobDbConfig.LOCAL_POOL_SIZE, String.valueOf(2 * config.parallelism));
    newProperties.setProperty(MySqlNamedBlobDbConfig.ENABLE_HARD_DELETE, String.valueOf(config.enableHardDelete));
    newProperties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, config.dbDatacenter);

    int numThreads = config.parallelism;
    ScheduledExecutorService executor = Utils.newScheduler(numThreads + 1, "workers-", false);
    MetricRegistry registry = new MetricRegistry();
    // Mock an account service
    AccountService accountService = createInMemoryAccountService();
    MySqlNamedBlobDbFactory factory =
        new MySqlNamedBlobDbFactory(new VerifiableProperties(newProperties), registry, accountService);
    DataSource dataSource = factory.buildDataSource(dbEndpoint);
    NamedBlobDb namedBlobDb = factory.getNamedBlobDb();
    TestType testType = config.testType;

    prepareDatabaseForPerfTest(testType, registry, namedBlobDb, dataSource, executor, accountService, config);
    runPerformanceTest(registry, namedBlobDb, testType, executor, accountService, numThreads, config);

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
        containers.add(
            new ContainerBuilder(containerId, containerName, Container.ContainerStatus.ACTIVE, "", accountId).build());
      }
      Account account =
          new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(containers).build();
      accounts.add(account);
    }
    // Now add the special account
    //@formatter:off
    accounts.add(
        new AccountBuilder(HUGE_LIST_ACCOUNT_ID, HUGE_LIST_ACCOUNT_NAME, Account.AccountStatus.ACTIVE)
            .containers(
                Collections.singletonList(
                    new ContainerBuilder(HUGE_LIST_CONTAINER_ID, HUGE_LIST_CONTAINER_NAME, Container.ContainerStatus.ACTIVE, "", HUGE_LIST_ACCOUNT_ID)
                        .build()))
            .build());
    //@formatter:on
    accountService.updateAccounts(accounts);
    return accountService;
  }

  /**
   * Fill database(table) with rows so we can reach the target number of rows.
   * @param registry The {@link MetricRegistry} object.
   * @param namedBlobDb The {@link NamedBlobDb} object.
   * @param executor The {@link ScheduledExecutorService} object.
   * @param accountService The {@link AccountService} object.
   * @param config The {@link PerfConfig} object that contains properties for the test.
   * @throws Exception
   */
  private static void prepareDatabaseForPerfTest(TestType testType, MetricRegistry registry, NamedBlobDb namedBlobDb,
      DataSource dataSource, ScheduledExecutorService executor, AccountService accountService, PerfConfig config)
      throws Exception {
    // First, fill the database with target number of rows
    long targetRows = config.targetRowsInMillion * 1000000L;
    long existingRows = testType.getNumberOfExistingRows(dataSource);
    if (existingRows >= targetRows) {
      System.out.println("Existing number of rows: " + existingRows + ", more than target number of rows: " + targetRows
          + ", skip filling database rows");
      return;
    }

    int numThreads = config.parallelism;
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
      futures.add(executor.submit(new RowFillWorker(i, namedBlobDb, testType, accountService, num, trackingRow)));
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
   * Worker class to insert rows into database.
   */
  public static class RowFillWorker implements Runnable {
    private final int id;
    private final NamedBlobDb namedBlobDb;
    private final List<Account> allAccounts;
    private final long numberOfInsert;
    private final TestType testType;
    private final AtomicLong trackingRow;

    /**
     * Constructor for this class.
     * @param id The id of this worker.
     * @param namedBlobDb The {@link NamedBlobDb} object.
     * @param accountService The {@link AccountService} object.
     * @param numberOfInsert The insert of rows to insert
     * @param trackingRow The {@link AtomicLong} object to keep track of how many rows are inserted.
     */
    public RowFillWorker(int id, NamedBlobDb namedBlobDb, TestType testType, AccountService accountService,
        long numberOfInsert, AtomicLong trackingRow) {
      this.id = id;
      this.namedBlobDb = namedBlobDb;
      this.testType = testType;
      this.numberOfInsert = numberOfInsert;
      this.trackingRow = trackingRow;
      allAccounts = new ArrayList<>(accountService.getAllAccounts());
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 0; l < numberOfInsert; l++) {
          NamedBlobRecord record = testType.generateNewNamedBlobRecord(random, allAccounts);
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
   * @param config The {@link PerfConfig} object that contains properties for the test.
   * @throws Exception
   */
  private static void runPerformanceTest(MetricRegistry registry, NamedBlobDb namedBlobDb, TestType testType,
      ScheduledExecutorService executor, AccountService accountService, int numThreads, PerfConfig config)
      throws Exception {
    int numberOfOperations = config.numOperations;
    System.out.println("Running performance test, number of puts: " + numberOfOperations);
    int numberOfInsertPerWorker = numberOfOperations / numThreads;
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      int num = numberOfInsertPerWorker;
      if (i == numThreads - 1) {
        num = numberOfOperations - i * numberOfInsertPerWorker;
      }
      futures.add(executor.submit((PerformanceTestWorker) testType.getWorkerClass()
          .getConstructor(int.class, NamedBlobDb.class, AccountService.class, int.class, PerfConfig.class)
          .newInstance(i, namedBlobDb, accountService, num, config)));
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
   * A base class for performance test worker
   */
  public static abstract class PerformanceTestWorker implements Runnable {
    protected final int id;
    protected final NamedBlobDb namedBlobDb;
    protected final AccountService accountService;
    protected final List<Account> allAccounts;
    protected final long numberOfOperations;
    protected final PerfConfig config;

    public PerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService, int numberOfOperations,
        PerfConfig config) {
      this.id = id;
      this.namedBlobDb = namedBlobDb;
      this.accountService = accountService;
      this.numberOfOperations = numberOfOperations;
      this.config = config;
      allAccounts = new ArrayList<>(accountService.getAllAccounts());
    }
  }

  /**
   * A performance worker class to do all point lookup operations for named blob, includin put, get, update and delete.
   */
  public static class ReadWritePerformanceTestWorker extends PerformanceTestWorker {
    private final List<NamedBlobRecord> allRecords = new ArrayList<>();

    public ReadWritePerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService,
        int numberOfOperations, PerfConfig config) {
      super(id, namedBlobDb, accountService, numberOfOperations, config);
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 0; l < numberOfOperations; l++) {
          NamedBlobRecord record = ReadWritePerformanceTestWorker.generateNewNamedBlobRecord(random, allAccounts);
          namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
          allRecords.add(record);
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes writing " + numberOfOperations + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.updateBlobTtlAndStateToReady(record).get();
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes updating " + numberOfOperations + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes reading " + numberOfOperations + " records");

        for (NamedBlobRecord record : allRecords) {
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        }
        System.out.println(
            "ReadWritePerformanceTestWorker " + id + " finishes deleting " + numberOfOperations + " records");
      } catch (Exception e) {
        System.out.println("ReadWritePerformanceTestWorker " + id + " has som exception " + e);
      }
    }

    /**
     * Get total number of rows of the target table.
     * @param datasource Datasource to execute query on.
     * @return Total number of rows
     * @throws Exception
     */
    public static long getNumberOfExistingRows(DataSource datasource) throws Exception {
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
     * Generate a random {@link NamedBlobRecord} for ReadWritePerformanceTestWorker.
     * @param random The {@link Random} object to generate random number.
     * @param allAccounts All the accounts in the account service.
     * @return A {@link NamedBlobRecord}.
     */
    public static NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) {
      Account account = allAccounts.get(random.nextInt(allAccounts.size()));
      List<Container> containers = new ArrayList<>(account.getAllContainers());
      Container container = containers.get(random.nextInt(containers.size()));
      String blobName = TestUtils.getRandomString(50);
      BlobId blobId =
          new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, account.getId(), container.getId(),
              PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId.toString(), -1);
      return record;
    }
  }

  /**
   * A performance worker for list operation for named blob db. The worker thread would iterate over all the blob names
   * with a give prefix under the given account and container.
   */
  public static class ListPerformanceTestWorker extends PerformanceTestWorker {

    public ListPerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService,
        int numberOfOperations, PerfConfig config) {
      super(id, namedBlobDb, accountService, numberOfOperations, config);
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < numberOfOperations; i++) {
          Page<NamedBlobRecord> page = null;
          String token = null;
          do {
            page =
                namedBlobDb.list(HUGE_LIST_ACCOUNT_NAME, HUGE_LIST_CONTAINER_NAME, HUGE_LIST_COMMON_PREFIX, token, null)
                    .get();
            token = page.getNextPageToken();
          } while (token != null);
          if (i % 100 == 0) {
            System.out.println("ListPerformanceTestWorker " + id + " finishes " + i + " operations");
          }
        }
      } catch (Exception e) {
        System.out.println("ListPerformanceTestWorker " + id + " has som exception " + e);
      }
    }

    /**
     * Get total number of rows for that starts with the list prefix under the given account and container.
     * @param datasource Datasource to execute query on.
     * @return Total number of rows
     * @throws Exception
     */
    public static long getNumberOfExistingRows(DataSource datasource) throws Exception {
      String rowQuerySql = "SELECT COUNT(*) as total FROM " + TABLE_NAME
          + " WHERE account_id = ? And container_id = ? and BLOB_NAME like ?";
      long numberOfRows = 0;
      try (Connection connection = datasource.getConnection()) {
        try (PreparedStatement queryStatement = connection.prepareStatement(rowQuerySql)) {
          queryStatement.setInt(1, HUGE_LIST_ACCOUNT_ID);
          queryStatement.setInt(2, HUGE_LIST_CONTAINER_ID);
          queryStatement.setString(3, HUGE_LIST_COMMON_PREFIX + "%");
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
     * Generate a random {@link NamedBlobRecord} for list operation.
     * @param random The {@link Random} object to generate random number.
     * @param allAccounts All the accounts in the account service.
     * @return A {@link NamedBlobRecord}.
     */
    public static NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) {
      String blobName = String.format(HUGE_LIST_TEMPLATE, UUID.randomUUID(), TestUtils.getRandomString(32));
      BlobId blobId = new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, HUGE_LIST_ACCOUNT_ID,
          HUGE_LIST_CONTAINER_ID, PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
      NamedBlobRecord record =
          new NamedBlobRecord(HUGE_LIST_ACCOUNT_NAME, HUGE_LIST_CONTAINER_NAME, blobName, blobId.toString(), -1);
      return record;
    }
  }

  /**
   * A performance worker for custom operation set for named blob db. This worker does a set of operations as you defined
   * in the method.
   */
  public static class CustomPerformanceTestWorker extends PerformanceTestWorker {
    private final boolean onlyWrites;

    public CustomPerformanceTestWorker(int id, NamedBlobDb namedBlobDb, AccountService accountService,
        int numberOfOperations, PerfConfig config) {
      super(id, namedBlobDb, accountService, numberOfOperations, config);
      this.onlyWrites = config.onlyWrites;
    }

    @Override
    public void run() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        for (long l = 1; l <= numberOfOperations; l++) {
          NamedBlobRecord record = CustomPerformanceTestWorker.generateNewNamedBlobRecord(random, allAccounts);
          if (!onlyWrites) {
            try {
              namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
            } catch (Exception e) {
              // expected NOT_FOUND failure
            }
            try {
              namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName() + "/").get();
            } catch (Exception e) {
              // expected NOT_FOUND failure
            }

            try {
              namedBlobDb.list(record.getAccountName(), record.getContainerName(), record.getBlobName() + "/", null,
                  null);
            } catch (Exception e) {
              // expected NOT_FOUND failure
            }
          }
          PutResult putResult = namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
          // Get the updated version
          record = putResult.getInsertedRecord();
          namedBlobDb.updateBlobTtlAndStateToReady(record).get();
          if (!onlyWrites) {
            // Get blob again
            namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
          }
          // Now delete
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
          if (l % 100 == 0) {
            System.out.println("CustomPerformanceTestWorker " + id + " finishes " + l + " records");
          }
        }
        System.out.println("CustomPerformanceTestWorker " + id + " finishes " + numberOfOperations + " records");
      } catch (Exception e) {
        System.out.println("CustomPerformanceTestWorker " + id + " has som exception " + e);
      }
    }

    /**
     * Just return 0 here.
     * @param datasource
     * @return
     * @throws Exception
     */
    public static long getNumberOfExistingRows(DataSource datasource) throws Exception {
      return 0L;
    }

    /**
     * Generate a random {@link NamedBlobRecord} for custom operation.
     * @param random The {@link Random} object to generate random number.
     * @param allAccounts All the accounts in the account service.
     * @return A {@link NamedBlobRecord}.
     */
    public static NamedBlobRecord generateNewNamedBlobRecord(Random random, List<Account> allAccounts) {
      Account account = allAccounts.get(random.nextInt(allAccounts.size()));
      List<Container> containers = new ArrayList<>(account.getAllContainers());
      Container container = containers.get(random.nextInt(containers.size()));
      String blobName =
          String.format("checkpoints/%s/chk-900/%s", TestUtils.getRandomString(32), UUID.randomUUID().toString());
      BlobId blobId =
          new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 1, account.getId(), container.getId(),
              PARTITION_ID, false, BlobId.BlobDataType.DATACHUNK);
      long expirationTime = Utils.addSecondsToEpochTime(System.currentTimeMillis() / 1000, TimeUnit.DAYS.toSeconds(2));
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId.toString(), expirationTime);
      return record;
    }
  }
}
