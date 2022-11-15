/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.named;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetryExecutor;
import com.github.ambry.commons.RetryPolicies;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.mysql.MySqlUtils.DbEndpoint;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link NamedBlobDb} that uses an active-active MySQL deployment as the source of truth for
 * named blob mappings.
 *
 * It uses the Hikari library for connection pooling, which is a widely used and performant JDBC connection pool
 * implementation.
 */
class MySqlNamedBlobDb implements NamedBlobDb {
  private static final Logger logger = LoggerFactory.getLogger(MySqlNamedBlobDb.class);
  // table name
  private static final String NAMED_BLOBS_V2 = "named_blobs_v2";
  // column names
  private static final String ACCOUNT_ID = "account_id";
  private static final String CONTAINER_ID = "container_id";
  private static final String BLOB_NAME = "blob_name";
  private static final String BLOB_ID = "blob_id";
  private static final String BLOB_STATE = "blob_state";
  private static final String VERSION = "version";
  private static final String DELETED_TS = "deleted_ts";
  // query building blocks
  private static final String CURRENT_TIME = "CURRENT_TIMESTAMP(6)";
  private static final String STATE_MATCH = String.format("%s = %s", BLOB_STATE, NamedBlobState.READY.ordinal());
  private static final String PK_MATCH = String.format("(%s, %s, %s) = (?, ?, ?)", ACCOUNT_ID, CONTAINER_ID, BLOB_NAME);
  private static final String PK_MATCH_VERSION = String.format("(%s, %s, %s, %s) = (?, ?, ?, ?)", ACCOUNT_ID,
      CONTAINER_ID, BLOB_NAME, VERSION);

  private static final Set<GetOption> includeDeletedOrExpiredOptions =
      new HashSet<>(Arrays.asList(GetOption.Include_All, GetOption.Include_Deleted_Blobs, GetOption.Include_Expired_Blobs));

  /**
   * Select a record that matches a blob name (lookup by primary key).
   */
  private static final String GET_QUERY_V2 =
      String.format("SELECT %s, %s, %s FROM %s WHERE %s AND %s ORDER BY %s DESC LIMIT 1", BLOB_ID, VERSION,
          DELETED_TS, NAMED_BLOBS_V2, PK_MATCH, STATE_MATCH, VERSION);

  /**
   * Below is the query for named blob list api
   * Which select records up to a specific limit where the blob name starts with a string prefix.
   * It contains two main parts:
   * 1. Pull out the max version for rows meeting below conditions:
   *     a). The account and container matches with user query
   *     b). The 'blob_state' is READY (1), not IN_PROGRESS (0)
   * 2. Use the result of step 1 to inner join with raw table on (account_id, container_id, blob_name, version),
   *    with filter on the blob_name, and order by blob_name.
   */
  // @formatter:off
  private static final String LIST_QUERY_V2 = String.format(""
          + "SELECT t1.blob_name, t1.blob_id, t1.version, t1.deleted_ts "
          + "FROM named_blobs_v2 t1 "
          + "INNER JOIN "
          + "(SELECT account_id, container_id, blob_name, max(version) as version "
          + "FROM named_blobs_v2 "
          + "WHERE (account_id, container_id) = (?, ?) AND %1$s "
          + "        GROUP BY account_id, container_id, blob_name) t2 "
          + "ON (t1.account_id,t1.container_id,t1.blob_name,t1.version) = (t2.account_id,t2.container_id,t2.blob_name,t2.version) "
          + "WHERE t1.blob_name LIKE ? AND t1.blob_name >= ? ORDER BY t1.blob_name ASC LIMIT ?",STATE_MATCH);
  // @formatter:on

  /**
   * Attempt to insert a new mapping into the database.
   */
  private static final String INSERT_QUERY_V2 =
      String.format("INSERT INTO %1$s (%2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s) VALUES (?, ?, ?, ?, ?, ?, ?)",
          NAMED_BLOBS_V2, ACCOUNT_ID, CONTAINER_ID, BLOB_NAME, BLOB_ID, DELETED_TS, VERSION, BLOB_STATE);

  /**
   * Find if there is currently a record present for a blob and acquire an exclusive lock in preparation for a delete.
   * This select call also allows the current blob ID to be retrieved prior to a delete.
   */
  private static final String SELECT_FOR_SOFT_DELETE_QUERY_V2 =
      String.format("SELECT %s, %s, %s, %s FROM %s WHERE %s AND %s ORDER BY %s DESC LIMIT 1 FOR UPDATE", BLOB_ID,
          VERSION, DELETED_TS, CURRENT_TIME, NAMED_BLOBS_V2, PK_MATCH, STATE_MATCH, VERSION);

  /**
   * Soft delete a blob by setting the delete timestamp to the current time.
   */
  private static final String SOFT_DELETE_QUERY_V2 =
      String.format("UPDATE %s SET %s = ? WHERE %s", NAMED_BLOBS_V2, DELETED_TS, PK_MATCH_VERSION);

  private final AccountService accountService;
  private final String localDatacenter;
  private final List<String> remoteDatacenters;
  private final RetryExecutor retryExecutor;
  private final Map<String, TransactionExecutor> transactionExecutors;
  private final MySqlNamedBlobDbConfig config;
  private final Metrics metricsRecoder;

  MySqlNamedBlobDb(AccountService accountService, MySqlNamedBlobDbConfig config, DataSourceFactory dataSourceFactory,
      String localDatacenter, MetricRegistry metricRegistry) {
    this.accountService = accountService;
    this.config = config;
    this.localDatacenter = localDatacenter;
    this.retryExecutor = new RetryExecutor(null);
    this.transactionExecutors = MySqlUtils.getDbEndpointsPerDC(config.dbInfo)
        .values()
        .stream()
        .flatMap(List::stream)
        .filter(DbEndpoint::isWriteable)
        .collect(Collectors.toMap(DbEndpoint::getDatacenter,
            dbEndpoint -> new TransactionExecutor(dbEndpoint.getDatacenter(),
                dataSourceFactory.getDataSource(dbEndpoint),
                localDatacenter.equals(dbEndpoint.getDatacenter()) ? config.localPoolSize : config.remotePoolSize)));
    this.remoteDatacenters = MySqlUtils.getRemoteDcFromDbInfo(config.dbInfo, localDatacenter);
    this.metricsRecoder = new MySqlNamedBlobDb.Metrics(metricRegistry);
  }

  private static class Metrics {
    public final Counter namedDataNotFoundGetCount;
    public final Counter namedDataErrorGetCount;
    public final Counter namedDataInconsistentGetCount;

    public final Counter namedDataInconsistentListCount;

    public final Counter namedDataNotFoundDeleteCount;
    public final Counter namedDataErrorDeleteCount;
    public final Counter namedDataInconsistentDeleteCount;

    public final Counter namedDataErrorPutCount;

    public final Histogram namedBlobGetTimeInMs;
    public final Histogram namedBlobListTimeInMs;
    public final Histogram namedBlobPutTimeInMs;
    public final Histogram namedBlobDeleteTimeInMs;

    /**
     * Constructor to create the Metrics.
     * @param metricRegistry The {@link MetricRegistry}.
     */
    public Metrics(MetricRegistry metricRegistry) {
      namedDataNotFoundGetCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataNotFoundGetCount"));
      namedDataErrorGetCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataErrorGetCount"));
      namedDataInconsistentGetCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataInconsistentGetCount"));

      namedDataInconsistentListCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataInconsistentListCount"));

      namedDataNotFoundDeleteCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataNotFoundDeleteCount"));
      namedDataErrorDeleteCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataErrorDeleteCount"));
      namedDataInconsistentDeleteCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataInconsistentDeleteCount"));

      namedDataErrorPutCount = metricRegistry.counter(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedDataErrorPutCount"));

      namedBlobGetTimeInMs = metricRegistry.histogram(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedBlobGetTimeInMs"));
      namedBlobListTimeInMs = metricRegistry.histogram(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedBlobListTimeInMs"));
      namedBlobPutTimeInMs = metricRegistry.histogram(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedBlobPutTimeInMs"));
      namedBlobDeleteTimeInMs = metricRegistry.histogram(
          MetricRegistry.name(MySqlNamedBlobDb.class, "NamedBlobDeleteTimeInMs"));
    }
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName,
      GetOption option) {
    TransactionStateTracker transactionStateTracker =
        new GetTransactionStateTracker(remoteDatacenters, localDatacenter);
    return executeTransactionAsync(accountName, containerName, true, (accountId, containerId, connection) -> {
      long startTime = System.currentTimeMillis();
      NamedBlobRecord record = run_get_v2(accountName, containerName, blobName, option, accountId, containerId, connection);
      metricsRecoder.namedBlobGetTimeInMs.update(System.currentTimeMillis() - startTime);
      return record;
    }, transactionStateTracker);
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken) {
    return executeTransactionAsync(accountName, containerName, true, (accountId, containerId, connection) -> {
      long startTime = System.currentTimeMillis();
      Page<NamedBlobRecord> recordPage = run_list_v2(accountName, containerName, blobNamePrefix, pageToken, accountId, containerId, connection);
      metricsRecoder.namedBlobListTimeInMs.update(System.currentTimeMillis() - startTime);
      return recordPage;
    }, null);
  }

  @Override
  public CompletableFuture<PutResult> put(NamedBlobRecord record, NamedBlobState state, Boolean isUpsert) {
    return executeTransactionAsync(record.getAccountName(), record.getContainerName(), true,
        (accountId, containerId, connection) -> {
          long startTime = System.currentTimeMillis();
          // Do upsert when it's using new table and 'x-ambry-named-upsert' header is not set to false (default is true)
          logger.trace("NamedBlobPutInfo: accountId='{}', containerId='{}', blobName='{}', dbRelyOnNewTable='{}', isUpsert='{}'",
              accountId, containerId, record.getBlobName(), config.dbRelyOnNewTable, isUpsert);
          if (!(config.dbRelyOnNewTable && isUpsert)) {
            NamedBlobRecord recordCurrent = null;
            try {
              recordCurrent = run_get_v2(record.getAccountName(), record.getContainerName(), record.getBlobName(),
                  GetOption.None, accountId, containerId, connection);
            } catch (RestServiceException e) {
              logger.trace("Skip exception in pulling data from db: accountId='{}', containerId='{}', blobName='{}': {}",
                  accountId, containerId, record.getBlobName(), e);
            }
            if (recordCurrent != null) {
              throw buildException("PUT: Blob still alive", RestServiceErrorCode.Conflict, record.getAccountName(),
                  record.getContainerName(), record.getBlobName());
            }
          }
          PutResult putResult = run_put_v2(record, state, accountId, containerId, connection);
          metricsRecoder.namedBlobPutTimeInMs.update(System.currentTimeMillis() - startTime);
          return putResult;
        }, null);
  }

  @Override
  public CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName) {
    return executeTransactionAsync(accountName, containerName, false, (accountId, containerId, connection) -> {
      long startTime = System.currentTimeMillis();
      DeleteResult deleteResult = run_delete_v2(accountName, containerName, blobName, accountId, containerId, connection);
      metricsRecoder.namedBlobDeleteTimeInMs.update(System.currentTimeMillis() - startTime);
      return deleteResult;
    }, null);
  }

  /**
   * Run a transaction on a thread pool and handle common logic surrounding looking up account metadata and error
   * handling. Eventually this will handle retries.
   * @param <T> the return type of the {@link Transaction}.
   * @param accountName the account name for the transaction.
   * @param containerName the container name for the transaction.
   * @param autoCommit true if each statement execution should be its own transaction. If set to false, this helper will
   *                   handle calling commit/rollback.
   * @param transaction the {@link Transaction} to run. This can either be a read only query or include DML.
   * @param transactionStateTracker the {@link TransactionStateTracker} to describe the retry strategy.
   * @return a {@link CompletableFuture} that will eventually contain the result of the transaction or an exception.
   */
  private <T> CompletableFuture<T> executeTransactionAsync(String accountName, String containerName, boolean autoCommit,
      Transaction<T> transaction, TransactionStateTracker transactionStateTracker) {
    CompletableFuture<T> future = new CompletableFuture<>();
    // Look up account and container IDs. This is common logic needed for all types of transactions.
    Account account = accountService.getAccountByName(accountName);
    if (account == null) {
      future.completeExceptionally(
          new RestServiceException("Account not found: " + accountName, RestServiceErrorCode.NotFound));
      return future;
    }
    Container container = account.getContainerByName(containerName);
    if (container == null) {
      future.completeExceptionally(
          new RestServiceException("Container not found: " + containerName, RestServiceErrorCode.NotFound));
      return future;
    }

    Callback<T> finalCallback = (result, exception) -> {
      if (exception != null) {
        future.completeExceptionally(exception);
      } else {
        future.complete(result);
      }
    };
    // TODO consider introducing CompletableFuture support in RetryExecutor so that we can use only futures, no callback
    if (transactionStateTracker != null) {
      retryExecutor.runWithRetries(RetryPolicies.fixedBackoffPolicy(transactionExecutors.size(), 0), callback -> {
        String datacenter = transactionStateTracker.getNextDatacenter();
        transactionExecutors.get(datacenter).executeTransaction(container, autoCommit, transaction, callback);
      }, transactionStateTracker::processFailure, finalCallback);
    } else {
      transactionExecutors.get(localDatacenter).executeTransaction(container, autoCommit, transaction, finalCallback);
    }
    return future;
  }

  /**
   * Execute transaction on datacenter.
   */
  private static class TransactionExecutor implements Closeable {
    private final DataSource dataSource;
    private final ExecutorService executor;

    TransactionExecutor(String datacenter, DataSource dataSource, int numThreads) {
      this.dataSource = dataSource;
      executor = Utils.newScheduler(numThreads, "Thread-" + datacenter, false);
    }

    <T> void executeTransaction(Container container, boolean autoCommit, Transaction<T> transaction,
        Callback<T> callback) {
      executor.submit(() -> {
        try (Connection connection = dataSource.getConnection()) {
          T result;
          if (autoCommit) {
            result = transaction.run(container.getParentAccountId(), container.getId(), connection);
          } else {
            // if autocommit is set to false, treat this as a multi-step txn that requires an explicit commit/rollback
            connection.setAutoCommit(false);
            try {
              result = transaction.run(container.getParentAccountId(), container.getId(), connection);
              connection.commit();
            } catch (Exception e) {
              connection.rollback();
              throw e;
            } finally {
              connection.setAutoCommit(true);
            }
          }
          callback.onCompletion(result, null);
        } catch (Exception e) {
          callback.onCompletion(null, e);
        }
      });
    }

    public DataSource getDataSource() {
      return dataSource;
    }

    @Override
    public void close() {
      Utils.shutDownExecutorService(executor, 1, TimeUnit.MINUTES);
    }
  }

  /**
   * Exposed for integration test usage.
   * @return a map from datacenter name to {@link DataSource}.
   */
  Map<String, DataSource> getDataSources() {
    return transactionExecutors.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDataSource()));
  }

  private NamedBlobRecord run_get_v2(String accountName, String containerName, String blobName, GetOption option, short accountId,
      short containerId, Connection connection) throws Exception {
    try (PreparedStatement statement = connection.prepareStatement(GET_QUERY_V2)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, blobName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw buildException("GET: Blob not found", RestServiceErrorCode.NotFound, accountName, containerName,
              blobName);
        }
        String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
        long version = resultSet.getLong(2);
        Timestamp deletionTime = resultSet.getTimestamp(3);
        long currentTime = System.currentTimeMillis();
        if (compareTimestamp(deletionTime, currentTime) <= 0 && !includeDeletedOrExpiredOptions.contains(option)) {
          throw buildException("GET: Blob is not available due to it is deleted or expired",
              RestServiceErrorCode.Deleted, accountName, containerName, blobName);
        } else {
          return new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(deletionTime),
              version);
        }
      }
    }
  }

  private Page<NamedBlobRecord> run_list_v2(String accountName, String containerName, String blobNamePrefix,
      String pageToken, short accountId, short containerId, Connection connection) throws Exception {
    try (PreparedStatement statement = connection.prepareStatement(LIST_QUERY_V2)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, blobNamePrefix + "%");
      statement.setString(4, pageToken != null ? pageToken : blobNamePrefix);
      statement.setInt(5, config.listMaxResults + 1);
      try (ResultSet resultSet = statement.executeQuery()) {
        String nextContinuationToken = null;
        List<NamedBlobRecord> entries = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        int resultIndex = 0;
        while (resultSet.next()) {
          if (resultIndex++ == config.listMaxResults) {
            nextContinuationToken = resultSet.getString(1);
            break;
          }
          String blobName = resultSet.getString(1);
          String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(2));
          long version = resultSet.getLong(3);
          Timestamp deletionTime = resultSet.getTimestamp(4);

          if (compareTimestamp(deletionTime, currentTime) <= 0) {
            logger.trace("LIST: Blob is not available due to it is deleted or expired, ignoring in list response; account='{}', container='{}', name='{}'",
                accountName, containerName, blobName);
          } else {
            entries.add(
                new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(deletionTime), version));
          }
        }
        return new Page<>(entries, nextContinuationToken);
      }
    }
  }

  private PutResult run_put_v2(NamedBlobRecord record, NamedBlobState state, short accountId, short containerId, Connection connection)
      throws Exception {
    // 1. Attempt to insert into the table. This is attempted first since it is the most common case.
    try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY_V2)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, record.getBlobName());
      statement.setBytes(4, Base64.decodeBase64(record.getBlobId()));
      if (record.getExpirationTimeMs() != Utils.Infinite_Time) {
        statement.setTimestamp(5, new Timestamp(record.getExpirationTimeMs()));
      } else {
        statement.setTimestamp(5, null);
      }
      statement.setLong(6, buildVersion());
      statement.setInt(7, state.ordinal());
      statement.executeUpdate();
    }
    return new PutResult(record);
  }

  private DeleteResult run_delete_v2(String accountName, String containerName, String blobName, short accountId,
      short containerId, Connection connection) throws Exception {
    String blobId;
    long version;
    Timestamp currentTime;
    boolean alreadyDeleted;
    try (PreparedStatement statement = connection.prepareStatement(SELECT_FOR_SOFT_DELETE_QUERY_V2)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, blobName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw buildException("DELETE: Blob not found", RestServiceErrorCode.NotFound, accountName, containerName,
              blobName);
        }
        blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
        version = resultSet.getLong(2);
        Timestamp deletionTime = resultSet.getTimestamp(3);
        currentTime = resultSet.getTimestamp(4);
        alreadyDeleted = (deletionTime != null && currentTime.after(deletionTime));
      }
    }
    // only need to issue an update statement if the row was not already marked as deleted.
    if (!alreadyDeleted) {
      try (PreparedStatement statement = connection.prepareStatement(SOFT_DELETE_QUERY_V2)) {
        // use the current time
        statement.setTimestamp(1, currentTime);
        statement.setInt(2, accountId);
        statement.setInt(3, containerId);
        statement.setString(4, blobName);
        statement.setLong(5, version);
        statement.executeUpdate();
      }
    }
    return new DeleteResult(blobId, alreadyDeleted);
  }

  /**
   * Build the version for Named Blob row based on timestamp and uuid postfix.
   * @return a long number whose rightmost 5 digits are uuid postfix, and the remaining digits are current timestamp
   */
  private static long buildVersion() {
    long currentTime = System.currentTimeMillis();
    UUID uuid = UUID.randomUUID();
    int base = 100000;
    return currentTime * base + Long.parseLong(uuid.toString().split("-")[0],16) % base;
  }

  /**
   * Compare a nullable timestamp against {@code otherTimeMs}.
   * @param timestamp the nullable {@link Timestamp} to compare.
   * @param otherTimeMs the time value to compare against.
   * @return -1 if the timestamp is earlier than {@code otherTimeMs}, 0 if the times are equal, and 1 if
   *         {@code otherTimeMs} is later than the timestamp. {@code null} is considered greater than any other time.
   */
  private static int compareTimestamp(Timestamp timestamp, long otherTimeMs) {
    return Utils.compareTimes(timestampToMs(timestamp), otherTimeMs);
  }

  /**
   * @param timestamp a {@link Timestamp}, can be null.
   * @return the milliseconds since the epoch if {@code timestamp} is non-null, or {@link Utils#Infinite_Time} if null.
   */
  private static long timestampToMs(Timestamp timestamp) {
    return timestamp == null ? Utils.Infinite_Time : timestamp.getTime();
  }

  private static RestServiceException buildException(String message, RestServiceErrorCode errorCode, String accountName,
      String containerName, String blobName) {
    return new RestServiceException(
        message + "; account='" + accountName + "', container='" + containerName + "', name='" + blobName + "'",
        errorCode);
  }

  /**
   * An interface that represents an action performed on an open database connection.
   * @param <T> the return type of the action.
   */
  private interface Transaction<T> {
    /**
     * @param accountId the account ID for this transaction.
     * @param containerId the container ID for this transaction.
     * @param connection the database connection to use.
     * @return the result of this transaction.
     * @throws Exception if there is an error.
     */
    T run(short accountId, short containerId, Connection connection) throws Exception;
  }

  /**
   * A factory that produces a configured {@link DataSource} based on supplied configs.
   */
  interface DataSourceFactory {
    /**
     * @param dbEndpoint {@link DbEndpoint} object containing the database connection settings to use.
     * @return an instance of {@link DataSource} for the provided {@link DbEndpoint}.
     */
    DataSource getDataSource(DbEndpoint dbEndpoint);
  }
}
