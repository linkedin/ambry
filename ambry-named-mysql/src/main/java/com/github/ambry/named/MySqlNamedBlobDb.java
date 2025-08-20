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
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
public class MySqlNamedBlobDb implements NamedBlobDb {
  private static final Logger logger = LoggerFactory.getLogger(MySqlNamedBlobDb.class);
  private static final int MAX_NUMBER_OF_VERSIONS_IN_DELETE = 1000;
  private static final int VERSION_BASE = 100000;

  private final Time time;
  private static final String MULTI_VERSION_PLACE_HOLDER = "MULTI_VERSION_PLACE_HOLDER";
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
  private static final String MODIFIED_TS = "modified_ts";
  private static final String BLOB_SIZE = "blob_size";

  // query building blocks
  private static final String CURRENT_TIME = "UTC_TIMESTAMP(6)";
  private static final String STATE_MATCH = String.format("%s = %s", BLOB_STATE, NamedBlobState.READY.ordinal());
  private static final String PK_MATCH = String.format("(%s, %s, %s) = (?, ?, ?)", ACCOUNT_ID, CONTAINER_ID, BLOB_NAME);
  private static final String PK_MATCH_VERSION =
      String.format("(%s, %s, %s, %s) = (?, ?, ?, ?)", ACCOUNT_ID, CONTAINER_ID, BLOB_NAME, VERSION);

  private static final Set<GetOption> includeDeletedOrExpiredOptions = new HashSet<>(
      Arrays.asList(GetOption.Include_All, GetOption.Include_Deleted_Blobs, GetOption.Include_Expired_Blobs));

  /**
   * Select a record that matches a blob name (lookup by primary key).
   */
  private static final String GET_QUERY =
      String.format("SELECT %s, %s, %s FROM %s WHERE %s AND %s ORDER BY %s DESC LIMIT 1", BLOB_ID, VERSION, DELETED_TS,
          NAMED_BLOBS_V2, PK_MATCH, STATE_MATCH, VERSION);

  private final String LIST_WITH_PREFIX_SQL;

  /**
   * Similar like LIST_QUERY_V2, but this query is used when user don't provide the prefix and want to list all records.
   */
  // @formatter:off
  private static final String LIST_ALL_QUERY = String.format(""
      + "SELECT t1.blob_name, t1.blob_id, t1.version, t1.deleted_ts, t1.blob_size, t1.modified_ts "
      + "FROM named_blobs_v2 t1 "
      + "INNER JOIN "
      + "(SELECT account_id, container_id, blob_name, max(version) as version "
      + "FROM named_blobs_v2 "
      + "WHERE (account_id, container_id) = (?, ?) AND %1$s "
      + "  AND (deleted_ts IS NULL OR deleted_ts>%2$S) "
      + "        GROUP BY account_id, container_id, blob_name) t2 "
      + "ON (t1.account_id,t1.container_id,t1.blob_name,t1.version) = (t2.account_id,t2.container_id,t2.blob_name,t2.version) "
      + "WHERE "
      + "  CASE "
      + "     WHEN ? IS NOT NULL THEN t1.blob_name >= ? "
      + "     ELSE 1 "
      + "   END "
      + "ORDER BY t1.blob_name ASC LIMIT ?",STATE_MATCH, CURRENT_TIME);
  // @formatter:on

  /**
   * Attempt to insert a new mapping into the database. The 'modified_ts' column in the DB will be auto-populated on
   * the server side
   */
  private static final String INSERT_QUERY =
      String.format("INSERT INTO %1$s (%2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s, %9$s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          NAMED_BLOBS_V2, ACCOUNT_ID, CONTAINER_ID, BLOB_NAME, BLOB_ID, DELETED_TS, VERSION, BLOB_STATE, BLOB_SIZE);

  /**
   * Find if there is currently a record present for a blob and acquire an exclusive lock in preparation for a delete.
   * This select call also allows the current blob ID to be retrieved prior to a delete. Notice that the lock will not
   * prevent other transactions from inserting new records for the same blob name. So when deleting the rows, we need to
   * be specific about the version to delete.
   */
  private static final String SELECT_FOR_DELETE_QUERY =
      String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s ORDER BY %s DESC FOR UPDATE", BLOB_ID, VERSION,
          DELETED_TS, CURRENT_TIME, BLOB_STATE, NAMED_BLOBS_V2, PK_MATCH, VERSION);

  /**
   * Soft delete a blob by setting the delete timestamp to the current time for a specific version.
   */
  private static final String SOFT_DELETE_WITH_VERSION_QUERY =
      String.format("UPDATE %s SET %s = ? WHERE %s", NAMED_BLOBS_V2, DELETED_TS, PK_MATCH_VERSION);

  /**
   * A sql template to soft delete multiple versions of a blob name. Multiple versions will be passed in as a
   * comma separated string and replace the MULTI_VERSION_PLACE_HOLDER.
   */
  private static final String SOFT_DELETE_MULTIPLE_VERSIONS_QUERY_TEMPLATE =
      String.format("UPDATE %s SET %s = ? WHERE %s AND %s IN ( " + MULTI_VERSION_PLACE_HOLDER + ")", NAMED_BLOBS_V2,
          DELETED_TS, PK_MATCH, VERSION);
  /**
   * A sql template to hard delete multiple versions of a blob name.Multiple versions will be passed in as a
   * comma separated string and replace the MULTI_VERSION_PLACE_HOLDER.
   */
  private static final String HARD_DELETE_MULTIPLE_VERSIONS_QUERY_TEMPLATE =
      String.format("DELETE FROM %s WHERE %s AND %s IN ( " + MULTI_VERSION_PLACE_HOLDER + ")", NAMED_BLOBS_V2, PK_MATCH,
          VERSION);

  /**
   * Set named blob state to be READY and delete timestamp to null for TtlUpdate case
   */
  private static final String TTL_UPDATE_QUERY =
      String.format("UPDATE %s SET %s, %s = NULL WHERE %s", NAMED_BLOBS_V2, STATE_MATCH, DELETED_TS, PK_MATCH_VERSION);

  private static final String GET_BLOBS_FOR_CONTAINER = String.format(
      "SELECT %s, %s, %s, %s, %s, %s, %s, %s " + "FROM %s "
          + "WHERE container_id = ? AND account_id = ? AND blob_name >= ? AND ( deleted_ts IS NULL or deleted_ts > UTC_TIMESTAMP()) "
          + "ORDER BY %s ASC, %s DESC " + "LIMIT ?", ACCOUNT_ID, CONTAINER_ID, BLOB_NAME, BLOB_ID, VERSION, BLOB_STATE,
      MODIFIED_TS, DELETED_TS, NAMED_BLOBS_V2, BLOB_NAME, VERSION);

  private static final String GET_MINIMUM_STALE_BLOB_COUNT = String.format(
      "SELECT COUNT(*) AS single_occurrence_ready_count " +
          "FROM ( " +
          "    SELECT %s " +
          "    FROM %s " +
          "    WHERE container_id = ? " +
          "      AND account_id = ? " +
          "      AND blob_state = ? " +
          "    GROUP BY %s, %s, %s " +
          "    HAVING COUNT(*) = 1 " +
          ") AS sub",
      BLOB_NAME, NAMED_BLOBS_V2, BLOB_NAME, CONTAINER_ID, ACCOUNT_ID
  );

  private final AccountService accountService;
  private final String localDatacenter;
  private final List<String> remoteDatacenters;
  private final RetryExecutor retryExecutor;
  private final Map<String, TransactionExecutor> transactionExecutors;
  private final MySqlNamedBlobDbConfig config;
  private final Metrics metricsRecoder;

  MySqlNamedBlobDb(AccountService accountService, MySqlNamedBlobDbConfig config, DataSourceFactory dataSourceFactory,
      String localDatacenter, Metrics metricRecorder, Time time) {
    this.accountService = accountService;
    this.config = config;
    this.LIST_WITH_PREFIX_SQL = getListWithPrefixSQLStatement(config);
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
    this.metricsRecoder = metricRecorder;
    this.time = time;
  }

  MySqlNamedBlobDb(AccountService accountService, MySqlNamedBlobDbConfig config, DataSourceFactory dataSourceFactory,
      String localDatacenter, Metrics metricRecorder) {
    this(accountService, config, dataSourceFactory, localDatacenter, metricRecorder, SystemTime.getInstance());
  }

  private String getListWithPrefixSQLStatement(MySqlNamedBlobDbConfig config) {
    switch (config.listNamedBlobsSQLOption) {
      case 2:
        /**
         * List named-blobs query, given a prefix.
         * The first query selects all versions of blobs with a given prefix that are not deleted, from a given account and container.
         * The second query selects the most recent version of blobs with a given prefix, from a given account and container.
         * Finally, we join and select a version for each blob, that is ready to serve.
         * This can be the most recent version if it is not deleted, or nothing.
         */
        // @formatter:off
        return String.format(""
            + " WITH "
            + "  BlobsAllVersion AS ( "
            + "   SELECT blob_name, blob_id, version, deleted_ts, blob_size, modified_ts "
            + "   FROM named_blobs_v2 "
            + "   WHERE account_id = ? " // 1
            + "     AND container_id = ? " // 2
            + "     AND %1$s " // blob_state = x
            + "     AND blob_name LIKE ? " // 3
            + "     AND blob_name >= ? " // 4
            + "     AND (deleted_ts IS NULL OR deleted_ts > %2$s) "
            + " ), "
            + " BlobsMaxVersion AS ( "
            + "   SELECT blob_name, MAX(version) as version "
            + "   FROM named_blobs_v2 "
            + "   WHERE account_id = ? " // 5
            + "     AND container_id = ? " // 6
            + "     AND %1$s " // blob_state = x
            + "     AND blob_name LIKE ? " // 7
            + "     AND blob_name >= ? " // 8
            + "   GROUP BY blob_name "
            + " ) "
            + " SELECT BlobsAllVersion.* "
            + " FROM BlobsAllVersion "
            + " INNER JOIN BlobsMaxVersion "
            + " ON (BlobsAllVersion.blob_name = BlobsMaxVersion.blob_name "
            + "   AND BlobsAllVersion.version = BlobsMaxVersion.version) "
            + " ORDER BY BlobsAllVersion.blob_name "
            + " LIMIT ?", STATE_MATCH, CURRENT_TIME); // 9
        // @formatter:on
      case 3:
        /**
         * List named-blobs query, given a prefix.
         * This query uses a subquery to get the max version of a given blob name.
         * 1. The subquery returns the max version whose blob_state is ready, even if it's deleted.
         * 2. The outer query select goes through all the (blob_name, version) combination and only returns the one when
         *    version is the max version from the subquery and not deleted.
         * This list sql statement uses subquery instead of CTE and inner join so it should be more efficient.
         */
        // @formatter:off
        return String.format(""
            + "SELECT candidate.blob_name, candidate.blob_id, candidate.version, candidate.deleted_ts, candidate.blob_size, candidate.modified_ts "
            + "FROM named_blobs_v2 candidate "
            + "WHERE candidate.account_id = ? "
            + "    AND candidate.container_id = ? "
            + "    AND candidate.%1$s"
            + "    AND candidate.blob_name LIKE ? "
            + "    AND candidate.blob_name >= ? "
            + "    AND (candidate.deleted_ts IS NULL OR candidate.deleted_ts > %2$s) "
            + "    AND candidate.version = ( "
            + "        SELECT MAX(latest.version) "
            + "        FROM named_blobs_v2 latest "
            + "        WHERE latest.account_id = ? "
            + "            AND latest.container_id = ? "
            + "            AND latest.blob_name = candidate.blob_name "
            + "            AND latest.%1$s"
            + "    ) "
            + "LIMIT ?", STATE_MATCH, CURRENT_TIME);
      // @formatter:on
      default:
        throw new IllegalArgumentException("Invalid listNamedBlobsSQLOption: " + config.listNamedBlobsSQLOption);
    }
  }

  @Override
  public void close() throws IOException {
    this.transactionExecutors.values().forEach(TransactionExecutor::close);
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName,
      GetOption option, boolean localGet) {
    TransactionStateTracker transactionStateTracker = null;
    if (!localGet) {
      transactionStateTracker = new GetTransactionStateTracker(remoteDatacenters, localDatacenter);
    }
    return executeTransactionAsync(accountName, containerName, true, (accountId, containerId, connection) -> {
      long startTime = this.time.milliseconds();
      NamedBlobRecord record =
          run_get_v2(accountName, containerName, blobName, option, accountId, containerId, connection);
      metricsRecoder.namedBlobGetTimeInMs.update(this.time.milliseconds() - startTime);
      return record;
    }, transactionStateTracker, this.metricsRecoder.namedBlobDBGetErrorCount);
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken, Integer maxKeys) {
    return executeTransactionAsync(accountName, containerName, true, (accountId, containerId, connection) -> {
      long startTime = this.time.milliseconds();
      Page<NamedBlobRecord> recordPage =
          run_list_v2(accountName, containerName, blobNamePrefix, pageToken, accountId, containerId, connection,
              maxKeys);
      metricsRecoder.namedBlobListTimeInMs.update(this.time.milliseconds() - startTime);
      return recordPage;
    }, null, this.metricsRecoder.namedBlobDBListErrorCount);
  }

  @Override
  public CompletableFuture<PutResult> put(NamedBlobRecord record, NamedBlobState state, Boolean isUpsert) {
    return executeTransactionAsync(record.getAccountName(), record.getContainerName(), true,
        (accountId, containerId, connection) -> {
          long startTime = this.time.milliseconds();
          // Do upsert when it's using new table and 'x-ambry-named-upsert' header is not set to false (default is true)
          logger.trace("NamedBlobPutInfo: accountId='{}', containerId='{}', blobName='{}', isUpsert='{}'", accountId,
              containerId, record.getBlobName(), isUpsert);
          if (!isUpsert) {
            NamedBlobRecord recordCurrent = null;
            try {
              recordCurrent =
                  run_get_v2(record.getAccountName(), record.getContainerName(), record.getBlobName(), GetOption.None,
                      accountId, containerId, connection);
            } catch (RestServiceException e) {
              logger.trace(
                  "Skip exception in pulling data from db: accountId='{}', containerId='{}', blobName='{}': {}",
                  accountId, containerId, record.getBlobName(), e);
            }
            if (recordCurrent != null) {
              logger.error(
                  "PUT conflict: Named blob {} already exist, the existing blob id is {}, the new blob id is {}",
                  record.getBlobName(), recordCurrent.getBlobId(), record.getBlobId());
              throw buildException("PUT: Blob still alive", RestServiceErrorCode.Conflict, record.getAccountName(),
                  record.getContainerName(), record.getBlobName());
            }
          }
          PutResult putResult = run_put_v2(record, state, accountId, containerId, connection);
          metricsRecoder.namedBlobPutTimeInMs.update(this.time.milliseconds() - startTime);
          return putResult;
        }, null, this.metricsRecoder.namedBlobDBInsertErrorCount);
  }

  @Override
  public CompletableFuture<PutResult> updateBlobTtlAndStateToReady(NamedBlobRecord record) {
    return executeTransactionAsync(record.getAccountName(), record.getContainerName(), true,
        (accountId, containerId, connection) -> {
          long startTime = this.time.milliseconds();
          logger.trace("Updating ttl and status to READY for Named Blob: {}", record);
          PutResult result = apply_ttl_update(record, accountId, containerId, connection);
          metricsRecoder.namedTtlupdateTimeInMs.update(this.time.milliseconds() - startTime);
          return result;
        }, null, this.metricsRecoder.namedBlobDBUpdateErrorCount);
  }

  @Override
  public CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName) {
    return executeTransactionAsync(accountName, containerName, false, (accountId, containerId, connection) -> {
      long startTime = this.time.milliseconds();
      DeleteResult deleteResult =
          run_delete_v2(accountName, containerName, blobName, accountId, containerId, connection);
      metricsRecoder.namedBlobDeleteTimeInMs.update(this.time.milliseconds() - startTime);
      return deleteResult;
    }, null, this.metricsRecoder.namedBlobDBDeleteErrorCount);
  }

  @Override
  public CompletableFuture<StaleBlobsWithLatestBlobName> pullStaleBlobs(Container container, String blobName) {
    TransactionStateTracker transactionStateTracker =
        new GetTransactionStateTracker(remoteDatacenters, localDatacenter);
    return executeGenericTransactionAsync(true, (connection) -> {
      long startTime = this.time.milliseconds();
      StaleBlobsWithLatestBlobName staleBlobsWithLatestBlobName = null;
      Boolean res = checkIfValidContainerForCleaning(connection, container);
      if (!(res)) {
        return new StaleBlobsWithLatestBlobName(new ArrayList<>(), null);
      }
      List<StaleNamedBlob> potentialStaleNamedBlobResults = getAllBlobsForContainer(connection, container, blobName);
      int resultSize = potentialStaleNamedBlobResults.size();
      if (resultSize == 0) {
        return new StaleBlobsWithLatestBlobName(potentialStaleNamedBlobResults, null);
      }

      Container.ContainerStatus status = container.getStatus();
      if (status == Container.ContainerStatus.ACTIVE) {
        staleBlobsWithLatestBlobName = getStaleBlobsForActiveContainer(potentialStaleNamedBlobResults, config.staleDataRetentionDays);
      } else {
        staleBlobsWithLatestBlobName = new StaleBlobsWithLatestBlobName(potentialStaleNamedBlobResults,
            potentialStaleNamedBlobResults.get(potentialStaleNamedBlobResults.size() - 1).getBlobName());
      }
      if (resultSize < config.queryStaleDataMaxResults) {
        staleBlobsWithLatestBlobName  = new StaleBlobsWithLatestBlobName(staleBlobsWithLatestBlobName.getStaleBlobs(), null);
      }

      metricsRecoder.namedBlobPullStaleTimeInMs.update(this.time.milliseconds() - startTime);
      return staleBlobsWithLatestBlobName;
    }, transactionStateTracker);
  }

  @Override
  public CompletableFuture<Integer> cleanupStaleData(List<StaleNamedBlob> staleRecords) {
    return executeGenericTransactionAsync(true, (connection) -> {
      long startTime = this.time.milliseconds();
      batchSoftDelete(staleRecords, connection);
      //TODO IN LATER PR: local hard delete
      metricsRecoder.namedBlobCleanupTimeInMs.update(this.time.milliseconds() - startTime);
      return staleRecords.size();
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
      Transaction<T> transaction, TransactionStateTracker transactionStateTracker, Counter dbErrorCounter) {
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
        if (exception instanceof SQLException) {
          dbErrorCounter.inc();
        }
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

  private <T> CompletableFuture<T> executeGenericTransactionAsync(boolean autoCommit, TransactionGeneric<T> transaction,
      TransactionStateTracker transactionStateTracker) {
    CompletableFuture<T> future = new CompletableFuture<>();

    Callback<T> finalCallback = (result, exception) -> {
      if (exception != null) {
        future.completeExceptionally(exception);
      } else {
        future.complete(result);
      }
    };

    if (transactionStateTracker != null) {
      retryExecutor.runWithRetries(RetryPolicies.fixedBackoffPolicy(transactionExecutors.size(), 0), callback -> {
        String datacenter = transactionStateTracker.getNextDatacenter();
        transactionExecutors.get(datacenter).executeTransactionGeneric(autoCommit, transaction, callback);
      }, transactionStateTracker::processFailure, finalCallback);
    } else {
      transactionExecutors.get(localDatacenter).executeTransactionGeneric(autoCommit, transaction, finalCallback);
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

    <T> void executeTransactionGeneric(boolean autoCommit, TransactionGeneric<T> transaction, Callback<T> callback) {
      executor.submit(() -> {
        try (Connection connection = dataSource.getConnection()) {
          T result;
          if (autoCommit) {
            result = transaction.run(connection);
          } else {
            // if autocommit is set to false, treat this as a multi-step txn that requires an explicit commit/rollback
            connection.setAutoCommit(false);
            try {
              result = transaction.run(connection);
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
      if (dataSource instanceof Closeable) {
        try {
          ((Closeable) dataSource).close();
        } catch (IOException e) {
          logger.error("Failed to close datasource", e);
        }
      }
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

  private NamedBlobRecord run_get_v2(String accountName, String containerName, String blobName, GetOption option,
      short accountId, short containerId, Connection connection) throws Exception {
    String query = "";
    try (PreparedStatement statement = connection.prepareStatement(GET_QUERY)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, blobName);
      query = statement.toString();
      logger.debug("Getting blob name from MySql. Query {}", query);
      metricsRecoder.namedBlobGetRate.mark();
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw buildException("GET: Blob not found", RestServiceErrorCode.NotFound, accountName, containerName,
              blobName);
        }
        String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
        long version = resultSet.getLong(2);
        Timestamp deletionTime = resultSet.getTimestamp(3);
        long currentTime = this.time.milliseconds();
        if (compareTimestamp(deletionTime, currentTime) <= 0 && !includeDeletedOrExpiredOptions.contains(option)) {
          throw buildException("GET: Blob is not available due to it is deleted or expired",
              RestServiceErrorCode.Deleted, accountName, containerName, blobName);
        } else {
          return new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(deletionTime),
              version);
        }
      }
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
  }

  private Page<NamedBlobRecord> run_list_v2(String accountName, String containerName, String blobNamePrefix,
      String pageToken, short accountId, short containerId, Connection connection, Integer maxKeys) throws Exception {
    String query = "";
    String queryStatement = blobNamePrefix == null ? LIST_ALL_QUERY : LIST_WITH_PREFIX_SQL;
    int maxKeysValue = maxKeys == null ? config.listMaxResults : maxKeys;
    try (PreparedStatement statement = connection.prepareStatement(queryStatement)) {
      if (blobNamePrefix == null) {
        constructListAllQuery(statement, accountId, containerId, pageToken, maxKeysValue);
      } else {
        if (config.listNamedBlobsSQLOption == MySqlNamedBlobDbConfig.MIN_LIST_NAMED_BLOBS_SQL_OPTION) {
          constructListQueryWithPrefixV2(statement, accountId, containerId, blobNamePrefix, pageToken, maxKeysValue);
        } else {
          constructListQueryWithPrefixV3(statement, accountId, containerId, blobNamePrefix, pageToken, maxKeysValue);
        }
      }
      query = statement.toString();
      logger.debug("Getting list of blobs matching prefix {} from MySql. Query {}", blobNamePrefix, query);
      metricsRecoder.namedBlobListRate.mark();
      try (ResultSet resultSet = statement.executeQuery()) {
        String nextContinuationToken = null;
        List<NamedBlobRecord> entries = new ArrayList<>();
        int resultIndex = 0;
        while (resultSet.next()) {
          String blobName = resultSet.getString(1);
          if (resultIndex++ == maxKeysValue) {
            nextContinuationToken = blobName;
            break;
          }
          String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(2));
          long version = resultSet.getLong(3);
          Timestamp deletionTime = resultSet.getTimestamp(4);
          long blobSize = resultSet.getLong(5);
          Timestamp modifiedTime = resultSet.getTimestamp(6);
          entries.add(
              new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(deletionTime), version,
                  blobSize, timestampToMs(modifiedTime), false));
        }
        return new Page<>(entries, nextContinuationToken);
      }
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
  }

  /**
   * Construct a list all query statement
   * @param statement The {@link PreparedStatement} to set the parameters on.
   * @param accountId The account id
   * @param containerId The container id
   * @param pageToken The page token
   * @param maxKeysValue The max key to return
   * @throws SQLException
   */
  private void constructListAllQuery(PreparedStatement statement, short accountId, short containerId, String pageToken,
      int maxKeysValue) throws SQLException {
    // list-all no prefix
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, pageToken);
    statement.setString(4, pageToken);
    statement.setInt(5, maxKeysValue + 1);
  }

  /**
   * Construct a list query statement with prefix when {@link MySqlNamedBlobDbConfig#listNamedBlobsSQLOption} is 2
   * @param statement The {@link PreparedStatement} to set the parameters on.
   * @param accountId The account id
   * @param containerId The container id
   * @param blobNamePrefix The blobname prefix
   * @param pageToken The page token
   * @param maxKeysValue The max key to return
   * @throws SQLException
   */
  private void constructListQueryWithPrefixV2(PreparedStatement statement, short accountId, short containerId,
      String blobNamePrefix, String pageToken, int maxKeysValue) throws SQLException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, blobNamePrefix + "%");
    statement.setString(4, pageToken != null ? pageToken : blobNamePrefix);
    statement.setInt(5, accountId);
    statement.setInt(6, containerId);
    statement.setString(7, blobNamePrefix + "%");
    statement.setString(8, pageToken != null ? pageToken : blobNamePrefix);
    statement.setInt(9, maxKeysValue + 1);
  }

  /**
   * Construct a list query statement with prefix when {@link MySqlNamedBlobDbConfig#listNamedBlobsSQLOption} is 3
   * @param statement The {@link PreparedStatement} to set the parameters on.
   * @param accountId The account id
   * @param containerId The container id
   * @param blobNamePrefix The blobname prefix
   * @param pageToken The page token
   * @param maxKeysValue The max key to return
   * @throws SQLException
   */
  private void constructListQueryWithPrefixV3(PreparedStatement statement, short accountId, short containerId,
      String blobNamePrefix, String pageToken, int maxKeysValue) throws SQLException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, blobNamePrefix + "%");
    statement.setString(4, pageToken != null ? pageToken : blobNamePrefix);
    statement.setInt(5, accountId);
    statement.setInt(6, containerId);
    statement.setInt(7, maxKeysValue + 1);
  }

  private PutResult run_put_v2(NamedBlobRecord record, NamedBlobState state, short accountId, short containerId,
      Connection connection) throws Exception {
    String query = "";
    NamedBlobRecord updatedRecord;
    try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, record.getBlobName());
      statement.setBytes(4, Base64.decodeBase64(record.getBlobId()));
      if (record.getExpirationTimeMs() != Utils.Infinite_Time) {
        statement.setTimestamp(5, new Timestamp(record.getExpirationTimeMs()));
      } else {
        statement.setTimestamp(5, null);
      }
      final long newVersion =
          record.getVersion() == NamedBlobRecord.UNINITIALIZED_VERSION ? buildVersion(this.time) : record.getVersion();
      statement.setLong(6, newVersion);
      statement.setInt(7, state.ordinal());
      statement.setLong(8, record.getBlobSize());
      updatedRecord = new NamedBlobRecord(record.getAccountName(), record.getContainerName(), record.getBlobName(),
          record.getBlobId(), record.getExpirationTimeMs(), newVersion);
      query = statement.toString();
      logger.debug("Putting blob name in MySql. Query {}", query);
      metricsRecoder.namedBlobInsertRate.mark();
      statement.executeUpdate();
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
    return new PutResult(updatedRecord);
  }

  private PutResult apply_ttl_update(NamedBlobRecord record, short accountId, short containerId, Connection connection)
      throws Exception {
    String query = "";
    try (PreparedStatement statement = connection.prepareStatement(TTL_UPDATE_QUERY)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, record.getBlobName());
      statement.setLong(4, record.getVersion());
      query = statement.toString();
      logger.debug("Updating TTL in MySql. Query {}", query);
      metricsRecoder.namedBlobUpdateRate.mark();
      int rowCount = statement.executeUpdate();
      if (rowCount == 0) {
        metricsRecoder.namedTtlupdateErrorCount.inc();
        throw buildException("TTL Update: Blob not found", RestServiceErrorCode.NotFound, record.getAccountName(),
            record.getContainerName(), record.getBlobName());
      }
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
    return new PutResult(record);
  }

  private DeleteResult run_delete_v2(String accountName, String containerName, String blobName, short accountId,
      short containerId, Connection connection) throws Exception {
    String blobId;
    long version;
    Timestamp currentDeleteTime = null;
    boolean alreadyDeleted;
    List<DeleteResult.BlobVersion> blobVersions = new ArrayList<>();
    List<Long> versionsToDelete = new ArrayList<>();
    String query = "";
    try (PreparedStatement statement = connection.prepareStatement(SELECT_FOR_DELETE_QUERY)) {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, blobName);
      query = statement.toString();
      logger.debug("Deleting blob name in MySql. Query {}", query);
      metricsRecoder.namedBlobDeleteRate.mark();
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
          version = resultSet.getLong(2);
          Timestamp originalDeletionTime = resultSet.getTimestamp(3);
          currentDeleteTime = resultSet.getTimestamp(4);
          alreadyDeleted = (originalDeletionTime != null && currentDeleteTime.after(originalDeletionTime));
          blobVersions.add(new DeleteResult.BlobVersion(blobId, version, alreadyDeleted));
          if (!alreadyDeleted) {
            versionsToDelete.add(version);
          }
        }
        if (blobVersions.isEmpty()) {
          throw buildException("DELETE: Blob not found", RestServiceErrorCode.NotFound, accountName, containerName,
              blobName);
        }
      }
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
    // only need to issue an update statement if the row was not already marked as deleted.
    if (!versionsToDelete.isEmpty()) {
      applyDelete(accountId, containerId, blobName, currentDeleteTime, versionsToDelete, connection);
    }
    return new DeleteResult(blobVersions);
  }

  /**
   * Identifies stale blobs from a list of blobs belonging to an active container,
   * based on their state and last modified timestamp relative to a cutoff period.
   *
   * The method iterates through the sorted list of blobs, comparing adjacent blobs
   * with the same name and marking blobs as stale according to these rules:
   * - If a blob is IN_PROGRESS and an equivalent READY blob exists,
   *   the IN_PROGRESS blob is stale if last modified before the cutoff.
   * - If two READY blobs exist with the same name, the later one is stale.
   * - If two IN_PROGRESS blobs exist, the older one is stale if last modified before cutoff.
   * - If a READY blob is followed by an IN_PROGRESS blob, the IN_PROGRESS blob is stale.
   *
   * At the end, if the last considered blob is IN_PROGRESS and older than the cutoff, it is also marked stale.
   *
   * @param blobList      The list of StaleNamedBlob objects to process. Should be sorted by blob name and timestamp.
   * @param cutoffDays    The number of days used to calculate the cutoff timestamp for staleness.
   * @return A StaleBlobsWithLatestBlobName object containing the list of stale blobs
   *                      and the name of the latest blob considered.
   */
  public StaleBlobsWithLatestBlobName getStaleBlobsForActiveContainer(List<StaleNamedBlob> blobList, int cutoffDays) {
    List<StaleNamedBlob> staleBlobs = new ArrayList<>();
    if (blobList.isEmpty()) {
      return new StaleBlobsWithLatestBlobName(staleBlobs, null);
    }

    long cutoffTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(cutoffDays);
    Timestamp cutoffTimestamp = new Timestamp(cutoffTime);

    StaleNamedBlob keepBlob = blobList.get(0);
    for (int i = 1; i < blobList.size(); i++) {
      StaleNamedBlob currentBlob = blobList.get(i);

      if (!keepBlob.getBlobName().equals(currentBlob.getBlobName())) {
        if (keepBlob.getBlobState() == NamedBlobState.IN_PROGRESS && keepBlob.getModifiedTS().before(cutoffTimestamp)) {
          staleBlobs.add(keepBlob);
        }
        keepBlob = currentBlob;
        continue;
      }

      NamedBlobState keepBlobState = keepBlob.getBlobState();
      NamedBlobState currentBlobState = currentBlob.getBlobState();
      Timestamp keepBlobModifiedTS = keepBlob.getModifiedTS();
      Timestamp currentBlobModifiedTS = currentBlob.getModifiedTS();

      if (keepBlobState == NamedBlobState.IN_PROGRESS && currentBlobState == NamedBlobState.READY) {
        if (keepBlobModifiedTS.before(cutoffTimestamp)) {
          staleBlobs.add(keepBlob);
        }
        keepBlob = currentBlob;
      } else if (keepBlobState == NamedBlobState.READY && currentBlobState == NamedBlobState.READY) {
        staleBlobs.add(currentBlob);
      } else if (keepBlobState == NamedBlobState.IN_PROGRESS && currentBlobState == NamedBlobState.IN_PROGRESS) {
        if (keepBlobModifiedTS.after(cutoffTimestamp) && currentBlobModifiedTS.before(cutoffTimestamp)) {
          staleBlobs.add(currentBlob);
        } else if (keepBlobModifiedTS.before(cutoffTimestamp) && currentBlobModifiedTS.before(cutoffTimestamp)) {
          staleBlobs.add(keepBlob);
          keepBlob = currentBlob;
        }
      } else if (keepBlobState == NamedBlobState.READY && currentBlobState == NamedBlobState.IN_PROGRESS) {
        staleBlobs.add(currentBlob);
      }
    }

    if (keepBlob.getBlobState() == NamedBlobState.IN_PROGRESS && keepBlob.getModifiedTS().before(cutoffTimestamp)) {
      staleBlobs.add(keepBlob);
    }

    if (!staleBlobs.isEmpty()) {
      logger.info("These are the stale blobs that will be marked for deletion: {} ", staleBlobs);
    }

    StaleBlobsWithLatestBlobName staleBlobsWithLatestBlobName =
        new StaleBlobsWithLatestBlobName(staleBlobs, keepBlob.getBlobName());
    return staleBlobsWithLatestBlobName;
  }

  /**
   * Retrieves a batch of blobs for the given container from the database.
   *
   * This method fetches a paginated list of blobs that may be considered for staleness
   * evaluation based on the container's status (ACTIVE or INACTIVE). The appropriate SQL
   * query is chosen depending on the container's state. Each row in the result set is
   * mapped to a StaleNamedBlob object.
   *
   * Parameters used in the query:
   * - Container ID
   * - Parent Account ID
   * - Limit: Maximum number of rows to retrieve (config.queryStaleDataMaxResults)
   * - Offset: Used for pagination, calculated as idx * batch size
   *
   * @param connection The active JDBC connection to the database.
   * @param container  The container for which blobs are to be fetched.
   * @return A list of StaleNamedBlob objects representing the result set.
   * @throws SQLException If a database access error occurs or the query fails.
   */
  private List<StaleNamedBlob> getAllBlobsForContainer(Connection connection, Container container,
      String latestBlobName) throws SQLException {
    List<StaleNamedBlob> resultList = new ArrayList<>();

    try (PreparedStatement statement = connection.prepareStatement(GET_BLOBS_FOR_CONTAINER)) {
      statement.setInt(1, container.getId());
      statement.setInt(2, container.getParentAccountId());
      statement.setString(3, latestBlobName);
      statement.setInt(4, config.queryStaleDataMaxResults);

      logger.info("Pulling potential stale blobs from MySql. Query {}", statement.toString());

      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          short accountId = resultSet.getShort(1);
          short containerId = resultSet.getShort(2);
          String blobName = resultSet.getString(3);
          String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(4));
          long version = resultSet.getLong(5);
          NamedBlobState blobState = NamedBlobState.values()[resultSet.getInt(6)];
          Timestamp modifiedTime = resultSet.getTimestamp(7);
          Timestamp deletedTime = resultSet.getTimestamp(8);

          StaleNamedBlob result =
              new StaleNamedBlob(accountId, containerId, blobName, blobId, version, deletedTime, blobState,
                  modifiedTime);
          resultList.add(result);
        }
      }
    } catch (SQLException e) {
      logger.error("Error executing query: {}", e.getMessage());
      throw e;
    }

    return resultList;
  }

  private boolean checkIfValidContainerForCleaning(Connection connection, Container container) throws SQLException {
    int minStaleCount = 0;
    try (PreparedStatement statement = connection.prepareStatement(GET_MINIMUM_STALE_BLOB_COUNT)) {
      statement.setInt(1, container.getId());
      statement.setInt(2, container.getParentAccountId());
      statement.setInt(3, NamedBlobState.READY.ordinal());

      logger.info("Determining the minimum number of stale blobs: Query {}", statement.toString());

      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          minStaleCount = resultSet.getInt(1);  // directly read the number returned by the query
        }
      }
    } catch (SQLException e) {
      logger.error("Error executing query: {}", e.getMessage());
      throw e;
    }

    return minStaleCount > 0;
  }

  /**
   * Performs a batch soft delete on the provided list of stale blobs.
   *
   * Each blob is marked as deleted by setting its delete timestamp in the database.
   * The delete operation uses a parameterized SQL query that identifies blobs
   * by account ID, container ID, blob name, and version.
   *
   * This method uses JDBC batch processing for efficiency.
   *
   * @param staleNamedBlobs A list of stale blobs to be soft deleted.
   * @param connection       The database connection used to execute the update.
   * @throws Exception If any error occurs during the batch execution.
   */
  private void batchSoftDelete(List<StaleNamedBlob> staleNamedBlobs, Connection connection) throws Exception {
    String query = SOFT_DELETE_WITH_VERSION_QUERY;

    try (PreparedStatement statement = connection.prepareStatement(query)) {
      for (StaleNamedBlob blob : staleNamedBlobs) {
        statement.setTimestamp(1,
            new Timestamp(System.currentTimeMillis()));  // Or use new Timestamp(System.currentTimeMillis()) if needed
        statement.setInt(2, blob.getAccountId());
        statement.setInt(3, blob.getContainerId());
        statement.setString(4, blob.getBlobName());
        statement.setLong(5, blob.getVersion());
        statement.addBatch();
      }
      int[] results = statement.executeBatch();
      logger.debug("Batch soft delete completed. Number of blobs deleted: {}", results.length);
    } catch (SQLException e) {
      logger.error("Failed to execute batch soft delete. Error: {}", e.getMessage());
      throw e;
    }
  }

  /**
   * Render a sql template with the number of versions to delete. The template should contain MULTI_VERSION_PLACE_HOLDER.
   * The number of versions provided in the argument would determine how many "?" to put int the sql command.
   * @param template The template string.
   * @param numberOfVersions
   * @return
   */
  private String renderMultiVersionTemplate(String template, int numberOfVersions) {
    if (numberOfVersions > MAX_NUMBER_OF_VERSIONS_IN_DELETE) {
      throw new IllegalArgumentException("Too many versions requested: " + numberOfVersions);
    }
    String placeholder = IntStream.range(0, numberOfVersions).mapToObj(i -> "? ").collect(Collectors.joining(", "));
    return template.replace(MULTI_VERSION_PLACE_HOLDER, placeholder);
  }

  /**
   * Delete multiple versions of a a blob name.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param blobName The blob name to delete
   * @param deletedTs The deleted ts timstamp.
   * @param versions The list of versions to delete
   * @param connection The mysql connection.
   * @throws Exception
   */
  private void applyDelete(short accountId, short containerId, String blobName, Timestamp deletedTs,
      List<Long> versions, Connection connection) throws Exception {
    String query = "";
    String deleteTemplate = config.enableHardDelete ? HARD_DELETE_MULTIPLE_VERSIONS_QUERY_TEMPLATE
        : SOFT_DELETE_MULTIPLE_VERSIONS_QUERY_TEMPLATE;
    String deleteStatement = renderMultiVersionTemplate(deleteTemplate, versions.size());
    try (PreparedStatement statement = connection.prepareStatement(deleteStatement)) {
      // use the current time
      int paramInd = 1;
      if (!config.enableHardDelete) {
        // Update statement requires a delete timestamp as first parameter
        statement.setTimestamp(paramInd++, deletedTs);
      }
      statement.setInt(paramInd++, accountId);
      statement.setInt(paramInd++, containerId);
      statement.setString(paramInd++, blobName);
      for (long version : versions) {
        statement.setLong(paramInd++, version);
      }
      query = statement.toString();
      logger.debug("Deleting blob in MySql. Query {}", query);
      statement.executeUpdate();
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
  }

  /**
   * Build the version for Named Blob row based on timestamp and uuid postfix.
   * @return a long number whose rightmost 5 digits are uuid postfix, and the remaining digits are current timestamp
   */
  public static long buildVersion(Time time) {
    long currentTime = time.milliseconds();
    UUID uuid = UUID.randomUUID();
    return currentTime * VERSION_BASE + Long.parseLong(uuid.toString().split("-")[0], 16) % VERSION_BASE;
  }

  /**
   * Compare a nullable timestamp against {@code otherTimeMs}.
   * @param timestamp the nullable {@link Timestamp} to compare.
   * @param otherTimeMs the time value to compare against.
   * @return -1 if the timestamp is earlier than {@code otherTimeMs}, 0 if the times are equal, and 1 if
   *         the timestamp is later than {@code otherTimeMs}. {@code null} is considered greater than any other time.
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

  private interface TransactionGeneric<T> {
    T run(Connection connection) throws Exception;
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
