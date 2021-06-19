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
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private static final String NAMED_BLOBS = "named_blobs";
  // column names
  private static final String ACCOUNT_ID = "account_id";
  private static final String CONTAINER_ID = "container_id";
  private static final String BLOB_NAME = "blob_name";
  private static final String BLOB_ID = "blob_id";
  private static final String DELETED_TS = "deleted_ts";
  private static final String EXPIRES_TS = "expires_ts";
  // query building blocks
  private static final String CURRENT_TIME = "CURRENT_TIMESTAMP(6)";
  private static final String CURRENT_TIME_COMPARISON = "(%1$s IS NOT NULL AND %1$s <= " + CURRENT_TIME + ")";
  private static final String IS_DELETED = String.format(CURRENT_TIME_COMPARISON, DELETED_TS);
  private static final String IS_EXPIRED = String.format(CURRENT_TIME_COMPARISON, EXPIRES_TS);
  private static final String IS_DELETED_OR_EXPIRED = "(" + IS_DELETED + " OR " + IS_EXPIRED + ")";
  private static final String PK_MATCH = String.format("(%s, %s, %s) = (?, ?, ?)", ACCOUNT_ID, CONTAINER_ID, BLOB_NAME);

  /**
   * Select a record that matches a blob name (lookup by primary key).
   */
  private static final String GET_QUERY =
      String.format("SELECT %s, %s, %s FROM %s WHERE %s", BLOB_ID, EXPIRES_TS, DELETED_TS, NAMED_BLOBS, PK_MATCH);

  /**
   * Select records up to a specific limit where the blob name starts with a string prefix. The fourth parameter can
   * be used for pagination.
   */
  private static final String LIST_QUERY = String.format("SELECT %1$s, %2$s, %3$s, %4$s FROM %5$s "
          + "WHERE (%6$s, %7$s) = (?, ?) AND %1$s LIKE ? AND %1$s >= ? ORDER BY %1$s ASC LIMIT ?", BLOB_NAME, BLOB_ID,
      EXPIRES_TS, DELETED_TS, NAMED_BLOBS, ACCOUNT_ID, CONTAINER_ID);

  /**
   * Attempt to insert a new mapping into the database.
   */
  private static final String INSERT_QUERY =
      String.format("INSERT INTO %s (%s, %s, %4$s, %5$s, %6$s) VALUES (?, ?, ?, ?, ?)", NAMED_BLOBS, ACCOUNT_ID,
          CONTAINER_ID, BLOB_NAME, BLOB_ID, EXPIRES_TS);

  /**
   * If a record already exists for a named blob, attempt an update if the record in the DB represents an expired or
   * deleted blob.
   */
  private static final String UPDATE_IF_DELETED_OR_EXPIRED_QUERY =
      String.format("UPDATE %s SET %s = ?, %s = ?, %s = null WHERE %s AND %s", NAMED_BLOBS, BLOB_ID, EXPIRES_TS,
          DELETED_TS, PK_MATCH, IS_DELETED_OR_EXPIRED);

  /**
   * Find if there is currently a record present for a blob and acquire an exclusive lock in preparation for a delete.
   * This select call also allows the current blob ID to be retrieved prior to a delete.
   */
  private static final String SELECT_FOR_SOFT_DELETE_QUERY =
      String.format("SELECT %s, %s, %s FROM %s WHERE %s FOR UPDATE", BLOB_ID, DELETED_TS, CURRENT_TIME, NAMED_BLOBS,
          PK_MATCH);

  /**
   * Soft delete a blob by setting the delete timestamp to the current time.
   */
  private static final String SOFT_DELETE_QUERY =
      String.format("UPDATE %s SET %s = ? WHERE %s", NAMED_BLOBS, DELETED_TS, PK_MATCH);

  private final AccountService accountService;
  private final String localDatacenter;
  private final List<String> remoteDatacenters;
  private final RetryExecutor retryExecutor;
  private final Map<String, TransactionExecutor> transactionExecutors;
  private final MySqlNamedBlobDbConfig config;

  MySqlNamedBlobDb(AccountService accountService, MySqlNamedBlobDbConfig config, DataSourceFactory dataSourceFactory,
      String localDatacenter) {
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
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName) {
    TransactionStateTracker transactionStateTracker =
        new GetTransactionStateTracker(remoteDatacenters, localDatacenter);
    return executeTransactionAsync(accountName, containerName, true, (accountId, containerId, connection) -> {
      try (PreparedStatement statement = connection.prepareStatement(GET_QUERY)) {
        statement.setInt(1, accountId);
        statement.setInt(2, containerId);
        statement.setString(3, blobName);
        try (ResultSet resultSet = statement.executeQuery()) {
          if (!resultSet.next()) {
            throw buildException("GET: Blob not found", RestServiceErrorCode.NotFound, accountName, containerName,
                blobName);
          }
          String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
          Timestamp expirationTime = resultSet.getTimestamp(2);
          Timestamp deletionTime = resultSet.getTimestamp(3);
          long currentTime = System.currentTimeMillis();
          if (compareTimestamp(expirationTime, currentTime) <= 0) {
            throw buildException("GET: Blob expired", RestServiceErrorCode.Deleted, accountName, containerName,
                blobName);
          } else if (compareTimestamp(deletionTime, currentTime) <= 0) {
            throw buildException("GET: Blob deleted", RestServiceErrorCode.Deleted, accountName, containerName,
                blobName);
          } else {
            return new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(expirationTime));
          }
        }
      }
    }, transactionStateTracker);
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken) {
    return executeTransactionAsync(accountName, containerName, true, (accountId, containerId, connection) -> {
      try (PreparedStatement statement = connection.prepareStatement(LIST_QUERY)) {
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
            Timestamp expirationTime = resultSet.getTimestamp(3);
            Timestamp deletionTime = resultSet.getTimestamp(4);

            if (compareTimestamp(expirationTime, currentTime) <= 0) {
              logger.trace("LIST: Blob expired, ignoring in list response; account='{}', container='{}', name='{}'",
                  accountName, containerName, blobName);
            } else if (compareTimestamp(deletionTime, currentTime) <= 0) {
              logger.trace("LIST: Blob deleted, ignoring in list response; account='{}', container='{}', name='{}'",
                  accountName, containerName, blobName);
            } else {
              entries.add(
                  new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(expirationTime)));
            }
          }
          return new Page<>(entries, nextContinuationToken);
        }
      }
    }, null);
  }

  @Override
  public CompletableFuture<PutResult> put(NamedBlobRecord record) {
    return executeTransactionAsync(record.getAccountName(), record.getContainerName(), true,
        (accountId, containerId, connection) -> {
          boolean rowAlreadyExists = false;
          // 1. Attempt to insert into the table. This is attempted first since it is the most common case.
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
            statement.executeUpdate();
          } catch (SQLIntegrityConstraintViolationException e) {
            rowAlreadyExists = true;
          }
          // 2. If the row already exists, attempt an update, checking that the update should be allowed (the current
          //    row in the db represents a blob that was soft deleted or expired. Since soft deleted records are not
          //    deleted from the table before a long retention period, there is no risk of the row not existing when
          //    this query is run.
          if (rowAlreadyExists) {
            try (PreparedStatement statement = connection.prepareStatement(UPDATE_IF_DELETED_OR_EXPIRED_QUERY)) {
              // fields to update
              statement.setBytes(1, Base64.decodeBase64(record.getBlobId()));
              if (record.getExpirationTimeMs() != Utils.Infinite_Time) {
                statement.setTimestamp(2, new Timestamp(record.getExpirationTimeMs()));
              } else {
                statement.setTimestamp(2, null);
              }
              // primary key fields
              statement.setInt(3, accountId);
              statement.setInt(4, containerId);
              statement.setString(5, record.getBlobName());
              // the number of rows found will be 0 if there is currently an entry for this blob name that is
              // not expired or deleted.
              if (statement.executeUpdate() == 0) {
                throw buildException("PUT: Blob still alive", RestServiceErrorCode.Conflict, record.getAccountName(),
                    record.getContainerName(), record.getBlobName());
              }
            }
          }
          return new PutResult(record);
        }, null);
  }

  @Override
  public CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName) {
    return executeTransactionAsync(accountName, containerName, false, (accountId, containerId, connection) -> {
      String blobId;
      Timestamp currentTime;
      boolean alreadyDeleted;
      try (PreparedStatement statement = connection.prepareStatement(SELECT_FOR_SOFT_DELETE_QUERY)) {
        statement.setInt(1, accountId);
        statement.setInt(2, containerId);
        statement.setString(3, blobName);
        try (ResultSet resultSet = statement.executeQuery()) {
          if (!resultSet.next()) {
            throw buildException("DELETE: Blob not found", RestServiceErrorCode.NotFound, accountName, containerName,
                blobName);
          }
          blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
          Timestamp deletionTime = resultSet.getTimestamp(2);
          currentTime = resultSet.getTimestamp(3);
          alreadyDeleted = (deletionTime != null && currentTime.after(deletionTime));
        }
      }
      // only need to issue an update statement if the row was not already marked as deleted.
      if (!alreadyDeleted) {
        try (PreparedStatement statement = connection.prepareStatement(SOFT_DELETE_QUERY)) {
          // use the current time
          statement.setTimestamp(1, currentTime);
          statement.setInt(2, accountId);
          statement.setInt(3, containerId);
          statement.setString(4, blobName);
          statement.executeUpdate();
        }
      }
      return new DeleteResult(blobId, alreadyDeleted);
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
