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
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.mysql.MySqlUtils.DbEndpoint;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private static final String CURRENT_TIME_COMPARISON = "(%1$s IS NOT NULL AND %1$s <= CURRENT_TIMESTAMP(6))";
  private static final String IS_DELETED = String.format(CURRENT_TIME_COMPARISON, DELETED_TS);
  private static final String IS_EXPIRED = String.format(CURRENT_TIME_COMPARISON, EXPIRES_TS);
  private static final String IS_DELETED_OR_EXPIRED = IS_DELETED + " OR " + IS_EXPIRED;
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
   * Create a blob name to blob ID mapping if a record for that blob name does not exist.
   * If there is an existing record but the existing blob was soft deleted or expired, allow it to be overwritten.
   * If there is an existing record and the existing blob is still valid (not expired or soft deleted), do not
   * overwrite it.
   * <p>
   * Eventually the put operation will support updates to the mapping in the third case above, but that requires other
   * work around concurrency control.
   */
  private static final String PUT_QUERY = String.format(
      "INSERT INTO %1$s (%2$s, %3$s, %4$s, %5$s, %6$s) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE "
          + "%5$s = IF(%8$s, VALUES(%5$s), %5$s), %6$s = IF(%8$s, VALUES(%6$s), %6$s), %7$s = IF(%8$s, null, %7$s)",
      NAMED_BLOBS, ACCOUNT_ID, CONTAINER_ID, BLOB_NAME, BLOB_ID, EXPIRES_TS, DELETED_TS, IS_DELETED_OR_EXPIRED);

  /**
   * Soft delete a blob by setting the delete timestamp to the current time if the blob is not already deleted.
   */
  private static final String DELETE_QUERY =
      String.format("UPDATE %1$s SET %2$s = IF(%3$s, %2$s, CURRENT_TIMESTAMP(6)) WHERE %4$s", NAMED_BLOBS, DELETED_TS,
          IS_DELETED, PK_MATCH);

  private final AccountService accountService;
  private final Map<String, DataSource> dcToDataSource;
  private final String localDatacenter;
  private final ExecutorService executorService;
  private final MySqlNamedBlobDbConfig config;

  MySqlNamedBlobDb(AccountService accountService, MySqlNamedBlobDbConfig config, DataSourceFactory dataSourceFactory,
      String localDatacenter) {
    this.accountService = accountService;
    this.config = config;
    this.localDatacenter = localDatacenter;
    this.dcToDataSource = MySqlUtils.getDbEndpointsPerDC(config.dbInfo)
        .values()
        .stream()
        .flatMap(List::stream)
        .filter(DbEndpoint::isWriteable)
        .collect(Collectors.toMap(DbEndpoint::getDatacenter, dataSourceFactory::getDataSource));
    // size this to match the connection pool
    executorService = Executors.newFixedThreadPool(config.poolSize);
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName) {
    return executeTransactionAsync(accountName, containerName, (accountId, containerId, connection) -> {
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
    });
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String continuationToken) {
    return executeTransactionAsync(accountName, containerName, (accountId, containerId, connection) -> {
      try (PreparedStatement statement = connection.prepareStatement(LIST_QUERY)) {
        statement.setInt(1, accountId);
        statement.setInt(2, containerId);
        statement.setString(3, blobNamePrefix + "%");
        statement.setString(4, continuationToken != null ? continuationToken : blobNamePrefix);
        statement.setInt(5, config.listMaxResults + 1);
        try (ResultSet resultSet = statement.executeQuery()) {
          String nextContinuationToken = null;
          List<NamedBlobRecord> elements = new ArrayList<>();
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
            long currentTime = System.currentTimeMillis();

            if (compareTimestamp(expirationTime, currentTime) <= 0) {
              logger.trace("LIST: Blob expired, ignoring in list response; account='{}', container='{}', name='{}'",
                  accountName, containerName, blobName);
            } else if (compareTimestamp(deletionTime, currentTime) <= 0) {
              logger.trace("LIST: Blob deleted, ignoring in list response; account='{}', container='{}', name='{}'",
                  accountName, containerName, blobName);
            } else {
              elements.add(
                  new NamedBlobRecord(accountName, containerName, blobName, blobId, timestampToMs(expirationTime)));
            }
          }
          return new Page<>(nextContinuationToken, elements);
        }
      }
    });
  }

  @Override
  public CompletableFuture<PutResult> put(NamedBlobRecord record) {
    return executeTransactionAsync(record.getAccountName(), record.getContainerName(),
        (accountId, containerId, connection) -> {
          try (PreparedStatement statement = connection.prepareStatement(PUT_QUERY)) {
            statement.setInt(1, accountId);
            statement.setInt(2, containerId);
            statement.setString(3, record.getBlobName());
            statement.setBytes(4, Base64.decodeBase64(record.getBlobId()));
            if (record.getExpirationTimeMs() != Utils.Infinite_Time) {
              statement.setTimestamp(5, new Timestamp(record.getExpirationTimeMs()));
            } else {
              statement.setTimestamp(5, null);
            }
            // affectedRows will be 0 if there is currently an entry for this blob name that is
            // not expired or deleted. Note that for this to return the number of changed rows instead of found rows,
            // the useAffectedRows connector property has to be set to true.
            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
              throw buildException("PUT: Blob still alive", RestServiceErrorCode.Conflict, record.getAccountName(),
                  record.getContainerName(), record.getBlobName());
            }
          }
          return new PutResult(record);
        });
  }

  @Override
  public CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName) {
    return executeTransactionAsync(accountName, containerName, (accountId, containerId, connection) -> {
      boolean notFoundOrAlreadyDeleted;
      try (PreparedStatement statement = connection.prepareStatement(DELETE_QUERY)) {
        statement.setInt(1, accountId);
        statement.setInt(2, containerId);
        statement.setString(3, blobName);
        // affectedRows will be 0 if an entry for this blob name is not found or if there is an entry, but it was
        // already marked deleted. Unfortunately, we cannot tell between these 2 cases with the useAffectedRows option,
        // but since delete is an idempotent operation and recreation of named blobs after deletes is allowed, returning
        // success in the not found case is probably okay.
        notFoundOrAlreadyDeleted = statement.executeUpdate() == 0;
        if (notFoundOrAlreadyDeleted) {
          logger.trace("DELETE: Blob does not exist or is already deleted; account='{}', container='{}', name='{}'",
              accountName, containerName, blobName);
        }
      }
      return new DeleteResult(notFoundOrAlreadyDeleted);
    });
  }

  /**
   * Run a transaction on a thread pool and handle common logic surrounding looking up account metadata and error
   * handling. Eventually this will handle retries.
   * @param accountName the account name for the transaction.
   * @param containerName the container name for the transaction.
   * @param transaction the {@link Transaction} to run. This can either be a read only query or include DML.
   * @param <T> the return type of the {@link Transaction}.
   * @return a {@link CompletableFuture} that will eventually contain the result of the transaction or an exception.
   */
  private <T> CompletableFuture<T> executeTransactionAsync(String accountName, String containerName,
      Transaction<T> transaction) {
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

    executorService.submit(() -> {
      // TODO introduce failover handling (retry on remote datacenters, handle SQL exceptions)
      try (Connection connection = dcToDataSource.get(localDatacenter).getConnection()) {
        future.complete(transaction.run(account.getId(), container.getId(), connection));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Exposed for integration test usage.
   * @return a map from datacenter name to {@link DataSource}.
   */
  Map<String, DataSource> getDataSources() {
    return dcToDataSource;
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
