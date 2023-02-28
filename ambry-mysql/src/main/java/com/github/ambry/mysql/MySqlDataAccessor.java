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
 */
package com.github.ambry.mysql;

import com.github.ambry.utils.Pair;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlUtils.*;
import static com.github.ambry.utils.Utils.*;


/**
 * Data Accessor to connect to MySql database.
 */
public class MySqlDataAccessor {

  private static final Logger logger = LoggerFactory.getLogger(MySqlDataAccessor.class);
  /** List of {@link DbEndpoint} sorted from best to worst */
  private final Map<String, PreparedStatement> statementCache = new HashMap<>();
  private final MySqlMetrics metrics;
  private final SQLException noWritableEndpointException = new SQLException("Could not connect to a writable database");
  private final SQLException noEndpointException = new SQLException("Could not connect to any database");
  private Driver mysqlDriver;
  private Connection activeConnection;
  private DbEndpoint connectedEndpoint;
  private List<DbEndpoint> sortedDbEndpoints;
  private EndpointComparator endpointComparator;
  // Time of most recent attempt to connect to optimal endpoint
  private long lastConnectionAttemptTime = 0;
  // TODO: make config property
  private long connectionRetryWaitTime = TimeUnit.SECONDS.toMillis(30);

  /**
   * List of operation types on the mysql store.
   */
  public enum OperationType {
    Write, Read, Copy, BatchUpdate
  }

  /** Production constructor */
  public MySqlDataAccessor(List<DbEndpoint> inputEndpoints, String localDatacenter, MySqlMetrics metrics)
      throws SQLException {
    this.metrics = metrics;
    setup(inputEndpoints, localDatacenter);
  }

  /** Test constructor */
  public MySqlDataAccessor(List<DbEndpoint> inputEndpoints, Driver mysqlDriver, MySqlMetrics metrics)
      throws SQLException {
    this.mysqlDriver = mysqlDriver;
    this.metrics = metrics;
    setup(inputEndpoints, inputEndpoints.get(0).getDatacenter());
  }

  /**
   * @return the {@link MySqlMetrics} being used.
   */
  public MySqlMetrics getMetrics() {
    return metrics;
  }

  /**
   * Setup for data access.
   * @param inputEndpoints the {@link DbEndpoint}s to use.
   * @param localDatacenter the name of the local datacenter.
   * @throws SQLException if setup fails.
   */
  private void setup(List<DbEndpoint> inputEndpoints, String localDatacenter) throws SQLException {
    if (inputEndpoints == null || inputEndpoints.isEmpty()) {
      throw new IllegalArgumentException("No endpoints supplied");
    }
    // Sort from best to worst
    endpointComparator = new EndpointComparator(localDatacenter);
    Collections.sort(inputEndpoints, endpointComparator);
    sortedDbEndpoints = inputEndpoints;
    initializeDriver(sortedDbEndpoints.get(0).getUrl());

    // AccountService needs to work if mysql is down.  Mysql can also reboot.
    try {
      getDatabaseConnection(false);
    } catch (SQLException e) {
      if (isCredentialError(e)) {
        throw e;
      } else {
        logger.error("No writable database available, will retry later.", e);
      }
    }
  }

  private void initializeDriver(String url) throws SQLException {
    if (mysqlDriver == null) {
      mysqlDriver = DriverManager.getDriver(url);
    }
  }

  /**
   * Enables or disables auto commit on current active {@link Connection}.
   * @param enable {@code true} to enable auto commit and {@code false} to disable auto commit.
   * @throws SQLException
   */
  public synchronized void setAutoCommit(boolean enable) throws SQLException {
    if (activeConnection == null) {
      throw new IllegalStateException("No active connection available");
    }
    activeConnection.setAutoCommit(enable);
  }

  /**
   * Commits transaction changes on current active {@link Connection}. This should only be used with auto commit disabled.
   * @throws SQLException
   */
  public synchronized void commit() throws SQLException {
    if (activeConnection == null) {
      throw new IllegalStateException("No active connection available");
    }
    activeConnection.commit();
  }

  /**
   * Rollback transaction changes on current active {@link Connection}. This should only be used with auto commit disabled.
   * @throws SQLException
   */
  public synchronized void rollback() throws SQLException {
    // Note: could be called due to not obtaining connection
    if (activeConnection != null) {
      activeConnection.rollback();
    }
  }

  /**
   * @return a JDBC {@link Connection} to the database.  An existing connection will be reused,
   * unless a connection to a better-ranked enpoint is available.
   * @param needWritable whether the database instance needs to be writeable.
   * @throws SQLException
   */
  public synchronized Connection getDatabaseConnection(boolean needWritable) throws SQLException {

    DbEndpoint bestEndpoint = sortedDbEndpoints.get(0);
    if (needWritable && !bestEndpoint.isWriteable()) {
      // Can never be satisfied
      throw noWritableEndpointException;
    }

    // Close active connection if no longer valid
    if (activeConnection != null && !activeConnection.isValid(5)) {
      closeActiveConnection();
    }

    // If the active connection is good and it's the best endpoint, keep it.
    if (activeConnection != null && !isBetterEndpoint(bestEndpoint, connectedEndpoint)) {
      return activeConnection;
    }
    // See if we can do better
    Pair<DbEndpoint, Connection> endpointConnectionPair = connectToBestAvailableEndpoint(needWritable);
    if (connectedEndpoint == endpointConnectionPair.getFirst()) {
      // No better endpoint was available.
      logger.debug("Still connected to {}", connectedEndpoint.getUrl());
    } else {
      // New connection established
      closeActiveConnection();
      connectedEndpoint = endpointConnectionPair.getFirst();
      activeConnection = endpointConnectionPair.getSecond();
      String qualifier = connectedEndpoint.isWriteable() ? "writable" : "read-only";
      logger.info("Connected to {} enpoint: {}", qualifier, connectedEndpoint.getUrl());
    }
    return activeConnection;
  }

  /**
   * Connect to the best available database instance that matches the specified criteria.<br/>
   * Order of preference for instances to connect to:
   * <OL>
   *    <LI/> Writeable instance in local colo
   *    <LI/> Writable instance in any colo
   *    <LI/> Read-only instance in local colo (if needWritable is false)
   *    <LI/> Read-only instance in any colo (if needWritable is false)
   *  </OL>
   * @param needWritable whether the endpoint needs to be writable
   * @return a pair of {@link DbEndpoint} and corresponding {@link Connection}.
   * @throws SQLException if connection could not be made to a suitable endpoint.
   */
  private Pair<DbEndpoint, Connection> connectToBestAvailableEndpoint(boolean needWritable) throws SQLException {
    SQLException lastException = null;
    for (DbEndpoint candidateEndpoint : sortedDbEndpoints) {
      if (!isBetterEndpoint(candidateEndpoint, connectedEndpoint)) {
        // What we have is the best we can do.  Is it good enough?
        if (needWritable && !connectedEndpoint.isWriteable()) {
          throw Optional.ofNullable(lastException).orElse(noWritableEndpointException);
        } else {
          return new Pair<>(connectedEndpoint, activeConnection);
        }
      }
      if (needWritable && !candidateEndpoint.isWriteable()) {
        throw Optional.ofNullable(lastException).orElse(noWritableEndpointException);
      }

      // Attempt to connect to candidate endpoint
      Properties credentials = new Properties();
      credentials.setProperty("user", candidateEndpoint.getUsername());
      credentials.setProperty("password", candidateEndpoint.getPassword());
      try {
        Connection connection = mysqlDriver.connect(candidateEndpoint.getUrl(), credentials);
        metrics.connectionSuccessCount.inc();
        return new Pair<>(candidateEndpoint, connection);
      } catch (SQLException e) {
        logger.warn("Unable to connect to endpoint {} due to {}", candidateEndpoint.getUrl(), e.getMessage());
        metrics.connectionFailureCount.inc();
        if (isCredentialError(e)) {
          // fail fast
          throw e;
        } else {
          lastException = e;
        }
      }
    }

    throw Optional.ofNullable(lastException).orElse(noEndpointException);
  }

  /**
   * @return a {@link PreparedStatement} using the supplied SQL text.
   * @param sql the SQL text to use.
   * @throws SQLException
   */
  public synchronized PreparedStatement getPreparedStatement(String sql, boolean needWritable) throws SQLException {
    // If connected to suboptimal endpoint, attempt to upgrade every so often
    long now = System.currentTimeMillis();
    if (lastConnectionAttemptTime < now - connectionRetryWaitTime) {
      getDatabaseConnection(needWritable);
      lastConnectionAttemptTime = now;
    }
    PreparedStatement statement = statementCache.get(sql);
    if (statement != null && !statement.isClosed()) {
      return statement;
    }
    Connection connection = getDatabaseConnection(needWritable);
    statement = connection.prepareStatement(sql);
    statementCache.put(sql, statement);
    return statement;
  }

  /**
   * @return true if the exception indicates invalid database credentials.
   * @param e the {@link SQLException}
   */
  public static boolean isCredentialError(SQLException e) {
    return e.getErrorCode() == MysqlErrorNumbers.ER_ACCESS_DENIED_ERROR;
  }

  /**
   * Handle a SQL exception on a database operation.
   * @param e the {@link SQLException} encountered.
   * @param operationType type of mysql operation
   */
  public void onException(Exception e, OperationType operationType) {

    // Record errors.
    if (operationType == OperationType.Write) {
      metrics.writeFailureCount.inc();
    } else if (operationType == OperationType.Read) {
      metrics.readFailureCount.inc();
    } else if (operationType == OperationType.Copy) {
      metrics.copyFailureCount.inc();
    } else if (operationType == OperationType.BatchUpdate) {
      metrics.batchUpdateFailureCount.inc();
    }

    // Close connection for all non transient sql exceptions.
    if (!(e instanceof SQLNonTransientException || (e instanceof BatchUpdateException
        && e.getCause() instanceof SQLNonTransientException))) {
      closeActiveConnection();
    }
  }

  /**
   * Handle successful database operation
   * @param operationType type of mysql operation
   * @param operationTimeInMs operation time in milliseconds
   */
  public void onSuccess(OperationType operationType, long operationTimeInMs) {
    if (operationType == OperationType.Write) {
      metrics.writeSuccessCount.inc();
      metrics.writeTimeMs.update(operationTimeInMs);
    } else if (operationType == OperationType.Read) {
      metrics.readSuccessCount.inc();
      metrics.readTimeMs.update(operationTimeInMs);
    } else if (operationType == OperationType.Copy) {
      metrics.copySuccessCount.inc();
      metrics.copyTimeMs.update(operationTimeInMs);
    } else if (operationType == OperationType.BatchUpdate) {
      metrics.batchUpdateSuccessCount.inc();
      metrics.batchUpdateTimeMs.update(operationTimeInMs);
    }
  }

  /**
   * Close the active connection and clear the statement cache.
   */
  public synchronized void closeActiveConnection() {
    for (PreparedStatement statement : statementCache.values()) {
      closeQuietly(statement);
    }
    statementCache.clear();
    closeQuietly(activeConnection);
    activeConnection = null;
    connectedEndpoint = null;
  }

  private boolean isBetterEndpoint(DbEndpoint first, DbEndpoint second) {
    if (first == null) {
      return false;
    }
    if (second == null) {
      return true;
    }
    return endpointComparator.compare(first, second) < 0;
  }

  /**
   * Comparator for instances of {@link DbEndpoint} that orders first by writeable instances, then by ones in local
   * datacenter.
   */
  private class EndpointComparator implements Comparator<DbEndpoint> {
    private final String localDatacenter;

    private EndpointComparator(String localDatacenter) {
      this.localDatacenter = localDatacenter;
    }

    @Override
    public int compare(DbEndpoint e1, DbEndpoint e2) {
      if (e1.isWriteable() != e2.isWriteable()) {
        return e1.isWriteable() ? -1 : 1;
      }
      // Both writeable or not, so decide on datacenter
      if (!e1.getDatacenter().equals(e2.getDatacenter())) {
        return e1.getDatacenter().equals(localDatacenter) ? -1 : e2.getDatacenter().equals(localDatacenter) ? 1 : 0;
      }
      return 0;
    }
  }
}
