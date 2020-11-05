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
package com.github.ambry.account.mysql;

import com.github.ambry.account.AccountServiceErrorCode;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.utils.Pair;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientConnectionException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.mysql.MySqlUtils.*;


/**
 * Data Accessor to connect to MySql database.
 */
public class MySqlDataAccessor {

  private static final Logger logger = LoggerFactory.getLogger(MySqlDataAccessor.class);
  private static final String INDEX_ACCOUNT_CONTAINER = "containers.accountContainer";
  private static final String INDEX_CONTAINER_NAME = "containers.uniqueName";
  /** List of {@link DbEndpoint} sorted from best to worst */
  private final Map<String, PreparedStatement> statementCache = new HashMap<>();
  private final MySqlAccountStoreMetrics metrics;
  private Driver mysqlDriver;
  private Connection activeConnection;
  private DbEndpoint connectedEndpoint;
  private List<DbEndpoint> sortedDbEndpoints;
  private EndpointComparator endpointComparator;

  /**
   * List of operation types on the mysql store.
   */
  public enum OperationType {
    Write, Read
  }

  /** Production constructor */
  public MySqlDataAccessor(List<DbEndpoint> inputEndpoints, String localDatacenter, MySqlAccountStoreMetrics metrics)
      throws SQLException {
    this.metrics = metrics;
    setup(inputEndpoints, localDatacenter);
  }

  /** Test constructor */
  public MySqlDataAccessor(List<DbEndpoint> inputEndpoints, Driver mysqlDriver, MySqlAccountStoreMetrics metrics)
      throws SQLException {
    this.mysqlDriver = mysqlDriver;
    this.metrics = metrics;
    setup(inputEndpoints, inputEndpoints.get(0).getDatacenter());
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
    if (!sortedDbEndpoints.get(0).isWriteable()) {
      throw new IllegalArgumentException("No endpoints are writable");
    }

    initializeDriver(sortedDbEndpoints.get(0).getUrl());

    // AccountService needs to work if mysql is down.  Mysql can also reboot.
    try {
      getDatabaseConnection(true);
    } catch (SQLException e) {
      if (isCredentialError(e)) {
        throw e;
      } else {
        logger.error("No writable database available, will retry later.");
      }
    }
  }

  private void initializeDriver(String url) throws SQLException {
    if (mysqlDriver == null) {
      mysqlDriver = DriverManager.getDriver(url);
    }
  }

  /**
   * @return a JDBC {@link Connection} to the database.  An existing connection will be reused,
   * unless a connection to a better-ranked enpoint is available.
   * @param needWritable whether the database instance needs to be writeable.
   * @throws SQLException
   */
  public synchronized Connection getDatabaseConnection(boolean needWritable) throws SQLException {

    // Close active connection if no longer valid
    if (activeConnection != null && !activeConnection.isValid(5)) {
      reset();
      activeConnection = null;
      connectedEndpoint = null;
    }

    // If the active connection is good and it's the best endpoint, keep it.
    if (activeConnection != null && !isBetterEndpoint(sortedDbEndpoints.get(0), connectedEndpoint)) {
      return activeConnection;
    }
    // See if we can do better
    Pair<DbEndpoint, Connection> endpointConnectionPair = connectToBestAvailableEndpoint(needWritable);
    if (connectedEndpoint == endpointConnectionPair.getFirst()) {
      // No better endpoint  was available.
      logger.debug("Still connected to {}", connectedEndpoint.getUrl());
    } else {
      // New connection established!
      // TODO: mysqlMetrics.connectionFixedCount.inc();
      connectedEndpoint = endpointConnectionPair.getFirst();
      String qualifier = connectedEndpoint.isWriteable() ? "writable" : "read-only";
      logger.info("Connected to {} enpoint: {}", qualifier, connectedEndpoint.getUrl());
      closeQuietly(activeConnection); // TODO: reset?
      activeConnection = endpointConnectionPair.getSecond();
    }
    return activeConnection;
  }

  /**
   * @return a {@link PreparedStatement} using the supplied SQL text.
   * @param sql the SQL text to use.
   * @throws SQLException
   */
  public synchronized PreparedStatement getPreparedStatement(String sql, boolean needWritable) throws SQLException {
    // TODO: if connected to suboptimal endpoint, try to upgrade
    PreparedStatement statement = statementCache.get(sql);
    if (statement != null) {
      return statement;
    }
    Connection connection = getDatabaseConnection(needWritable);
    statement = connection.prepareStatement(sql);
    statementCache.put(sql, statement);
    return statement;
  }

  /**
   * Translate a {@link SQLException} to a {@link AccountServiceException}.
   * @param e the input exception.
   * @return the corresponding {@link AccountServiceException}.
   */
  public static AccountServiceException translateSQLException(SQLException e) {
    if (e instanceof SQLIntegrityConstraintViolationException) {
      SQLIntegrityConstraintViolationException icve = (SQLIntegrityConstraintViolationException) e;
      String message;
      if (icve.getMessage().contains(INDEX_ACCOUNT_CONTAINER)) {
        // Example: Duplicate entry '101-5' for key 'containers.accountContainer'
        message = "Duplicate containerId";
      } else if (icve.getMessage().contains(INDEX_CONTAINER_NAME)) {
        // duplicate container name: need to update cache but retry may fail
        message = "Duplicate container name";
      } else {
        message = "Constraint violation";
      }
      return new AccountServiceException(message, AccountServiceErrorCode.ResourceConflict);
    } else if (MySqlDataAccessor.isCredentialError(e)) {
      return new AccountServiceException("Invalid database credentials", AccountServiceErrorCode.InternalError);
    } else {
      return new AccountServiceException(e.getMessage(), AccountServiceErrorCode.InternalError);
    }
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
  void onException(SQLException e, OperationType operationType) {
    if (e instanceof SQLTransientConnectionException) {
      if (operationType == OperationType.Write) {
        metrics.writeFailureCount.inc();
      } else {
        metrics.readFailureCount.inc();
      }
      reset();
    }
  }

  /**
   * Handle successful database operation
   * @param operationType type of mysql operation
   * @param operationTimeInMs operation time in milliseconds
   */
  void onSuccess(OperationType operationType, long operationTimeInMs) {
    if (operationType == OperationType.Write) {
      metrics.writeSuccessCount.inc();
      metrics.writeTimeInMs.update(operationTimeInMs);
    } else {
      metrics.readSuccessCount.inc();
      metrics.readTimeInMs.update(operationTimeInMs);
    }
  }

  /**
   * Close the active connection and clear the statement cache.
   * This should be called after a failed database operation.
   */
  synchronized void reset() {
    for (PreparedStatement statement : statementCache.values()) {
      closeQuietly(statement);
    }
    statementCache.clear();
    closeQuietly(activeConnection);
  }

  /**
   * Connect to the best available database instance that matches the specified criteria.
   * Order of preference for instances to connect to:
   *   1) writeable instance in local colo
   *   2) writable instance in any colo
   *   3) readonly instance in local colo (if needWritable is false)
   *   4) readonly instance in any colo (if needWritable is false)
   * @param needWritable whether the endpoint needs to be writable
   * @return a pair of {@link DbEndpoint} and corresponding {@link Connection}.
   * @throws SQLException if connection could not be made to a suitable endpoint.
   */
  private Pair<DbEndpoint, Connection> connectToBestAvailableEndpoint(boolean needWritable) throws SQLException {
    SQLException lastException = null;
    for (DbEndpoint dbEndpoint : sortedDbEndpoints) {
      if (!isBetterEndpoint(dbEndpoint, connectedEndpoint)) {
        // This is the best we can do
        if (needWritable && !connectedEndpoint.isWriteable()) {
          // TODO: throw lastException if any
          throw new SQLException("Could not connect to any writable database");
        } else {
          return new Pair<>(connectedEndpoint, activeConnection);
        }
      }
      if (needWritable && !dbEndpoint.isWriteable()) {
        continue;
      }

      // Try to connect to candidate endpoint
      Properties credentials = new Properties();
      credentials.setProperty("user", dbEndpoint.getUsername());
      credentials.setProperty("password", dbEndpoint.getPassword());
      try {
        Connection connection = mysqlDriver.connect(dbEndpoint.getUrl(), credentials);
        return new Pair<>(dbEndpoint, connection);
      } catch (SQLException e) {
        logger.warn("Unable to connect to endpoint {} due to {}", dbEndpoint.getUrl(), e.getMessage());
        // TODO: mysqlMetrics.connectionErrorCount.inc();
        if (isCredentialError(e)) {
          // fail fast
          throw e;
        } else {
          lastException = e;
        }
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      // Should never get here.
      throw new IllegalStateException("No suitable connection found");
    }
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
   * Close a resource without throwing exception.
   * @param resource the resource to close.
   */
  private static void closeQuietly(AutoCloseable resource) {
    try {
      if (resource != null) {
        resource.close();
      }
    } catch (Exception e) {
      logger.warn("Closing resource", e);
    }
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
