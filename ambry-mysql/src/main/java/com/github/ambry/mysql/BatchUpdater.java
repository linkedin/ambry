/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.GenericThrowableConsumer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;


/**
 * This class buffers a list of sql update commands (insert, update, delete) and sends them to database
 * in batch. The BatchUpdater would greatly increase the performance due to less overhead in network.
 *
 * The implementation of batch with auto commit is vendor-dependent and may not fully take advantage of
 * batch operation since each sql command might still be committed separately. Thus this class always disable
 * auto commit in sql connection.
 *
 * When constructing an instance of this class, caller has to provide a {@code maxBatchSize}. The value can't
 * be negative. If it's 0, there is no limitation on batch size. All sql commands would be buffered and then
 * sent to database in one batch. If it's positive, then when the number of buffered sql commands reaches the
 * maxBatchSize, this batch would be sent to database.
 *
 * Sample usage:
 * <pre>
 *   BatchUpdater batch = new BatchUpdater(dataAccessor, "insert into myTable values(?)", "myTable", 100);
 *   for (String name: names) {
 *       batch.addUpdateToBatch(statement -> {
 *           statement.setString(1, name);
 *       });
 *   }
 *   batch.flush();
 * </pre>
 * After batch is flushed, you shouldn't use the same object again. To perform another batch operation, please
 * create another {@link BatchUpdater} object.
 */
public class BatchUpdater {
  private static final Logger logger = LoggerFactory.getLogger(BatchUpdater.class);
  private final boolean autoCommit;
  private final PreparedStatement statement;
  private final int maxBatchSize;
  private final MySqlDataAccessor dataAccessor;
  private final String tableName;
  private int currentBatchSize = 0;
  private int committedBatchSize = 0;
  private int numBatches = 0;
  // The time when the first statement is added
  private long startTime = 0;

  /**
   * Constructor to instantiate a {@link BatchUpdater}.
   * @param dataAccessor The {@link MySqlDataAccessor}.
   * @param sql The sql command to prepare the statement.
   * @param tableName The name of the table that update statements are targeting at.
   * @param maxBatchSize The max batch size.
   * @throws SQLException
   */
  public BatchUpdater(MySqlDataAccessor dataAccessor, String sql, String tableName, int maxBatchSize)
      throws SQLException {
    this.dataAccessor = Objects.requireNonNull(dataAccessor, "MySqlDataAccessor is empty");
    this.tableName = Objects.requireNonNull(tableName, "TableName is empty");
    if (maxBatchSize < 0) {
      throw new IllegalArgumentException("MaxBatchSize is not valid: " + maxBatchSize);
    }
    this.maxBatchSize = maxBatchSize;
    try {
      // Calling getPreparedStatement first, since it will setup connection if there is none
      statement = dataAccessor.getPreparedStatement(sql, true);
      statement.clearBatch();
      autoCommit = dataAccessor.getAutoCommmit();
      dataAccessor.setAutoCommit(false);
    } catch (SQLException e) {
      dataAccessor.onException(e, BatchUpdate);
      logger.error("Failed to prepare for batch insert on {}", tableName, e);
      throw e;
    }
  }

  /**
   * Add statement to the batch. Since the sql statement provided to the constructor is a prepared statement,
   * this method only takes a lambda to supply values to the {@link PreparedStatement}.
   * @param valueSupplier The consumer to supply the values. The {@link PreparedStatement} is the parameter to this
   *                      consumer. It can also throws an {@link SQLException}.
   * @throws SQLException
   */
  protected synchronized void addUpdateToBatch(GenericThrowableConsumer<PreparedStatement, SQLException> valueSupplier)
      throws SQLException {
    try {
      if (startTime == 0) {
        startTime = System.currentTimeMillis();
      }
      if (maxBatchSize != 0 && currentBatchSize >= maxBatchSize) {
        executeBatchAndCommit();
      }
      valueSupplier.accept(statement);
      statement.addBatch();
      currentBatchSize++;
    } catch (SQLException e) {
      rollback(e);
      throw e;
    }
  }

  /**
   * Flush all the sql commands added to this batch. If it fails, the batch would be rolled back.
   * @throws SQLException
   */
  public void flush() throws SQLException {
    try {
      executeBatchAndCommit();
      if (startTime != 0) {
        dataAccessor.onSuccess(BatchUpdate, System.currentTimeMillis() - startTime);
      }
      dataAccessor.setAutoCommit(autoCommit);
    } catch (SQLException e) {
      rollback(e);
      throw e;
    }
  }

  /**
   * Execute all the buffered sql commands in the batch.
   * @throws SQLException
   */
  private void executeBatchAndCommit() throws SQLException {
    if (currentBatchSize == 0) {
      return;
    }
    statement.executeBatch();
    numBatches++;
    dataAccessor.commit();
    statement.clearBatch();
    committedBatchSize += currentBatchSize;
    currentBatchSize = 0;
  }

  /**
   * Rollback the transaction due to exception {@code e}.
   * @param e The {@link SQLException}.
   * @throws SQLException
   */
  private void rollback(SQLException e) throws SQLException {
    logger.error("Failed batch operation on {}, current batch size {}, already committed {}, rolling back", tableName,
        currentBatchSize, committedBatchSize, e);
    try {
      // First try to rollback the transaction, this might fail due to connection error.
      dataAccessor.rollback();
    } finally {
      // Then deal with exception, this might close the connection.
      dataAccessor.onException(e, BatchUpdate);
      if (dataAccessor.hasActiveConnection()) {
        dataAccessor.setAutoCommit(autoCommit);
      }
    }
  }

  /**
   * Only For testing.
   * @return The number of batches;
   */
  int getNumBatches() {
    return numBatches;
  }
}
