/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ambry.accountstats;

import com.github.ambry.mysql.MySqlMetrics;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Persists a completion marker around each host account report publication.
 */
class HostAccountReportsStateDao {
  static final String HOST_ACCOUNT_REPORTS_STATE_TABLE = "HostAccountReportsState";
  private static final String MARK_STARTED_SQL = String.format(
      "INSERT INTO %s (clusterName, hostname, reportTimestampMs, reportVersion, isComplete, reportedPartitions) "
          + "VALUES (?, ?, ?, 1, 0, ?) ON DUPLICATE KEY UPDATE reportTimestampMs=?, "
          + "reportVersion=reportVersion+1, isComplete=0, reportedPartitions=?",
      HOST_ACCOUNT_REPORTS_STATE_TABLE);
  private static final String MARK_COMPLETE_SQL = String.format(
      "UPDATE %s SET isComplete=1 WHERE clusterName=? AND hostname=? AND reportVersion=? AND isComplete=0",
      HOST_ACCOUNT_REPORTS_STATE_TABLE);
  private static final String QUERY_SQL = String.format(
      "SELECT reportTimestampMs, reportVersion, isComplete, reportedPartitions FROM %s "
          + "WHERE clusterName=? AND hostname=?",
      HOST_ACCOUNT_REPORTS_STATE_TABLE);
  private static final String DELETE_SQL =
      String.format("DELETE FROM %s WHERE clusterName=? AND hostname=?", HOST_ACCOUNT_REPORTS_STATE_TABLE);
  private static final Logger logger = LoggerFactory.getLogger(HostAccountReportsStateDao.class);
  private final DataSource dataSource;
  private final MySqlMetrics metrics;

  HostAccountReportsStateDao(DataSource dataSource, MySqlMetrics metrics) {
    this.dataSource = Objects.requireNonNull(dataSource, "DataSource is empty");
    this.metrics = Objects.requireNonNull(metrics, "Metrics is empty");
  }

  State markReportStarted(String clusterName, String hostname, long reportTimestampMs, String reportedPartitions)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement(MARK_STARTED_SQL)) {
        statement.setString(1, clusterName);
        statement.setString(2, hostname);
        statement.setLong(3, reportTimestampMs);
        statement.setString(4, reportedPartitions);
        statement.setLong(5, reportTimestampMs);
        statement.setString(6, reportedPartitions);
        statement.executeUpdate();
        State state = queryState(connection, clusterName, hostname);
        if (state == null) {
          throw new SQLException("Account report publication state was not created for host " + hostname);
        }
        connection.commit();
        connection.setAutoCommit(autoCommit);
        return state;
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error("Failed to mark account report publication started for host {}", hostname, e);
      throw e;
    }
  }

  void markReportComplete(String clusterName, String hostname, long reportVersion) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(MARK_COMPLETE_SQL)) {
      statement.setString(1, clusterName);
      statement.setString(2, hostname);
      statement.setLong(3, reportVersion);
      if (statement.executeUpdate() != 1) {
        throw new SQLException("Account report publication state changed before completion for host " + hostname);
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error("Failed to mark account report publication complete for host {}", hostname, e);
      throw e;
    }
  }

  State queryState(String clusterName, String hostname) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      return queryState(connection, clusterName, hostname);
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
      logger.error("Failed to query account report publication state for host {}", hostname, e);
      throw e;
    }
  }

  void deleteState(String clusterName, String hostname) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(DELETE_SQL)) {
      statement.setString(1, clusterName);
      statement.setString(2, hostname);
      statement.executeUpdate();
    } catch (SQLException e) {
      metrics.deleteFailureCount.inc();
      logger.error("Failed to delete account report publication state for host {}", hostname, e);
      throw e;
    }
  }

  private State queryState(Connection connection, String clusterName, String hostname) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(QUERY_SQL)) {
      statement.setString(1, clusterName);
      statement.setString(2, hostname);
      try (ResultSet resultSet = statement.executeQuery()) {
        return resultSet.next() ? new State(resultSet.getLong("reportTimestampMs"),
            resultSet.getLong("reportVersion"), resultSet.getBoolean("isComplete"),
            resultSet.getString("reportedPartitions")) : null;
      }
    }
  }

  static class State {
    private final long reportTimestampMs;
    private final long reportVersion;
    private final boolean complete;
    private final String reportedPartitions;

    State(long reportTimestampMs, long reportVersion, boolean complete, String reportedPartitions) {
      this.reportTimestampMs = reportTimestampMs;
      this.reportVersion = reportVersion;
      this.complete = complete;
      this.reportedPartitions = reportedPartitions;
    }

    long getReportTimestampMs() {
      return reportTimestampMs;
    }

    long getReportVersion() {
      return reportVersion;
    }

    boolean isComplete() {
      return complete;
    }

    String getReportedPartitions() {
      return reportedPartitions;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof State)) {
        return false;
      }
      State that = (State) other;
      return reportTimestampMs == that.reportTimestampMs && reportVersion == that.reportVersion
          && complete == that.complete && Objects.equals(reportedPartitions, that.reportedPartitions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(reportTimestampMs, reportVersion, complete, reportedPartitions);
    }
  }
}
