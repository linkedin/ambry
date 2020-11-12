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

import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.mysql.MySqlDataAccessor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;


/**
 * Container Data Access Object.
 */
public class ContainerDao {
  public static final String CONTAINER_TABLE = "Containers";
  // TODO: make Container key constants public
  public static final String ACCOUNT_ID = "accountId";
  public static final String CONTAINER_ID = "containerId";
  public static final String CONTAINER_INFO = "containerInfo";
  public static final String VERSION = "version";
  public static final String CREATION_TIME = "creationTime";
  public static final String LAST_MODIFIED_TIME = "lastModifiedTime";

  public static final String INDEX_ACCOUNT_CONTAINER = "containers.accountContainer";
  public static final String INDEX_CONTAINER_NAME = "containers.uniqueName";

  private final MySqlDataAccessor dataAccessor;
  private final String insertSql;
  private final String getSinceSql;
  private final String getByAccountSql;
  private final String updateSql;

  public ContainerDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = dataAccessor;
    insertSql = String.format("insert into %s (%s, %s, %s, %s, %s) values (?, ?, ?, now(3), now(3))", CONTAINER_TABLE,
        ACCOUNT_ID, CONTAINER_INFO, VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getSinceSql = String.format("select %s, %s, %s, %s from %s where %s > ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
        LAST_MODIFIED_TIME, CONTAINER_TABLE, LAST_MODIFIED_TIME);
    getByAccountSql = String.format("select %s, %s, %s, %s from %s where %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
        LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID);
    updateSql = String.format("update %s set %s = ?, %s = ?, %s = now(3) where %s = ? AND %s = ? ", CONTAINER_TABLE,
        CONTAINER_INFO, VERSION, LAST_MODIFIED_TIME, ACCOUNT_ID, CONTAINER_ID);
  }

  /**
   * Add a container to the database.
   * @param accountId the container's parent account id.
   * @param container the container to insert.
   * @throws SQLException
   */
  public void addContainer(int accountId, Container container) throws SQLException {
    try {
      // Note: assuming autocommit for now
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql, true);
      insertStatement.setInt(1, accountId);
      insertStatement.setString(2, container.toJson().toString());
      insertStatement.setInt(3, container.getSnapshotVersion());
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Updates a container in the database.
   * @param accountId the container's parent account id.
   * @param container the container to update.
   * @throws SQLException
   */
  public void updateContainer(int accountId, Container container) throws SQLException {
    try {
      // Note: assuming autocommit for now
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement updateStatement = dataAccessor.getPreparedStatement(updateSql, true);
      updateStatement.setString(1, container.toJson().toString());
      updateStatement.setInt(2, container.getSnapshotVersion());
      updateStatement.setInt(3, accountId);
      updateStatement.setInt(4, container.getId());
      updateStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Gets the containers in a specified account.
   * @param accountId the id for the parent account.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  public List<Container> getContainers(int accountId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    PreparedStatement getByAccountStatement = dataAccessor.getPreparedStatement(getByAccountSql, false);
    getByAccountStatement.setInt(1, accountId);
    try (ResultSet rs = getByAccountStatement.executeQuery()) {
      List<Container> containers = convertResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Gets all containers that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  public List<Container> getNewContainers(long updatedSince) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Timestamp sinceTime = new Timestamp(updatedSince);
    PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getSinceSql, false);
    getSinceStatement.setTimestamp(1, sinceTime);
    try (ResultSet rs = getSinceStatement.executeQuery()) {
      List<Container> containers = convertResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Convert a query result set to a list of containers.
   * @param resultSet the result set.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  private List<Container> convertResultSet(ResultSet resultSet) throws SQLException {
    List<Container> containers = new ArrayList<>();
    while (resultSet.next()) {
      int accountId = resultSet.getInt(ACCOUNT_ID);
      String containerJson = resultSet.getString(CONTAINER_INFO);
      Timestamp lastModifiedTime = resultSet.getTimestamp(LAST_MODIFIED_TIME);
      int version = resultSet.getInt(VERSION);
      Container container = new ContainerBuilder(
          Container.fromJson(new JSONObject(containerJson), (short) accountId)).setLastModifiedTime(
          lastModifiedTime.getTime()).setSnapshotVersion(version).build();
      containers.add(container);
    }
    return containers;
  }
}
