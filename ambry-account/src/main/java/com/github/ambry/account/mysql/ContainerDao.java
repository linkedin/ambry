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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;


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

  private final MySqlDataAccessor dataAccessor;
  private final String insertSql;
  private final String getSinceSql;
  private final String getByAccountSql;
  private final String updateSql;

  public ContainerDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = dataAccessor;
    insertSql = String.format("insert into %s (%s, %s, %s, %s, %s) values (?, ?, 1, now(3), now(3))", CONTAINER_TABLE,
        ACCOUNT_ID, CONTAINER_INFO, VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getSinceSql =
        String.format("select %s, %s, %s from %s where %s > ?", ACCOUNT_ID, CONTAINER_INFO, LAST_MODIFIED_TIME,
            CONTAINER_TABLE, LAST_MODIFIED_TIME);
    getByAccountSql =
        String.format("select %s, %s, %s from %s where %s = ?", ACCOUNT_ID, CONTAINER_INFO, LAST_MODIFIED_TIME,
            CONTAINER_TABLE, ACCOUNT_ID);
    // TODO: For update, take the version from Container object after adding the field in it.
    updateSql = String.format("update %s set %s = ?, %s = 1, %s = now(3) where %s = ? AND %s = ? ", CONTAINER_TABLE,
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
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql);
      insertStatement.setInt(1, accountId);
      insertStatement.setString(2, container.toJson().toString());
      insertStatement.executeUpdate();
    } catch (SQLException e) {
      dataAccessor.onException(e);
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
      PreparedStatement updateStatement = dataAccessor.getPreparedStatement(updateSql);
      updateStatement.setString(1, container.toJson().toString());
      updateStatement.setInt(2, accountId);
      updateStatement.setInt(3, container.getId());
      updateStatement.executeUpdate();
    } catch (SQLException e) {
      dataAccessor.onException(e);
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
    PreparedStatement getByAccountStatement = dataAccessor.getPreparedStatement(getByAccountSql);
    getByAccountStatement.setInt(1, accountId);
    try (ResultSet rs = getByAccountStatement.executeQuery()) {
      return convertResultSet(rs);
    } catch (SQLException e) {
      dataAccessor.onException(e);
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
    Timestamp sinceTime = new Timestamp(updatedSince);
    PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getSinceSql);
    getSinceStatement.setTimestamp(1, sinceTime);
    try (ResultSet rs = getSinceStatement.executeQuery()) {
      return convertResultSet(rs);
    } catch (SQLException e) {
      dataAccessor.onException(e);
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
      Container container = Container.fromJson(new JSONObject(containerJson), (short) accountId);
      container = new ContainerBuilder(container).setLastModifiedTime(lastModifiedTime.getTime()).build();
      containers.add(container);
    }
    return containers;
  }
}
