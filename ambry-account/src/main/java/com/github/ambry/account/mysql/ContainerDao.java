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

import com.github.ambry.account.AccountSerdeUtils;
import com.github.ambry.account.Container;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


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

  private final Connection dbConnection;
  private final MySqlDataAccessor dataAccessor;
  private final PreparedStatement insertStatement;
  private final PreparedStatement getSinceStatement;
  private final PreparedStatement getByAccountStatement;

  public ContainerDao(MySqlDataAccessor dataAccessor) throws SQLException {
    this.dataAccessor = dataAccessor;
    this.dbConnection = dataAccessor.getDatabaseConnection();

    String insertSql =
        String.format("insert into %s (%s, %s, %s, %s, %s) values (?, ?, 1, now(), now())", CONTAINER_TABLE, ACCOUNT_ID,
            CONTAINER_INFO, VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    insertStatement = dbConnection.prepareStatement(insertSql);

    String getSinceSql =
        String.format("select %s, %s, %s from %s where %s > ?", ACCOUNT_ID, CONTAINER_INFO, LAST_MODIFIED_TIME,
            CONTAINER_TABLE, LAST_MODIFIED_TIME);
    getSinceStatement = dbConnection.prepareStatement(getSinceSql);

    String getByAccountSql =
        String.format("select %s, %s, %s from %s where %s = ?", ACCOUNT_ID, CONTAINER_INFO, LAST_MODIFIED_TIME,
            CONTAINER_TABLE, ACCOUNT_ID);
    getByAccountStatement = dbConnection.prepareStatement(getByAccountSql);
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
      insertStatement.setInt(1, accountId);
      insertStatement.setString(2, AccountSerdeUtils.containerToJson(container));
      insertStatement.executeUpdate();
    } catch (SQLException e) {
      // record failure, parse exception to figure out what we did wrong (eg. id or name collision)
      throw e;
    }
  }

  /**
   * Gets the containers in a specified account.
   * @param accountId the id for the account.
   * @return a list of {@link Container}.
   * @throws SQLException
   */
  public List<Container> getContainers(int accountId) throws SQLException {
    getByAccountStatement.setInt(1, accountId);
    try (ResultSet rs = getByAccountStatement.executeQuery()) {
      return convertResultSet(rs);
    }
  }

  /**
   * Gets all containers that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Container}.
   * @throws SQLException
   */
  public List<Container> getNewContainers(long updatedSince) throws SQLException {
    Timestamp sinceTime = new Timestamp(updatedSince);
    getSinceStatement.setTimestamp(1, sinceTime);
    try (ResultSet rs = getSinceStatement.executeQuery()) {
      return convertResultSet(rs);
    } catch (SQLException e) {
      // record failure, parse exception, ...
      throw e;
    }
  }

  private List<Container> convertResultSet(ResultSet rs) throws SQLException {
    List<Container> containers = new ArrayList<>();
    while (rs.next()) {
      int accountId = rs.getInt(ACCOUNT_ID);
      String containerJson = rs.getString(CONTAINER_INFO);
      Timestamp lastModifiedTime = rs.getTimestamp(LAST_MODIFIED_TIME);
      Container container = AccountSerdeUtils.containerFromJson(containerJson, (short) accountId);
      //container.setLastModifiedTime(lastModifiedTime);
      containers.add(container);
    }
    return containers;
  }
}
