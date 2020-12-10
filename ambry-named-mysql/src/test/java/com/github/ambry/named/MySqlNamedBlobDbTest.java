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
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.utils.TestUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Tests for {@link MySqlNamedBlobDb} that require a mocked data source.
 */
public class MySqlNamedBlobDbTest {
  private final List<String> datacenters = Arrays.asList("dc1", "dc2", "dc3");
  private final String localDatacenter = datacenters.get(0);
  private final MockDataSourceFactory dataSourceFactory = new MockDataSourceFactory();
  private final InMemAccountService accountService = new InMemAccountService(false, false);
  private final MySqlNamedBlobDb namedBlobDb;
  private final Account account;
  private final Container container;

  public MySqlNamedBlobDbTest() {
    Properties properties = new Properties();
    JSONArray dbInfo = new JSONArray();
    for (String datacenter : datacenters) {
      dbInfo.put(new JSONObject().put("url", "jdbc:mysql://" + datacenter)
          .put("datacenter", datacenter)
          .put("isWriteable", true)
          .put("username", "test")
          .put("password", "password"));
    }
    properties.setProperty(MySqlNamedBlobDbConfig.DB_INFO, dbInfo.toString());
    namedBlobDb = new MySqlNamedBlobDb(accountService, new MySqlNamedBlobDbConfig(new VerifiableProperties(properties)),
        dataSourceFactory, localDatacenter);
    account = accountService.createAndAddRandomAccount();
    container = account.getAllContainers().iterator().next();
  }

  /**
   * Test connection failure.
   */
  @Test
  public void testConnectionFailure() throws Exception {
    SQLException sqlException = new SQLException("bad");
    dataSourceFactory.triggerConnectionError(localDatacenter, sqlException);
    TestUtils.assertException(ExecutionException.class,
        () -> namedBlobDb.get(account.getName(), container.getName(), "blobName").get(),
        e -> Assert.assertEquals(sqlException, e.getCause()));
  }

  /**
   * Test query execution failure
   */
  @Test
  public void testQueryExecutionFailure() throws Exception {
    SQLException sqlException = new SQLException("bad");
    dataSourceFactory.triggerQueryExecutionError(localDatacenter, sqlException);
    TestUtils.assertException(ExecutionException.class,
        () -> namedBlobDb.get(account.getName(), container.getName(), "blobName").get(),
        e -> Assert.assertEquals(sqlException, e.getCause()));
  }

  private static class MockDataSourceFactory implements MySqlNamedBlobDb.DataSourceFactory {
    private final Map<String, DataSource> dataSources = new HashMap<>();

    @Override
    public DataSource getDataSource(MySqlUtils.DbEndpoint dbEndpoint) {
      return dataSources.computeIfAbsent(dbEndpoint.getDatacenter(), k -> mock(DataSource.class));
    }

    private void triggerConnectionError(String datacenter, SQLException connectionError) {
      try {
        DataSource dataSource = dataSources.get(datacenter);
        reset(dataSource);
        when(dataSource.getConnection()).thenThrow(connectionError);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    private void triggerQueryExecutionError(String datacenter, SQLException executionError) {
      try {
        DataSource dataSource = dataSources.get(datacenter);
        reset(dataSource);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenThrow(executionError);
        when(statement.executeUpdate()).thenThrow(executionError);
        when(statement.execute()).thenThrow(executionError);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(any())).thenReturn(statement);
        when(dataSource.getConnection()).thenReturn(connection);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

