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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link MySqlNamedBlobDb} that require a mocked data source.
 */
public class MySqlNamedBlobDbTest {
  private final List<String> datacenters = Arrays.asList("dc3", "dc2", "dc1");
  private static final Set<String> notFoundDatacenters = new HashSet<>();
  private final String localDatacenter = "dc1";
  private final MockDataSourceFactory dataSourceFactory = new MockDataSourceFactory();
  private final InMemAccountService accountService = new InMemAccountService(false, false);
  private final MySqlNamedBlobDb namedBlobDb;
  private final Account account;
  private final Container container;
  private PartitionId partitionId;
  private static String id;

  public MySqlNamedBlobDbTest() throws IOException {
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
        dataSourceFactory, localDatacenter, new MetricRegistry());
    account = accountService.createAndAddRandomAccount();
    container = account.getAllContainers().iterator().next();
    MockClusterMap clusterMap = new MockClusterMap();
    partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    id = getBlobId(account, container);
    notFoundDatacenters.addAll(Arrays.asList("dc1", "dc2"));
  }

  @Test
  public void testRetryGetNamedBlobFromDiffDcs() throws Exception {
    dataSourceFactory.setLocalDatacenter(localDatacenter);
    dataSourceFactory.triggerEmptyResultSetForLocalDataCenter(datacenters);
    NamedBlobRecord namedBlobRecord = namedBlobDb.get(account.getName(), container.getName(), "blobName").get();
    assertEquals("Blob Id is not matched with the record", id, namedBlobRecord.getBlobId());
    NamedBlobRecord namedBlobRecord1 = namedBlobDb.get(account.getName(), container.getName(), "blobName").get();
    assertEquals("Blob Id is not matched with the record", id, namedBlobRecord1.getBlobId());
    dataSourceFactory.triggerEmptyResultSetForAllDataCenters(datacenters);
    checkErrorCode(() -> namedBlobDb.get(account.getName(), container.getName(), "blobName"), RestServiceErrorCode.NotFound);
  }

  @Test
  public void testRetryDeleteNamedBlob() throws Exception {
    dataSourceFactory.setLocalDatacenter(localDatacenter);
    dataSourceFactory.triggerEmptyResultSetForLocalDataCenter(datacenters);
    checkErrorCode(() -> namedBlobDb.delete(account.getName(), container.getName(), "blobName"), RestServiceErrorCode.NotFound);
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

  @Test
  public void testPullAndCleanStaleNamedBlobs() throws Exception {
    dataSourceFactory.setLocalDatacenter(localDatacenter);
    dataSourceFactory.triggerEmptyResultSetForLocalDataCenter(datacenters);
    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    namedBlobDb.cleanupStaleData(staleNamedBlobs);
  }

  /**
   * @param callable an async call, where the {@link Future} is expected to be completed with an exception.
   * @param errorCode the expected {@link RestServiceErrorCode}.
   */
  private void checkErrorCode(Callable<Future<?>> callable, RestServiceErrorCode errorCode) throws Exception {
    TestUtils.assertException(ExecutionException.class, () -> callable.call().get(), e -> {
      RestServiceException rse = (RestServiceException) e.getCause();
      assertEquals("Unexpected error code for get after delete", errorCode, rse.getErrorCode());
    });
  }

  private static class MockDataSourceFactory implements MySqlNamedBlobDb.DataSourceFactory {
    private final Map<String, DataSource> dataSources = new HashMap<>();
    private String localDatacenter;

    @Override
    public DataSource getDataSource(MySqlUtils.DbEndpoint dbEndpoint) {
      return dataSources.computeIfAbsent(dbEndpoint.getDatacenter(), k -> mock(DataSource.class));
    }

    private void setLocalDatacenter(String localDatacenter) {
      this.localDatacenter = localDatacenter;
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

    private void triggerEmptyResultSetForLocalDataCenter(List<String> datacenters) {
      for (String datacenter : datacenters) {
        try {
          DataSource dataSource = dataSources.get(datacenter);
          reset(dataSource);
          PreparedStatement statement = mock(PreparedStatement.class);
          ResultSet resultSet = mock(ResultSet.class);
          if (notFoundDatacenters.contains(datacenter)) {
            when(resultSet.next()).thenReturn(false);
            when(statement.executeQuery()).thenReturn(resultSet);
            Connection connection = mock(Connection.class);
            when(connection.prepareStatement(any())).thenReturn(statement);
            when(dataSource.getConnection()).thenReturn(connection);
          } else {
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getBytes(1)).thenReturn(Base64.decodeBase64(id));
            when(statement.executeQuery()).thenReturn(resultSet);
            Connection connection = mock(Connection.class);
            when(connection.prepareStatement(any())).thenReturn(statement);
            when(dataSource.getConnection()).thenReturn(connection);
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void triggerEmptyResultSetForAllDataCenters(List<String> datacenters) {
      for (String datacenter : datacenters) {
        try {
          DataSource dataSource = dataSources.get(datacenter);
          reset(dataSource);
          PreparedStatement statement = mock(PreparedStatement.class);
          ResultSet resultSet = mock(ResultSet.class);
          when(resultSet.next()).thenReturn(false);
          when(statement.executeQuery()).thenReturn(resultSet);
          Connection connection = mock(Connection.class);
          when(connection.prepareStatement(any())).thenReturn(statement);
          when(dataSource.getConnection()).thenReturn(connection);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Get a sample blob ID.
   * @param account the account of the blob.
   * @param container the container of the blob.
   * @return the base64 blob ID.
   */
  private String getBlobId(Account account, Container container) {
    return new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, account.getId(), container.getId(),
        partitionId, false, BlobId.BlobDataType.SIMPLE).getID();
  }
}

