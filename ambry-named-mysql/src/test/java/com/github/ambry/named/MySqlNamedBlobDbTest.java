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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.mysql.MySqlUtils.DbEndpoint.SSLMode;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
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
  private final String foundDatacenter = "dc3";
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
          .put("password", "password")
          .put("sslMode", SSLMode.NONE));
    }
    properties.setProperty(MySqlNamedBlobDbConfig.DB_INFO, dbInfo.toString());
    namedBlobDb = new MySqlNamedBlobDb(accountService, new MySqlNamedBlobDbConfig(new VerifiableProperties(properties)),
        dataSourceFactory, localDatacenter, new Metrics(new MetricRegistry(), ""));
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
    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
    namedBlobDb.cleanupStaleData(staleNamedBlobs);
  }

  /**
   * Verify that {@link MySqlNamedBlobDb.TransactionExecutor} registers the per-datacenter queue-size and
   * active-count gauges, the per-datacenter rejected-count counter, and the per-datacenter enqueue-wait histogram
   * on construction.
   */
  @Test
  public void testTransactionExecutorMetricsRegistered() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Metrics metrics = new Metrics(registry, "");
    MySqlNamedBlobDb db =
        new MySqlNamedBlobDb(accountService, buildSmallPoolConfig(1, 1, 100), dataSourceFactory, localDatacenter,
            metrics);
    try {
      for (String dc : datacenters) {
        assertTrue("queue-size gauge missing for datacenter " + dc, registry.getGauges()
            .containsKey("com.github.ambry.named.MySqlNamedBlobDb.TransactionExecutorQueueSize." + dc));
        assertTrue("active-count gauge missing for datacenter " + dc, registry.getGauges()
            .containsKey("com.github.ambry.named.MySqlNamedBlobDb.TransactionExecutorActiveCount." + dc));
        assertTrue("rejected counter missing for datacenter " + dc, registry.getCounters()
            .containsKey("com.github.ambry.named.MySqlNamedBlobDb.NamedBlobTransactionRejectedCount." + dc));
        assertTrue("enqueue-wait histogram missing for datacenter " + dc, registry.getHistograms()
            .containsKey("com.github.ambry.named.MySqlNamedBlobDb.NamedBlobEnqueueWaitTimeInMs." + dc));
      }
    } finally {
      db.close();
    }
  }

  /**
   * Verify that {@link MySqlNamedBlobDb.TransactionExecutor#close()} deregisters all per-datacenter metrics so
   * subsequent instantiations do not leak metrics or fail with duplicate-registration errors.
   */
  @Test
  public void testTransactionExecutorGaugesRemovedOnClose() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Metrics metrics = new Metrics(registry, "");
    MySqlNamedBlobDb db =
        new MySqlNamedBlobDb(accountService, buildSmallPoolConfig(1, 1, 100), dataSourceFactory, localDatacenter,
            metrics);
    db.close();
    for (String dc : datacenters) {
      assertFalse("queue-size gauge for " + dc + " should be removed after close", registry.getGauges()
          .containsKey("com.github.ambry.named.MySqlNamedBlobDb.TransactionExecutorQueueSize." + dc));
      assertFalse("active-count gauge for " + dc + " should be removed after close", registry.getGauges()
          .containsKey("com.github.ambry.named.MySqlNamedBlobDb.TransactionExecutorActiveCount." + dc));
      assertFalse("rejected counter for " + dc + " should be removed after close", registry.getCounters()
          .containsKey("com.github.ambry.named.MySqlNamedBlobDb.NamedBlobTransactionRejectedCount." + dc));
      assertFalse("enqueue-wait histogram for " + dc + " should be removed after close", registry.getHistograms()
          .containsKey("com.github.ambry.named.MySqlNamedBlobDb.NamedBlobEnqueueWaitTimeInMs." + dc));
    }
  }

  /**
   * Verify that {@link MySqlNamedBlobDb.TransactionExecutor} records enqueue-wait time on a successful submission.
   */
  @Test
  public void testEnqueueWaitTimeRecordedOnSuccessfulSubmit() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Metrics metrics = new Metrics(registry, "");
    dataSourceFactory.setLocalDatacenter(localDatacenter);
    dataSourceFactory.triggerEmptyResultSetForLocalDataCenter(datacenters);
    MySqlNamedBlobDb db =
        new MySqlNamedBlobDb(accountService, buildSmallPoolConfig(1, 1, 100), dataSourceFactory, localDatacenter,
            metrics);
    try {
      // Issue any DB call; the resolution itself does not need to succeed for the histogram update to fire.
      try {
        db.get(account.getName(), container.getName(), "blobName").get(10, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // We only care that the worker thread ran and recorded the histogram.
      }
      Histogram enqueueWaitTime = registry.getHistograms()
          .get("com.github.ambry.named.MySqlNamedBlobDb.NamedBlobEnqueueWaitTimeInMs." + localDatacenter);
      assertNotNull("per-datacenter enqueue-wait histogram missing", enqueueWaitTime);
      assertTrue("enqueue-wait histogram should have at least one sample", enqueueWaitTime.getCount() >= 1);
    } finally {
      db.close();
    }
  }

  /**
   * Verify that submissions are rejected with {@link RestServiceErrorCode#ServiceUnavailable} when the per-datacenter
   * work queue is full, and that the rejection counter increments. Uses {@code delete} (autoCommit=false) which routes
   * directly to the local datacenter executor without the cross-DC retry policy that {@code get} applies.
   */
  @Test
  public void testTransactionRejectedWhenQueueFull() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Metrics metrics = new Metrics(registry, "");
    CountDownLatch workerEntered = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);

    MySqlNamedBlobDb.DataSourceFactory blockingFactory = endpoint -> {
      DataSource ds = mock(DataSource.class);
      try {
        when(ds.getConnection()).thenAnswer(inv -> {
          workerEntered.countDown();
          if (!releaseWorker.await(30, TimeUnit.SECONDS)) {
            throw new SQLException("test timeout waiting for release");
          }
          // Build a connection that yields no rows so the lambda completes once released. The
          // delete path uses executeUpdate, which we let return 0 rows affected.
          Connection connection = mock(Connection.class);
          PreparedStatement statement = mock(PreparedStatement.class);
          ResultSet resultSet = mock(ResultSet.class);
          when(resultSet.next()).thenReturn(false);
          when(statement.executeQuery()).thenReturn(resultSet);
          when(statement.executeUpdate()).thenReturn(0);
          when(connection.prepareStatement(any())).thenReturn(statement);
          return connection;
        });
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      return ds;
    };

    MySqlNamedBlobDb db =
        new MySqlNamedBlobDb(accountService, buildSmallPoolConfig(1, 1, 1), blockingFactory, localDatacenter, metrics);
    try {
      // Op 1: occupies the single worker (blocks on releaseWorker latch).
      CompletableFuture<?> op1 = db.delete(account.getName(), container.getName(), "blob1");
      assertTrue("worker did not enter dataSource.getConnection()",
          workerEntered.await(10, TimeUnit.SECONDS));
      // Op 2: fills the bounded queue (capacity 1).
      CompletableFuture<?> op2 = db.delete(account.getName(), container.getName(), "blob2");
      // Op 3: queue is full, worker is busy → AbortPolicy rejects.
      CompletableFuture<?> op3 = db.delete(account.getName(), container.getName(), "blob3");

      TestUtils.assertException(ExecutionException.class, () -> op3.get(5, TimeUnit.SECONDS), e -> {
        RestServiceException rse = (RestServiceException) e.getCause();
        assertEquals("rejected transaction should surface as ServiceUnavailable",
            RestServiceErrorCode.ServiceUnavailable, rse.getErrorCode());
      });
      Counter rejected = registry.getCounters()
          .get("com.github.ambry.named.MySqlNamedBlobDb.NamedBlobTransactionRejectedCount." + localDatacenter);
      assertNotNull("per-datacenter rejected counter missing", rejected);
      assertEquals("rejected counter should be 1 after one rejection", 1, rejected.getCount());

      // Confirm gauges reflect the busy/queued state before we drain.
      Gauge<?> queueGauge = registry.getGauges()
          .get("com.github.ambry.named.MySqlNamedBlobDb.TransactionExecutorQueueSize." + localDatacenter);
      Gauge<?> activeGauge = registry.getGauges()
          .get("com.github.ambry.named.MySqlNamedBlobDb.TransactionExecutorActiveCount." + localDatacenter);
      assertNotNull("queue-size gauge missing", queueGauge);
      assertNotNull("active-count gauge missing", activeGauge);
      assertEquals("queue should hold op2", 1, ((Number) queueGauge.getValue()).intValue());
      assertEquals("worker should be running op1", 1, ((Number) activeGauge.getValue()).intValue());

      // Drain the worker so op1 / op2 can complete cleanly during shutdown.
      releaseWorker.countDown();
      try {
        op1.get(10, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // delete on an empty result set typically yields NotFound; we don't care about op1's outcome here.
      }
      try {
        op2.get(10, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // same as above.
      }
    } finally {
      releaseWorker.countDown();
      db.close();
    }
  }

  /**
   * Build a {@link MySqlNamedBlobDbConfig} with the test datacenters and the given pool / queue sizing.
   */
  private MySqlNamedBlobDbConfig buildSmallPoolConfig(int localPoolSize, int remotePoolSize,
      int maxPendingTransactionsPerDatacenter) {
    Properties properties = new Properties();
    JSONArray dbInfo = new JSONArray();
    for (String datacenter : datacenters) {
      dbInfo.put(new JSONObject().put("url", "jdbc:mysql://" + datacenter)
          .put("datacenter", datacenter)
          .put("isWriteable", true)
          .put("username", "test")
          .put("password", "password")
          .put("sslMode", SSLMode.NONE));
    }
    properties.setProperty(MySqlNamedBlobDbConfig.DB_INFO, dbInfo.toString());
    properties.setProperty(MySqlNamedBlobDbConfig.LOCAL_POOL_SIZE, Integer.toString(localPoolSize));
    properties.setProperty(MySqlNamedBlobDbConfig.REMOTE_POOL_SIZE, Integer.toString(remotePoolSize));
    properties.setProperty(MySqlNamedBlobDbConfig.MAX_PENDING_TRANSACTIONS_PER_DATACENTER,
        Integer.toString(maxPendingTransactionsPerDatacenter));
    return new MySqlNamedBlobDbConfig(new VerifiableProperties(properties));
  }


  /**
   * Helper method to obtain stale blobs
   */
  private List<StaleNamedBlob> getStaleBlobList() throws ExecutionException, InterruptedException {
    List<StaleNamedBlob> staleNamedBlobsList = new ArrayList<>();
    NamedBlobDb.StaleBlobsWithLatestBlobName staleBlobsWithLatestBlobName;

    Set<Container> containers = accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE);
    for (Container container : containers) {

      staleBlobsWithLatestBlobName = namedBlobDb.pullStaleBlobs(container, "\0").get();
      staleNamedBlobsList.addAll(staleBlobsWithLatestBlobName.getStaleBlobs());
    }
    return staleNamedBlobsList;
  }

  @Test
  public void testUpdateBlobTtlAndStateToReady() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, 10);

    dataSourceFactory.setLocalDatacenter(foundDatacenter);
    dataSourceFactory.triggerDataResultSet(datacenters);

    long expirationTimeMs = calendar.getTimeInMillis() + 5000;
    final NamedBlobRecord record = new NamedBlobRecord(account.getName(), container.getName(), "blobName", id, expirationTimeMs);
    PutResult putResult = namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();

    NamedBlobRecord updatedRecord =
        new NamedBlobRecord(record.getAccountName(), record.getContainerName(), record.getBlobName(),
            record.getBlobId(), record.getExpirationTimeMs(), putResult.getInsertedRecord().getVersion());
    namedBlobDb.updateBlobTtlAndStateToReady(updatedRecord).get();

    NamedBlobRecord namedBlobRecord = namedBlobDb.get(account.getName(), container.getName(), "blobName").get();
    assertEquals("Blob Id is not matched with the record", id, namedBlobRecord.getBlobId());
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
            when(resultSet.getBytes(1)).thenReturn(Utils.base64DecodeUrlSafe(id));
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

    private void triggerDataResultSet(List<String> datacenters) {
      for (String datacenter : datacenters) {
        try {
          DataSource dataSource = dataSources.get(datacenter);
          reset(dataSource);
          PreparedStatement statement = mock(PreparedStatement.class);
          ResultSet resultSet = mock(ResultSet.class);
          when(resultSet.next()).thenReturn(true);
          when(resultSet.getBytes(1)).thenReturn(Utils.base64DecodeUrlSafe(id));
          when(statement.executeQuery()).thenReturn(resultSet);
          Connection connection = mock(Connection.class);
          when(connection.prepareStatement(any())).thenReturn(statement);
          when(dataSource.getConnection()).thenReturn(connection);
          when(statement.executeUpdate()).thenReturn(1);
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

