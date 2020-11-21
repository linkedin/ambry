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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Integration tests for {@link MySqlNamedBlobDb}.
 */
public class MySqlNamedBlobDbIntegrationTest {
  private static final String LOCAL_DC = "dc1";
  private final MySqlNamedBlobDb namedBlobDb;
  private final InMemAccountService accountService;
  private final PartitionId partitionId;

  public MySqlNamedBlobDbIntegrationTest() throws Exception {
    Properties properties = Utils.loadPropsFromResource("mysql.properties");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, LOCAL_DC);
    accountService = new InMemAccountService(false, false);
    for (int i = 0; i < 5; i++) {
      accountService.createAndAddRandomAccount();
    }
    MockClusterMap clusterMap = new MockClusterMap();
    partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MySqlNamedBlobDbFactory namedBlobDbFactory =
        new MySqlNamedBlobDbFactory(new VerifiableProperties(properties), new MetricRegistry(), accountService);
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();

    cleanup();
  }

  /**
   * Tests sequences of puts, gets, lists, and deletes across multiple containers.
   * @throws Exception
   */
  @Test
  public void testPutGetListDeleteSequence() throws Exception {
    int blobsPerContainer = 5;

    List<NamedBlobRecord> records = new ArrayList<>();
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          String blobId = getBlobId(account, container);
          String blobName = "name/" + i + "/more path segments--";
          long expirationTime =
              i % 2 == 0 ? Utils.Infinite_Time : System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);
          namedBlobDb.put(record).get();
          records.add(record);
        }
      }
    }

    // get records just inserted
    for (NamedBlobRecord record : records) {
      NamedBlobRecord recordFromStore =
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      assertEquals("Record does not match expectations.", record, recordFromStore);
    }

    // list records in each container
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        Page<NamedBlobRecord> page = namedBlobDb.list(account.getName(), container.getName(), "name", null).get();
        assertNull("No continuation token expected", page.getContinuationToken());
        assertEquals("Unexpected number of blobs in container", blobsPerContainer, page.getElements().size());
      }
    }

    // check that puts to the same keys fail.
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          String blobId = getBlobId(account, container);
          String blobName = "name/" + i + "/more path segments--";
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);
          checkErrorCode(() -> namedBlobDb.put(record), RestServiceErrorCode.Conflict);
        }
      }
    }

    // delete the records and check that they cannot be fetched with a get call.
    for (NamedBlobRecord record : records) {
      namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()),
          RestServiceErrorCode.Deleted);
    }

    // deletes should be idempotent and additional delete calls should succeed
    for (NamedBlobRecord record : records) {
      namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
    }

    records.clear();
    // should be able to put new records again after deletion
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          String blobId = getBlobId(account, container);
          String blobName = "name/" + i + "/more path segments--";
          long expirationTime =
              i % 2 == 1 ? Utils.Infinite_Time : System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);
          namedBlobDb.put(record).get();
          records.add(record);
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

  /**
   * Empties the accounts and containers tables.
   * @throws SQLException
   */
  private void cleanup() throws SQLException {
    for (DataSource dataSource : namedBlobDb.getDataSources().values()) {
      try (Connection connection = dataSource.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.executeUpdate("DELETE FROM named_blobs");
        }
      }
    }
  }
}
