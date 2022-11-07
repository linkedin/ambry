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
import com.github.ambry.frontend.Page;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
        assertNull("No continuation token expected", page.getNextPageToken());
        assertEquals("Unexpected number of blobs in container", blobsPerContainer, page.getEntries().size());
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
      DeleteResult deleteResult =
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      assertEquals("Unexpected deleted ID", record.getBlobId(), deleteResult.getBlobId());
      assertFalse("Unexpected alreadyDeleted value", deleteResult.isAlreadyDeleted());
      checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()),
          RestServiceErrorCode.Deleted);
      NamedBlobRecord recordFromStore =
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName(),
              GetOption.Include_Deleted_Blobs).get();
      checkRecordsEqual(record, recordFromStore);
      recordFromStore = namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName(),
          GetOption.Include_All).get();
      checkRecordsEqual(record, recordFromStore);
    }

    // deletes should be idempotent and additional delete calls should succeed
    for (NamedBlobRecord record : records) {
      DeleteResult deleteResult =
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      assertEquals("Unexpected deleted ID", record.getBlobId(), deleteResult.getBlobId());
      assertTrue("Unexpected alreadyDeleted value", deleteResult.isAlreadyDeleted());
    }

    // delete and get for non existent blobs should return not found.
    for (NamedBlobRecord record : records) {
      String nonExistentName = record.getBlobName() + "-other";
      checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), nonExistentName),
          RestServiceErrorCode.NotFound);
      checkErrorCode(() -> namedBlobDb.delete(record.getAccountName(), record.getContainerName(), nonExistentName),
          RestServiceErrorCode.NotFound);
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
   * Test behavior with expired blobs
   */
  @Test
  public void testExpiredBlobs() throws Exception {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();

    String blobId = getBlobId(account, container);
    String blobName = "name";
    long expirationTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);
    namedBlobDb.put(record).get();

    Thread.sleep(100);
    checkErrorCode(() -> namedBlobDb.get(account.getName(), container.getName(), blobName),
        RestServiceErrorCode.Deleted);
    NamedBlobRecord recordFromStore =
        namedBlobDb.get(account.getName(), container.getName(), blobName, GetOption.Include_All).get();
    assertEquals("Record does not match expectations.", record, recordFromStore);
    recordFromStore =
        namedBlobDb.get(account.getName(), container.getName(), blobName, GetOption.Include_Expired_Blobs).get();
    assertEquals("Record does not match expectations.", record, recordFromStore);

    // replacement should succeed
    blobId = getBlobId(account, container);
    record = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);
    namedBlobDb.put(record).get();
    assertEquals("Record should have been replaced", record,
        namedBlobDb.get(account.getName(), container.getName(), blobName).get());
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
          statement.executeUpdate("DELETE FROM named_blobs_v2");
        }
      }
    }
  }

  private void checkRecordsEqual(NamedBlobRecord record1, NamedBlobRecord record2) {
    assertEquals("AccountName mismatch", record1.getAccountName(), record2.getAccountName());
    assertEquals("ContainerName mismatch", record1.getContainerName(), record2.getContainerName());
    assertEquals("BlobName mismatch", record1.getBlobName(), record2.getBlobName());
    assertEquals("BlobId mismatch", record1.getBlobId(), record2.getBlobId());
  }
}
