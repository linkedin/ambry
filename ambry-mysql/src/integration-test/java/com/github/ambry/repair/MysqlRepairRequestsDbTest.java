/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.repair;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.MysqlRepairRequestsDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.Test;

import static com.github.ambry.repair.RepairRequestRecord.OperationType.*;
import static org.junit.Assert.*;


/**
 * Integration tests for {@link MysqlRepairRequestsDb}.
 */
public class MysqlRepairRequestsDbTest {
  private static final String LOCAL_DC = "DC1";
  private static final Random random = new Random();
  private MysqlRepairRequestsDb repairRequestsDb = null;
  private final InMemAccountService accountService;
  MockClusterMap clusterMap;

  public MysqlRepairRequestsDbTest() throws Exception {
    accountService = new InMemAccountService(false, false);
    for (int i = 0; i < 5; i++) {
      accountService.createAndAddRandomAccount();
    }
    clusterMap = new MockClusterMap();
  }

  private void prepareDb(int list_max_result) throws Exception {
    Properties properties = Utils.loadPropsFromResource("repairRequests_mysql.properties");
    properties.setProperty(MysqlRepairRequestsDbConfig.LIST_MAX_RESULTS, Integer.toString(list_max_result));
    properties.setProperty(MysqlRepairRequestsDbConfig.LOCAL_POOL_SIZE, "5");

    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metrics = new MetricRegistry();
    MysqlRepairRequestsDbFactory factory = new MysqlRepairRequestsDbFactory(verifiableProperties, metrics, LOCAL_DC);
    repairRequestsDb = factory.getRepairRequestsDb();

    cleanup();
  }

  /**
   * Tests sequences of puts, gets and deletes across multiple containers.
   * @throws Exception
   */
  @Test
  public void testPutGetDeleteSequence() throws Exception {
    prepareDb(100);

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    int blobsPerContainer = 5;

    String hostName1 = "localhost1";
    String hostName2 = "localhost2";
    int hostPort1 = 6024;
    int hostPort2 = 6025;
    // simulate the requests which will be sent on this host (thisNodeName, thisNodePort)
    String thisNodeName = hostName1;
    int thisNodePort = hostPort1;

    // Prepare RepairRequests and insert them to the DB.
    // Map<Partition ID, Map<BlobId, RepairRequestRecord>>
    Map<Integer, Map<String, RepairRequestRecord>> records = new HashMap<>();
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          PartitionId partitionId = partitionIds.get(random.nextInt(partitionIds.size()));
          String blobId = generateBlobId(account, container, partitionId);
          RepairRequestRecord.OperationType operationType = i % 2 == 0 ? TtlUpdateRequest : DeleteRequest;
          long operationTime = System.currentTimeMillis() - random.nextInt(1000);
          short lifeVersion = -1;
          long expirationTime =
              i % 2 == 0 ? Utils.Infinite_Time : System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
          String hostName = random.nextInt(2) == 0 ? hostName1 : hostName2;
          int hostPort = random.nextInt(2) == 0 ? hostPort1 : hostPort2;
          RepairRequestRecord record =
              new RepairRequestRecord(blobId, (int) partitionId.getId(), hostName, hostPort, operationType,
                  operationTime, lifeVersion, expirationTime);
          repairRequestsDb.putRepairRequests(record);
          records.putIfAbsent((int) partitionId.getId(), new HashMap<>());
          records.get((int) partitionId.getId()).put(record.getBlobId(), record);
        }
      }
    }

    // on one node with name as thisNodeName and port as thisNodePort,
    // read the database but exclude all the records which has the source replica as this node.
    // we should run ODR to fix the requests on the nodes except the source replica
    for (PartitionId id : partitionIds) {
      List<RepairRequestRecord> recordFromStore =
          repairRequestsDb.getRepairRequestsExcludingHost((int) id.getId(), thisNodeName, thisNodePort);
      Map<String, RepairRequestRecord> orgRecords = records.get((int) id.getId());
      for (RepairRequestRecord record : recordFromStore) {
        RepairRequestRecord org = orgRecords.get(record.getBlobId());
        assertEquals("Record does not match expectation ", org, record);
        assertTrue("should exclude this node",
            !thisNodeName.equals(record.getSourceHostName()) || thisNodePort != record.getSourceHostPort());
        orgRecords.remove(record.getBlobId());
        repairRequestsDb.removeRepairRequests(record.getBlobId(), record.getOperationType());
      }
    }

    // get the remaining records.
    for (PartitionId id : partitionIds) {
      List<RepairRequestRecord> recordFromStore = repairRequestsDb.getRepairRequestsForPartition((int) id.getId());
      Map<String, RepairRequestRecord> orgRecords = records.get((int) id.getId());
      assertEquals("Record number doesn't match.", orgRecords.size(), recordFromStore.size());
      for (RepairRequestRecord record : recordFromStore) {
        RepairRequestRecord org = orgRecords.get(record.getBlobId());
        assertEquals("Record does not match expectation ", org, record);
        assertEquals(thisNodeName, record.getSourceHostName());
        assertEquals(thisNodePort, record.getSourceHostPort());
        orgRecords.remove(record.getBlobId());
        repairRequestsDb.removeRepairRequests(record.getBlobId(), record.getOperationType());
      }
      assertTrue("Should be emptry now.", orgRecords.isEmpty());
      records.remove((int) id.getId());
    }
    assertTrue("Should have zero records now", records.isEmpty());

    // delete the records and check that they cannot be fetched with a get call.
    for (PartitionId id : partitionIds) {
      List<RepairRequestRecord> recordFromStore = repairRequestsDb.getRepairRequestsForPartition((int) id.getId());
      assertTrue("No more records", recordFromStore.isEmpty());
    }

    records.clear();
  }

  /**
   * Tests the different config of the db operation
   * @throws Exception
   */
  @Test
  public void testConfig() throws Exception {
    int list_max_result = 10;
    int total_result = 98;
    prepareDb(list_max_result);

    PartitionId partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    String sourceHostName = "localhost";
    int sourceHostPort = 6024;

    // Prepare RepairRequests and insert them to the DB.
    List<RepairRequestRecord> records = new ArrayList<>();
    for (int i = 0; i < total_result; i++) {
      String blobId =
          new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, (short) 101, (short) 102, partitionId,
              false, BlobId.BlobDataType.SIMPLE).getID();
      RepairRequestRecord.OperationType operationType = i % 2 == 0 ? TtlUpdateRequest : DeleteRequest;
      long operationTime = System.currentTimeMillis() - random.nextInt(1000);
      short lifeVersion = -1;
      long expirationTime = i % 2 == 0 ? Utils.Infinite_Time : System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
      RepairRequestRecord record =
          new RepairRequestRecord(blobId, (int) partitionId.getId(), sourceHostName, sourceHostPort, operationType,
              operationTime, lifeVersion, expirationTime);
      repairRequestsDb.putRepairRequests(record);
      records.add(record);
    }
    assertEquals("Record number is not expected", records.size(), total_result);

    // get records just inserted. Only get list_max_result numbers.
    List<RepairRequestRecord> recordFromStore =
        repairRequestsDb.getRepairRequestsForPartition((int) partitionId.getId());
    assertEquals("Record number is not expected", recordFromStore.size(), list_max_result);
    assertTrue("only read one page", total_result > list_max_result);
  }

  @Test
  public void testErrorInput() throws Exception {
    // LOCAL_CONSISTENCY_TODO
    // Error input: Should reject invalid parameters when insert to the database.
  }

  /**
   * Get a sample blob ID.
   * @param account the account of the blob.
   * @param container the container of the blob.
   * @return the base64 blob ID.
   */
  private String generateBlobId(Account account, Container container, PartitionId partitionId) {
    return new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, account.getId(), container.getId(),
        partitionId, false, BlobId.BlobDataType.SIMPLE).getID();
  }

  /**
   * Empty the table
   * @throws SQLException throw any SQL related exception
   */
  private void cleanup() throws SQLException {
    try {
      DataSource dataSource = repairRequestsDb.getDataSource();
      try (Connection connection = dataSource.getConnection()) {
        Statement statement = connection.createStatement();
        statement.executeUpdate("DELETE FROM " + MysqlRepairRequestsDb.REPAIR_REQUESTS_TABLE + ";");
      }
    } catch (SQLException e) {
      throw e;
    }
  }
}
