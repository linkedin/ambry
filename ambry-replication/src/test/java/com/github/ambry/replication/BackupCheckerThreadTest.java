/**
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
 */
package com.github.ambry.replication;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.store.MessageInfoType;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@Ignore
@RunWith(Parameterized.class)
public class BackupCheckerThreadTest extends ReplicationTestHelper {

  @Parameterized.Parameters
  public static List<Object[]> data() {
    //@formatter:off
    return Arrays.asList(new Object[][]{
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5, true},
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5, false},
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6, true},
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6, false},
    });
    //@formatter:on
  }

  public BackupCheckerThreadTest(short requestVersion, short responseVersion, boolean enableContinuousReplication) {
    super(requestVersion, responseVersion, enableContinuousReplication);
  }

  @Test
  public void testReplication() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    List<MockHost> allHosts = Arrays.asList(localHost, remoteHost);

    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 100;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(true, batchSize, clusterMap, localHost, storeKeyConverter, transformer, null,
            null, remoteHost);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    BackupCheckerThread checkerThread = (BackupCheckerThread) replicasAndThread.getSecond();
    MockBackupCheckerFileManager fileManager = (MockBackupCheckerFileManager) checkerThread.getFileManager();
    MockNetworkClient mockNetworkClient = (MockNetworkClient) checkerThread.getNetworkClient();

    // Test case 1. all the records are on both local and remote host
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    int expectedIndex = 0;
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      // Adding 1 store key that ends with PUT
      //        1 store key that ends with ttl_update
      //        1 store key that ends with ttl_update and delete
      //        1 store key that ends with delete
      List<StoreKey> keys = addPutMessagesToReplicasOfPartition(partitionId, allHosts, 4);
      // ttl update
      StoreKey key = keys.get(1);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, key, allHosts, Utils.Infinite_Time);

      key = keys.get(2);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, key, allHosts, Utils.Infinite_Time);
      addDeleteMessagesToReplicasOfPartition(partitionId, key, allHosts);

      key = keys.get(3);
      addDeleteMessagesToReplicasOfPartition(partitionId, key, allHosts);
    }
    expectedIndex += 8;
    checkerThread.replicate();

    // There should not be any missing records in the local host
    // There should not be any GetRequest sent out
    Assert.assertEquals(0, fileManager.appendRecordsForPartitions.size());
    Assert.assertEquals(0, mockNetworkClient.numGetRequest.get());
    List<ReplicaThread.ExchangeMetadataResponse> responses =
        checkerThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    for (ReplicaThread.ExchangeMetadataResponse response : responses) {
      Assert.assertEquals(expectedIndex, ((MockFindToken) response.remoteToken).getIndex());
    }

    // Test case 2. Missing deletes in the local host
    Map<PartitionId, Set<StoreKey>> expectedDeletedKeysByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      // Adding 1 store key that ends with PUT to local, but PUT and delete to remote
      List<StoreKey> keys = addPutMessagesToReplicasOfPartition(partitionId, allHosts, 1);
      StoreKey key = keys.get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, key, Collections.singletonList(remoteHost));
      expectedDeletedKeysByPartition.put(partitionId, new HashSet<>(keys));
    }
    expectedIndex += 2;
    fileManager.appendRecordsForPartitions.clear();

    checkerThread.replicate();
    Assert.assertEquals(expectedDeletedKeysByPartition.size(), fileManager.appendRecordsForPartitions.size());
    for (Map.Entry<PartitionId, Set<StoreKey>> expectedDeletedKeys : expectedDeletedKeysByPartition.entrySet()) {
      Map<StoreKey, BackupCheckerAppendRecord> records =
          fileManager.appendRecordsForPartitions.get(expectedDeletedKeys.getKey());
      Assert.assertNotNull(records);
      Set<StoreKey> keys = expectedDeletedKeys.getValue();
      Assert.assertEquals(keys.size(), records.size());
      for (StoreKey key : keys) {
        BackupCheckerAppendRecord record = records.get(key);
        Assert.assertNotNull(record);
        Assert.assertEquals(
            "/tmp/" + expectedDeletedKeys.getKey().getId() + "/" + record.remoteReplicaInfo.getReplicaId()
                .getDataNodeId()
                .getHostname() + "/missingKeys", record.filePath);
        Assert.assertEquals(EnumSet.of(MessageInfoType.DELETE), record.acceptableLocalBlobStates);
        Assert.assertEquals(EnumSet.of(MessageInfoType.PUT, MessageInfoType.DELETE), record.remoteBlobState);
        Assert.assertEquals(EnumSet.of(MessageInfoType.PUT), record.localBlobState);
      }
    }
    Assert.assertEquals(0, mockNetworkClient.numGetRequest.get());
    responses = checkerThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    for (ReplicaThread.ExchangeMetadataResponse response : responses) {
      Assert.assertEquals(expectedIndex, ((MockFindToken) response.remoteToken).getIndex());
    }

    // Test case 3. Missing ttl update in the local host
    Map<PartitionId, Set<StoreKey>> expectedTtlUpdateKeysByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      // Adding 1 store key that ends with PUT to local, but PUT and delete to remote
      List<StoreKey> keys = addPutMessagesToReplicasOfPartition(partitionId, allHosts, 1);
      StoreKey key = keys.get(0);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, key, Collections.singletonList(remoteHost),
          Utils.Infinite_Time);
      expectedTtlUpdateKeysByPartition.put(partitionId, new HashSet<>(keys));
    }
    expectedIndex += 2;
    fileManager.appendRecordsForPartitions.clear();

    checkerThread.replicate();
    Assert.assertEquals(expectedTtlUpdateKeysByPartition.size(), fileManager.appendRecordsForPartitions.size());
    for (Map.Entry<PartitionId, Set<StoreKey>> expectedTtlUpdateKeys : expectedTtlUpdateKeysByPartition.entrySet()) {
      Map<StoreKey, BackupCheckerAppendRecord> records =
          fileManager.appendRecordsForPartitions.get(expectedTtlUpdateKeys.getKey());
      Assert.assertNotNull(records);
      Set<StoreKey> keys = expectedTtlUpdateKeys.getValue();
      Assert.assertEquals(keys.size(), records.size());
      for (StoreKey key : keys) {
        BackupCheckerAppendRecord record = records.get(key);
        Assert.assertNotNull(record);
        Assert.assertEquals(
            "/tmp/" + expectedTtlUpdateKeys.getKey().getId() + "/" + record.remoteReplicaInfo.getReplicaId()
                .getDataNodeId()
                .getHostname() + "/missingKeys", record.filePath);
        Assert.assertEquals(EnumSet.of(MessageInfoType.TTL_UPDATE), record.acceptableLocalBlobStates);
        Assert.assertEquals(EnumSet.of(MessageInfoType.PUT, MessageInfoType.TTL_UPDATE), record.remoteBlobState);
        Assert.assertEquals(EnumSet.of(MessageInfoType.PUT), record.localBlobState);
      }
    }
    Assert.assertEquals(0, mockNetworkClient.numGetRequest.get());
    responses = checkerThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    for (ReplicaThread.ExchangeMetadataResponse response : responses) {
      Assert.assertEquals(expectedIndex, ((MockFindToken) response.remoteToken).getIndex());
    }

    // Test case 4. Missing Put in the local host
    Map<PartitionId, Set<StoreKey>> expectedPutKeysByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      // Adding 1 store key that ends with PUT to local, but PUT and delete to remote
      List<StoreKey> keys = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1);
      expectedPutKeysByPartition.put(partitionId, new HashSet<>(keys));
    }
    expectedIndex += 1;
    fileManager.appendRecordsForPartitions.clear();

    checkerThread.replicate();
    Assert.assertEquals(expectedPutKeysByPartition.size(), fileManager.appendRecordsForPartitions.size());
    for (Map.Entry<PartitionId, Set<StoreKey>> expectedPutKeys : expectedPutKeysByPartition.entrySet()) {
      Map<StoreKey, BackupCheckerAppendRecord> records =
          fileManager.appendRecordsForPartitions.get(expectedPutKeys.getKey());
      Assert.assertNotNull(records);
      Set<StoreKey> keys = expectedPutKeys.getValue();
      Assert.assertEquals(keys.size(), records.size());
      for (StoreKey key : keys) {
        BackupCheckerAppendRecord record = records.get(key);
        Assert.assertNotNull(record);
        Assert.assertEquals("/tmp/" + expectedPutKeys.getKey().getId() + "/" + record.remoteReplicaInfo.getReplicaId()
            .getDataNodeId()
            .getHostname() + "/missingKeys", record.filePath);
        Assert.assertEquals(EnumSet.of(MessageInfoType.PUT), record.acceptableLocalBlobStates);
        Assert.assertEquals(EnumSet.of(MessageInfoType.PUT), record.remoteBlobState);
        Assert.assertEquals(StoreErrorCodes.ID_Not_Found, record.storeErrorCodes);
      }
    }
    Assert.assertEquals(0, mockNetworkClient.numGetRequest.get());
    responses = checkerThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    for (ReplicaThread.ExchangeMetadataResponse response : responses) {
      Assert.assertEquals(expectedIndex, ((MockFindToken) response.remoteToken).getIndex());
    }
  }
}
