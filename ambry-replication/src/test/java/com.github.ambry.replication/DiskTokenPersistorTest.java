/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreFindTokenFactory;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.*;


/**
 * Test for {@link DiskTokenPersistor}.
 */
public class DiskTokenPersistorTest {
  private static Map<String, Set<PartitionInfo>> mountPathToPartitionInfoList;
  private static ClusterMap clusterMap;
  private static ReplicaId replicaId;
  private static List<RemoteReplicaInfo.ReplicaTokenInfo> replicaTokenInfos;
  private static FindTokenHelper findTokenHelper;
  private static StorageManager mockStorageManager;
  private static String REPLICA_TOKEN_FILENAME = "replicaTokens";

  /**
   * Create the one time setup for the tests.
   * @throws Exception if Exception happens during setup.
   */
  @BeforeClass
  public static void setup() throws Exception {
    clusterMap = new MockClusterMap();
    mountPathToPartitionInfoList = new HashMap<>();
    BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
    StoreFindTokenFactory factory = new StoreFindTokenFactory(blobIdFactory);
    PartitionId partitionId = clusterMap.getAllPartitionIds(null).get(0);

    replicaId = partitionId.getReplicaIds().get(0);
    List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicas = new ArrayList<>();
    replicaTokenInfos = new ArrayList<>();
    for (ReplicaId remoteReplica : peerReplicas) {
      RemoteReplicaInfo remoteReplicaInfo =
          new RemoteReplicaInfo(remoteReplica, replicaId, null, factory.getNewFindToken(), 10, SystemTime.getInstance(),
              remoteReplica.getDataNodeId().getPortToConnectTo());
      remoteReplicas.add(remoteReplicaInfo);
      replicaTokenInfos.add(new RemoteReplicaInfo.ReplicaTokenInfo(remoteReplicaInfo));
    }
    PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partitionId, null, replicaId);
    mountPathToPartitionInfoList.computeIfAbsent(replicaId.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);

    Properties replicationProperties = new Properties();
    replicationProperties.setProperty("replication.cloud.token.factory", MockFindTokenFactory.class.getName());
    ReplicationConfig replicationConfig = new ReplicationConfig(new VerifiableProperties(replicationProperties));
    findTokenHelper = new FindTokenHelper(blobIdFactory, replicationConfig);
    mockStorageManager = Mockito.mock(StorageManager.class);
    Mockito.when(mockStorageManager.isDiskAvailableAtMountPath(anyString())).thenReturn(true);
  }

  /**
   * Basic test to persist and retrieve disk tokens.
   * @throws Exception if an Exception happens.
   */
  @Test
  public void basicTest() throws Exception {
    DiskTokenPersistor diskTokenPersistor = new DiskTokenPersistor(REPLICA_TOKEN_FILENAME, mountPathToPartitionInfoList,
        new ReplicationMetrics(new MetricRegistry(), Collections.emptyList()), clusterMap, findTokenHelper,
        mockStorageManager);

    //Simple persist and retrieve should pass
    diskTokenPersistor.persist(replicaId.getMountPath(), replicaTokenInfos);
    List<RemoteReplicaInfo.ReplicaTokenInfo> retrievedReplicaTokenInfos =
        diskTokenPersistor.retrieve(replicaId.getMountPath());

    Assert.assertEquals("Number of tokens doesn't match.", replicaTokenInfos.size(), retrievedReplicaTokenInfos.size());
    for (int i = 0; i < replicaTokenInfos.size(); i++) {
      Assert.assertArrayEquals("Token is not correct.", replicaTokenInfos.get(i).getReplicaToken().toBytes(),
          retrievedReplicaTokenInfos.get(i).getReplicaToken().toBytes());
    }
  }

  /**
   * Persist a replica with ReplicaTokenSerde version 0 and retrieve with current version (VERSION_1)
   * This tests cases replica token file is previously persisted in older version (VERSION_0) and new code is deployed that bumps the version to VERSION_1
   * @throws Exception if an exception happens
   */
  @Test
  public void testForVersion0AndCurrentVersionRetrieve() throws Exception {
    DiskTokenPersistor diskTokenPersistor = new DiskTokenPersistor(REPLICA_TOKEN_FILENAME, mountPathToPartitionInfoList,
        new ReplicationMetrics(new MetricRegistry(), Collections.emptyList()), clusterMap, findTokenHelper,
        mockStorageManager);

    persistVersion0(replicaId.getMountPath(), replicaTokenInfos);
    List<RemoteReplicaInfo.ReplicaTokenInfo> retrievedReplicaTokenInfos =
        diskTokenPersistor.retrieve(replicaId.getMountPath());
    Assert.assertEquals("Number of tokens doesn't match.", replicaTokenInfos.size(), retrievedReplicaTokenInfos.size());
    for (int i = 0; i < replicaTokenInfos.size(); i++) {
      Assert.assertArrayEquals("Token is not correct.", replicaTokenInfos.get(i).getReplicaToken().toBytes(),
          retrievedReplicaTokenInfos.get(i).getReplicaToken().toBytes());
    }
  }

  /**
   * Persist token in VERSION_0 format.
   * @param mountPath Path where persisted tokens will be saved.
   * @param tokenInfoList {@link RemoteReplicaInfo.ReplicaTokenInfo} list to serialize.
   * @throws IOException if an exception happens while persisting.
   */
  private void persistVersion0(String mountPath, List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfoList)
      throws IOException {
    File temp = new File(mountPath, REPLICA_TOKEN_FILENAME + ".tmp");
    File actual = new File(mountPath, REPLICA_TOKEN_FILENAME);
    try (FileOutputStream fileStream = new FileOutputStream(temp)) {
      serializeVersion0Tokens(tokenInfoList, fileStream);

      // swap temp file with the original file
      temp.renameTo(actual);
    }
  }

  /**
   * Serialize token in VERSION_0 format.
   * @param tokenInfoList {@link RemoteReplicaInfo.ReplicaTokenInfo} list to serialize.
   * @param outputStream {@link FileOutputStream} to persist the tokens to.
   * @throws IOException if an exception happens during serialization.
   */
  private void serializeVersion0Tokens(List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfoList,
      FileOutputStream outputStream) throws IOException {
    CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
    DataOutputStream writer = new DataOutputStream(crcOutputStream);
    try {
      // write the current version
      writer.writeShort(0);
      for (RemoteReplicaInfo.ReplicaTokenInfo replicaTokenInfo : tokenInfoList) {
        writer.write(replicaTokenInfo.getPartitionId().getBytes());
        // Write hostname
        writer.writeInt(replicaTokenInfo.getHostname().getBytes().length);
        writer.write(replicaTokenInfo.getHostname().getBytes());
        // Write replica path
        writer.writeInt(replicaTokenInfo.getReplicaPath().getBytes().length);
        writer.write(replicaTokenInfo.getReplicaPath().getBytes());
        // Write port
        writer.writeInt(replicaTokenInfo.getPort());
        //Write total bytes read from local store
        writer.writeLong(replicaTokenInfo.getTotalBytesReadFromLocalStore());
        // Write replica token
        writer.write(replicaTokenInfo.getReplicaToken().toBytes());
      }
      long crcValue = crcOutputStream.getValue();
      writer.writeLong(crcValue);
    } finally {
      if (outputStream != null) {
        // flush and overwrite file
        outputStream.getChannel().force(true);
      }
    }
  }
}
