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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.StoreFindTokenFactory;
import com.github.ambry.utils.SystemTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test of the CloudTokenPersistor.
 */
public class CloudTokenPersistorTest {

  @Test
  public void basicTest() throws Exception {
    Properties props = VcrTestUtil.createVcrProperties("DC1", "vcrClusterName", "zkConnectString", 12310, 12410, null);
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    ClusterMap clusterMap = new MockClusterMap();
    DataNodeId dataNodeId = new CloudDataNode(cloudConfig, clusterMapConfig);
    Map<String, List<PartitionInfo>> mountPathToPartitionInfoList = new HashMap<>();
    StoreFindTokenFactory factory = new StoreFindTokenFactory(new BlobIdFactory(clusterMap));
    PartitionId partitionId = clusterMap.getAllPartitionIds(null).get(0);

    ReplicaId cloudReplicaId = new CloudReplica(cloudConfig, partitionId, dataNodeId);

    List<? extends ReplicaId> peerReplicas = cloudReplicaId.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
    List<RemoteReplicaInfo.ReplicaTokenInfo> replicaTokenInfos = new ArrayList<>();
    for (ReplicaId remoteReplica : peerReplicas) {
      RemoteReplicaInfo remoteReplicaInfo =
          new RemoteReplicaInfo(remoteReplica, cloudReplicaId, null, factory.getNewFindToken(), 10,
              SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());
      remoteReplicas.add(remoteReplicaInfo);
      replicaTokenInfos.add(new RemoteReplicaInfo.ReplicaTokenInfo(remoteReplicaInfo));
    }
    PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partitionId, null, cloudReplicaId);
    mountPathToPartitionInfoList.computeIfAbsent(cloudReplicaId.getMountPath(), key -> new ArrayList<>())
        .add(partitionInfo);

    LatchBasedInMemoryCloudDestination cloudDestination =
        new LatchBasedInMemoryCloudDestination(Collections.emptyList());
    CloudTokenPersistor cloudTokenPersistor = new CloudTokenPersistor("replicaTokens", mountPathToPartitionInfoList,
        new ReplicationMetrics(new MetricRegistry(), Collections.emptyList()), clusterMap, factory, cloudDestination);
    cloudTokenPersistor.persist(cloudReplicaId.getMountPath(), replicaTokenInfos);
    List<RemoteReplicaInfo.ReplicaTokenInfo> retrievedReplicaTokenInfos =
        cloudTokenPersistor.retrieve(cloudReplicaId.getMountPath());

    Assert.assertEquals("Number of tokens doesn't match.", replicaTokenInfos.size(),
        retrievedReplicaTokenInfos.size());
    for (int i = 0; i < replicaTokenInfos.size(); i++) {
      Assert.assertArrayEquals("Token is not correct.", replicaTokenInfos.get(i).getReplicaToken().toBytes(),
          retrievedReplicaTokenInfos.get(i).getReplicaToken().toBytes());
    }
  }
}
