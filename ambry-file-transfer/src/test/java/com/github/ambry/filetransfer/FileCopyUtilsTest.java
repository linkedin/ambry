/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.filetransfer.utils.FileCopyUtils;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


public class FileCopyUtilsTest {

  /**
   * Test for {@link FileCopyUtils#getPeerForFileCopy(PartitionId, String)}
   * Test if no LEADER replica is found in the given datacenter.
   */
  @Test
  public void testGetPeerForFileCopyWithLeaderNotFound() {
    MockPartitionId partitionId = new MockPartitionId(123456, "RANDOM_PARTITION_CLASS");
    ReplicaId replicaId = FileCopyUtils.getPeerForFileCopy(partitionId, "datacenter");
    assertNull(replicaId);
  }

  /**
   * Test for {@link FileCopyUtils#getPeerForFileCopy(PartitionId, String)}
   * Test if a LEADER replica is found in the given datacenter and it is healthy
   */
  @Test
  public void testGetPeerForFileCopyWithLeaderFoundAndAvailable() {
    List<String> mountPaths = new ArrayList<>();
    mountPaths.add("/tmp/1");
    mountPaths.add("/tmp/2");
    Port port = new Port(1, PortType.PLAINTEXT);

    MockDataNodeId dataNodeId =
        new MockDataNodeId("peerNode", Collections.singletonList(port), mountPaths, "peerDatacenter");

    PartitionId partitionId =
        new MockPartitionId(100, MockClusterMap.DEFAULT_PARTITION_CLASS, Collections.singletonList(dataNodeId), 0);

    ReplicaId replicaId = FileCopyUtils.getPeerForFileCopy(partitionId, "peerDatacenter");
    assertNotNull(replicaId);
    PartitionId leaderPartition =
        replicaId.getPartitionId().getReplicaIdsByState(ReplicaState.LEADER, "peerDatacenter").get(0).getPartitionId();
    assertEquals(partitionId, leaderPartition);
    assertFalse(replicaId.isDown());
  }

  /**
   * Test for {@link FileCopyUtils#getPeerForFileCopy(PartitionId, String)}
   * Test if a LEADER replica is found in the given datacenter and it is not healthy
   * we do not get any replica
   */
  @Test
  public void testGetPeerForFileCopyWithLeaderFoundAndUnAvailable() {
    List<String> mountPaths = new ArrayList<>();
    mountPaths.add("/tmp/1");
    mountPaths.add("/tmp/2");
    Port port = new Port(1, PortType.PLAINTEXT);

    MockDataNodeId dataNodeId =
        new MockDataNodeId("peerNode", Collections.singletonList(port), mountPaths, "peerDatacenter");

    PartitionId partitionId =
        new MockPartitionId(100, MockClusterMap.DEFAULT_PARTITION_CLASS, Collections.singletonList(dataNodeId), 0);
    // Get Healthy Leader replica first
    ReplicaId replicaId = FileCopyUtils.getPeerForFileCopy(partitionId, "peerDatacenter");
    assertNotNull(replicaId);
    // Mark replica as unhealthy
    replicaId.markDiskDown();

    // we should not get any replica now as leader is unhealthy
    replicaId = FileCopyUtils.getPeerForFileCopy(partitionId, "peerDatacenter");
    assertNull(replicaId);
  }
}
