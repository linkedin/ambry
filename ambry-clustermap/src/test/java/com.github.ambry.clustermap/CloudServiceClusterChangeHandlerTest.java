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

package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test {@link CloudServiceClusterChangeHandler}.
 */
public class CloudServiceClusterChangeHandlerTest {
  private final long replicaCapacity = 27 * 1024 * 1024 * 1024L;
  private final ClusterMapConfig config = TestUtils.getDummyConfig();

  @Test
  public void testHandler() throws Exception {
    HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback =
        mock(HelixClusterManager.ClusterChangeHandlerCallback.class);
    CloudServiceClusterChangeHandler handler =
        new CloudServiceClusterChangeHandler("dc1", TestUtils.getDummyConfig(), clusterChangeHandlerCallback);

    assertNull("getDataNode should return null", handler.getDataNode("dc1"));
    List<AmbryDataNode> dataNodes = handler.getAllDataNodes();
    assertEquals("Only one node should be present", 1, handler.getAllDataNodes().size());
    CloudServiceDataNode cloudServiceDataNode = (CloudServiceDataNode) dataNodes.get(0);

    AmbryPartition p1 = new AmbryPartition(1, "class", mock(ClusterManagerCallback.class));
    AmbryPartition p2 = new AmbryPartition(2, "class", mock(ClusterManagerCallback.class));
    AmbryPartition p3 = new AmbryPartition(3, "class", mock(ClusterManagerCallback.class));
    handler.onReplicaAddedOrRemoved(makeReplicas(p1, p1, p1, p3, p2, p3), makeReplicas(p2));
    verify(clusterChangeHandlerCallback).addReplicasToPartition(eq(p1), any());
    verify(clusterChangeHandlerCallback).addReplicasToPartition(eq(p2), any());
    verify(clusterChangeHandlerCallback).addReplicasToPartition(eq(p3), any());
    verify(clusterChangeHandlerCallback, never()).removeReplicasFromPartition(any(), any());
    verifyNoMoreInteractions(clusterChangeHandlerCallback);

    reset(clusterChangeHandlerCallback);
    AmbryPartition p4 = new AmbryPartition(4, "class", mock(ClusterManagerCallback.class));
    ClusterMapChangeListener listener = mock(ClusterMapChangeListener.class);
    handler.registerClusterMapListener(listener);
    handler.registerClusterMapListener(handler);
    handler.onReplicaAddedOrRemoved(makeReplicas(p2, p4), makeReplicas(p4));
    verify(clusterChangeHandlerCallback).addReplicasToPartition(eq(p4), any());
    verify(clusterChangeHandlerCallback, never()).removeReplicasFromPartition(any(), any());
    verifyNoMoreInteractions(clusterChangeHandlerCallback);
    verify(listener).onReplicaAddedOrRemoved(
        Collections.singletonList(handler.getReplicaId(cloudServiceDataNode, p4.toPathString())),
        Collections.emptyList());

    // test basic datacenter view methods
    assertEquals(Collections.singletonList(handler.getReplicaId(cloudServiceDataNode, p3.toPathString())),
        handler.getReplicaIdsByState(p3, ReplicaState.LEADER).collect(Collectors.toList()));
    AmbryPartition p5 = new AmbryPartition(5, "class", mock(ClusterManagerCallback.class));
    assertEquals(Collections.emptyList(),
        handler.getReplicaIdsByState(p5, ReplicaState.LEADER).collect(Collectors.toList()));
    assertEquals(Collections.emptyMap(), handler.getDataNodeToDisksMap());
    assertEquals(4, handler.getReplicaIds(cloudServiceDataNode).size());
    assertEquals(Collections.emptyList(), handler.getReplicaIds(mock(AmbryDataNode.class)));
    assertEquals(Collections.emptySet(), handler.getDisks(cloudServiceDataNode));
    assertEquals(Collections.emptyMap(), handler.getPartitionToResourceMap());
    assertEquals(0, handler.getErrorCount());

    // simulate failure
    doThrow(new RuntimeException()).when(clusterChangeHandlerCallback).addReplicasToPartition(any(), any());
    handler.onReplicaAddedOrRemoved(makeReplicas(p5), makeReplicas());
    assertEquals(1, handler.getErrorCount());
  }

  private List<ReplicaId> makeReplicas(AmbryPartition... partitionIds) throws Exception {
    List<ReplicaId> replicaIds = new ArrayList<>(partitionIds.length);
    for (AmbryPartition partitionId : partitionIds) {
      replicaIds.add(new AmbryServerReplica(config, partitionId, mock(AmbryDisk.class), false, replicaCapacity, false));
    }
    return replicaIds;
  }
}
