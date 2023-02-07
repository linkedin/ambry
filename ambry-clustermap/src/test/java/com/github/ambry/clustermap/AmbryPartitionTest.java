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
 */
package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests for {@link AmbryPartition}.
 */
public class AmbryPartitionTest {
  private final TestClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode>
      clusterManagerQueryHelper = new TestClusterManagerQueryHelper<>();
  private final ClusterMapConfig clusterMapConfig;

  /**
   * Constructor for {@link AmbryPartitionTest}.
   */
  public AmbryPartitionTest() {
    Properties properties = new Properties();
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.port", "0");
    properties.setProperty("clustermap.cluster.name", "clusterName");
    properties.setProperty("clustermap.datacenter.name", "dcName");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
  }

  /**
   * Test for {@link AmbryPartition#resolvePartitionState()}.
   */
  @Test
  public void testResolvePartitionState() throws Exception {
    testResolvePartitionState(true);
    testResolvePartitionState(false);
  }

  private void testResolvePartitionState(boolean incrementSealState) throws Exception {
    AmbryPartition ambryPartition = new AmbryPartition(1, "test", clusterManagerQueryHelper);
    int totalReplicas = 9;
    for (int numSealedReplicas = 0; numSealedReplicas <= 9; numSealedReplicas++) {
      for (int numPartiallySealedReplicas = 0; numPartiallySealedReplicas <= (totalReplicas - numSealedReplicas);
          numPartiallySealedReplicas++) {
        int numUnSealedReplicas = totalReplicas - numSealedReplicas - numPartiallySealedReplicas;
        clusterManagerQueryHelper.setReplicaIds(
            generateReplicaIdListWithSealedStatus(numSealedReplicas, numPartiallySealedReplicas, numUnSealedReplicas,
                ambryPartition));
        if (incrementSealState) {
          clusterManagerQueryHelper.incrementSealedStateCounter();
        }
        ambryPartition.resolvePartitionState();
        PartitionState expectedPartitionState = PartitionState.READ_WRITE;
        if (numSealedReplicas > 0) {
          expectedPartitionState = PartitionState.READ_ONLY;
        } else if (numPartiallySealedReplicas > 0) {
          expectedPartitionState = PartitionState.PARTIAL_READ_WRITE;
        }
        Assert.assertEquals(String.format(
                "Unexpected partition state numSealedReplicas: %s, numPartiallySealedReplicas: %s, numUnsealedReplicas: %s",
                numSealedReplicas, numPartiallySealedReplicas, numUnSealedReplicas),
            incrementSealState ? expectedPartitionState : PartitionState.READ_WRITE,
            ambryPartition.getPartitionState());
      }
    }
  }

  /**
   * Generate a {@link List} of {@link AmbryReplica}s with the specified count of replica states.
   *
   * @param numSealedReplicas          number of sealed replicas in the list.
   * @param numPartiallySealedReplicas number of partially sealed replicas in the list.
   * @param numUnSealedReplicas        number of unsealed replicas in the list.
   * @param ambryPartition             {@link AmbryPartition} object.
   * @return List of {@link AmbryReplica} objects.
   * @throws Exception in case of any error.
   */
  private List<AmbryReplica> generateReplicaIdListWithSealedStatus(int numSealedReplicas,
      int numPartiallySealedReplicas, int numUnSealedReplicas, AmbryPartition ambryPartition) throws Exception {
    List<AmbryReplica> replicaIds = new ArrayList<>();
    while (numSealedReplicas > 0) {
      replicaIds.add(new TestAmbryReplica(ReplicaSealStatus.SEALED, ambryPartition));
      numSealedReplicas--;
    }
    while (numPartiallySealedReplicas > 0) {
      replicaIds.add(new TestAmbryReplica(ReplicaSealStatus.PARTIALLY_SEALED, ambryPartition));
      numPartiallySealedReplicas--;
    }
    while (numUnSealedReplicas > 0) {
      replicaIds.add(new TestAmbryReplica(ReplicaSealStatus.NOT_SEALED, ambryPartition));
      numUnSealedReplicas--;
    }
    return replicaIds;
  }

  /**
   * Test implementation of {@link ClusterManagerQueryHelper} class to return specified {@link ReplicaId}s.
   */
  static class TestClusterManagerQueryHelper<R extends ReplicaId, D extends DiskId, P extends PartitionId, N extends DataNodeId>
      implements ClusterManagerQueryHelper<R, D, P, N> {
    private int sealedStateCounter = 0;
    private List<R> replicaIds;

    @Override
    public List<R> getReplicaIdsForPartition(P partition) {
      return replicaIds;
    }

    @Override
    public List<String> getResourceNamesForPartition(P partition) {
      return null;
    }

    @Override
    public List<R> getReplicaIdsByState(P partition, ReplicaState state, String dcName) {
      return null;
    }

    @Override
    public void getReplicaIdsByStates(Map<ReplicaState, List<R>> replicasByState, P partition, Set<ReplicaState> states,
        String dcName) {
    }

    @Override
    public long getSealedStateChangeCounter() {
      return sealedStateCounter;
    }

    @Override
    public Collection<D> getDisks(N dataNode) {
      return null;
    }

    @Override
    public Collection<P> getPartitions() {
      return null;
    }

    /**
     * Increment the sealed state counter by one.
     */
    public void incrementSealedStateCounter() {
      sealedStateCounter++;
    }

    /**
     * Set the replica ids to the specified list.
     *
     * @param replicaIds {@link List} of {@link AmbryReplica} objects.
     */
    public void setReplicaIds(List<R> replicaIds) {
      this.replicaIds = replicaIds;
    }
  }

  /**
   * Test implementation of {@link AmbryReplica} class to return specific {@link ReplicaSealStatus}.
   */
  public class TestAmbryReplica extends AmbryReplica {
    AmbryDataNode ambryDataNode = Mockito.mock(AmbryDataNode.class);

    /**
     * Constructor for {@link TestAmbryReplica}.
     *
     * @param replicaSealStatus {@link ReplicaSealStatus} of this replica.
     * @param ambryPartition    {@link AmbryPartition} object.
     * @throws Exception in case of any error.
     */
    public TestAmbryReplica(ReplicaSealStatus replicaSealStatus, AmbryPartition ambryPartition) throws Exception {
      super(clusterMapConfig, ambryPartition, false, ClusterMapUtils.MIN_REPLICA_CAPACITY_IN_BYTES + 1,
          replicaSealStatus);
      Mockito.when(ambryDataNode.getHostname()).thenReturn("localhost");
      Mockito.when(ambryDataNode.getPort()).thenReturn(0);
    }

    @Override
    public AmbryDataNode getDataNodeId() {
      return ambryDataNode;
    }

    @Override
    public String getMountPath() {
      return null;
    }

    @Override
    public String getReplicaPath() {
      return null;
    }

    @Override
    public AmbryDisk getDiskId() {
      return null;
    }

    @Override
    public void markDiskDown() {

    }

    @Override
    public void markDiskUp() {

    }

    @Override
    public ReplicaType getReplicaType() {
      return null;
    }

    @Override
    public JSONObject getSnapshot() {
      return null;
    }
  }
}
