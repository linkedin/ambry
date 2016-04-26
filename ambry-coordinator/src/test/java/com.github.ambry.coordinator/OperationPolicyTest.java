/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.coordinator;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Tests operation policy implementations.
 */
public class OperationPolicyTest {
  final String datacenterAlpha = "alpha";
  final String datacenterBeta = "beta";
  final String datacenterGamma = "gamma";
  final String[] dataCenters = new String[]{datacenterAlpha, datacenterBeta, datacenterGamma};
  final int REPLICAS_PER_DATACENTER = 3;
  final ArrayList<String> sslEnabledDataCenters = new ArrayList<String>();

  /**
   * SerialOperationPolicy is used as the Policy for GetOperation
   * @throws CoordinatorException
   */
  @Test
  public void testSerialOperationPolicy()
      throws Exception {
    OperationContext oc = new OperationContext("client1", 1000, true, null, null, sslEnabledDataCenters);
    // Simple success test
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failures but still succeed test
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      for (int i = 0; i < 5; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
      }
      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      assertTrue(op.mayComplete());
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced (local replicas then remote); ensure sendMore is correct
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
        replicasInFlight.add(replicaId);
        assertFalse(op.sendMoreRequests(replicasInFlight));
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
        replicasInFlight.clear();
        assertTrue(op.sendMoreRequests(replicasInFlight));
      }
      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
        replicasInFlight.add(replicaId);
        assertFalse(op.sendMoreRequests(replicasInFlight));
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
        replicasInFlight.clear();
        if (i != 2) {
          assertTrue(op.sendMoreRequests(replicasInFlight));
        } else {
          assertFalse(op.sendMoreRequests(replicasInFlight));
        }
      }
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    oc = new OperationContext("client1", 1000, false, null, null, sslEnabledDataCenters);

    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 3);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
        replicasInFlight.add(replicaId);
        assertFalse(op.sendMoreRequests(replicasInFlight));
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
        replicasInFlight.clear();
        boolean sendMoreRequests = op.sendMoreRequests(replicasInFlight);
        if (i < 2) {
          Assert.assertTrue(sendMoreRequests);
        } else {
          Assert.assertFalse(sendMoreRequests);
        }
      }

      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    oc = new OperationContext("client1", 1000, true, null, null, sslEnabledDataCenters);
    // Corruption test
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertTrue(op.isCorrupt());
    }
  }

  @Test
  public void testGetTwoInParallelOperationPolicy()
      throws CoordinatorException {
    // Simple success test
    {
      OperationPolicy op =
          new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // One failure but still succeeds
    {
      OperationPolicy op =
          new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());
      assertFalse(op.sendMoreRequests(replicasInFlight));

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(replicasInFlight));

      op.onSuccessfulResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());
    }

    // Many failures but still succeeds
    {
      OperationPolicy op =
          new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      for (int i = 0; i < 5; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
      }
      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      assertTrue(op.mayComplete());
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced (local replicas then remote); ensure sendMore is correct (2
    // plus 1 for good luck in flight)
    {
      OperationPolicy op =
          new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId3);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId4);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId3);
      replicasInFlight.remove(replicaId3);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertEquals(replicaId5.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId4);
      replicasInFlight.remove(replicaId4);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId5);
      replicasInFlight.remove(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());

      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op =
          new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), false);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 3);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());

      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Corruption test
    {
      OperationPolicy op =
          new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      for (int i = 0; i < 3; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertTrue(op.isCorrupt());
    }
  }

  @Test
  public void testGetCrossColoParallelOperationPolicy()
      throws Exception {

    OperationContext oc =
        new OperationContext("client1", 1000, true, new CoordinatorMetrics(new MockClusterMap(), true), null,
            sslEnabledDataCenters);
    // Simple success test
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // One failure but still succeeds
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      List<ReplicaId> replicasInFlight = new ArrayList<ReplicaId>();

      for (int i = 0; i < 3; i++) {
        assertTrue(op.sendMoreRequests(replicasInFlight));
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        replicasInFlight.add(replicaId);
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
      }

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      op.onFailedResponse(replicaId1);

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      assertTrue(op.mayComplete());
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Many failures but still succeeds
    // A different version of above testcase, where in first remote replica succeeds
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      // send more should return true for remote now
      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      assertTrue(op.mayComplete());

      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      op.onSuccessfulResponse(replicaId3);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Many failures but still succeeds
    // A different version of above testcase, where in last remote replica succeeds
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());
      assertFalse(op.sendMoreRequests(replicasInFlight));

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      // send more should return true for remote now
      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId4);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
      op.onFailedResponse(replicaId3);
      replicasInFlight.remove(replicaId3);
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId4);
      replicasInFlight.remove(replicaId4);

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertEquals(replicaId5.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId5);
      assertTrue(op.mayComplete());
      assertFalse(op.sendMoreRequests(replicasInFlight));

      op.onSuccessfulResponse(replicaId5);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced (local replicas then remote);
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId4);

      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId3);
      replicasInFlight.remove(replicaId3);
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId4);
      replicasInFlight.remove(replicaId4);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertEquals(replicaId5.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId5);
      replicasInFlight.remove(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());

      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Corruption test
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      ArrayList<ReplicaId> replicasInFlight = new ArrayList<ReplicaId>();

      for (int i = 0; i < 3; i++) {
        assertTrue(op.sendMoreRequests(replicasInFlight));
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
        replicasInFlight.add(replicaId);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      for (int i = 0; i < 3; i++) {
        assertTrue(op.sendMoreRequests(replicasInFlight));
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
        replicasInFlight.add(replicaId);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertTrue(op.isCorrupt());
    }

    // 3 datacenters
    // Simple success test
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(9), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 9);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // One failure but still succeeds
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(9), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 9);
      assertTrue(op.mayComplete());
      List<ReplicaId> replicasInFlight = new ArrayList<ReplicaId>();

      for (int i = 0; i < 3; i++) {
        assertTrue(op.sendMoreRequests(replicasInFlight));
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        replicasInFlight.add(replicaId);
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
      }

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      op.onFailedResponse(replicaId1);

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertTrue((replicaId.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      assertTrue(op.mayComplete());
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Many failures but still succeeds
    // A different version of above testcase, where in first remote replica succeeds
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(9), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 9);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      // send more should return true for remote now
      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertTrue((replicaId3.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId3.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      assertTrue(op.mayComplete());

      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      op.onSuccessfulResponse(replicaId3);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Many failures but still succeeds
    // A different version of above testcase, where in last remote replica succeeds
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(9), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 9);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());
      assertFalse(op.sendMoreRequests(replicasInFlight));

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      // send more should return true for remote now

      Map<String, List<ReplicaId>> dataCentertoReplicaListMap = new HashMap<String, List<ReplicaId>>();

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertTrue((replicaId3.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId3.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId3);
      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertTrue((replicaId4.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId4.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId4);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId4);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertTrue((replicaId5.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId5.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId5);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId5);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId6 = op.getNextReplicaIdForSend();
      assertTrue((replicaId6.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId6.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId6);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId6);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));

      for (List<ReplicaId> replicaIdList : dataCentertoReplicaListMap.values()) {
        ReplicaId replicaIdToFail = replicaIdList.remove(0);
        op.onFailedResponse(replicaIdToFail);
        replicasInFlight.remove(replicaIdToFail);
        assertTrue(op.mayComplete());
      }

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId7 = op.getNextReplicaIdForSend();
      assertTrue((replicaId7.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId7.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId7);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId7);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId8 = op.getNextReplicaIdForSend();
      assertTrue((replicaId8.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId8.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId8);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId8);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
      for (List<ReplicaId> replicaIdList : dataCentertoReplicaListMap.values()) {
        ReplicaId replicaIdToFail = replicaIdList.remove(0);
        op.onFailedResponse(replicaIdToFail);
        replicasInFlight.remove(replicaIdToFail);
        assertTrue(op.mayComplete());
      }

      op.onSuccessfulResponse(replicaId8);
      assertTrue(op.mayComplete());
      assertTrue(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced (local replicas then remote);
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(9), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 9);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());
      assertFalse(op.sendMoreRequests(replicasInFlight));

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      // send more should return true for remote now

      Map<String, List<ReplicaId>> dataCentertoReplicaListMap = new HashMap<String, List<ReplicaId>>();

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertTrue((replicaId3.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId3.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId3);
      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertTrue((replicaId4.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId4.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId4);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId4);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertTrue((replicaId5.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId5.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId5);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId5);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId6 = op.getNextReplicaIdForSend();
      assertTrue((replicaId6.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId6.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId6);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId6);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));

      for (List<ReplicaId> replicaIdList : dataCentertoReplicaListMap.values()) {
        ReplicaId replicaIdToFail = replicaIdList.remove(0);
        op.onFailedResponse(replicaIdToFail);
        replicasInFlight.remove(replicaIdToFail);
        assertTrue(op.mayComplete());
      }

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId7 = op.getNextReplicaIdForSend();
      assertTrue((replicaId7.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId7.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId7);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId7);
      assertTrue(op.mayComplete());

      assertTrue(op.sendMoreRequests(replicasInFlight));
      ReplicaId replicaId8 = op.getNextReplicaIdForSend();
      assertTrue((replicaId8.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId8.getDataNodeId()
          .getDatacenterName().equals(datacenterGamma)));
      replicasInFlight.add(replicaId8);
      addReplicaToDataCenter(dataCentertoReplicaListMap, replicaId8);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));

      for (List<ReplicaId> replicaIdList : dataCentertoReplicaListMap.values()) {
        ReplicaId replicaIdToFail = replicaIdList.remove(0);
        op.onFailedResponse(replicaIdToFail);
        replicasInFlight.remove(replicaIdToFail);
        assertTrue(op.mayComplete());
      }

      op.onFailedResponse(replicaId7);
      replicasInFlight.remove(replicaId7);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());

      op.onFailedResponse(replicaId8);
      replicasInFlight.remove(replicaId8);
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Corruption test
    {
      OperationPolicy op =
          new GetCrossColoParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(9), 2, oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 9);
      assertTrue(op.mayComplete());
      ArrayList<ReplicaId> replicasInFlight = new ArrayList<ReplicaId>();

      for (int i = 0; i < 3; i++) {
        assertTrue(op.sendMoreRequests(replicasInFlight));
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
        replicasInFlight.add(replicaId);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      for (int i = 0; i < 6; i++) {
        assertTrue(op.sendMoreRequests(replicasInFlight));
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertTrue((replicaId.getDataNodeId().getDatacenterName().equals(datacenterBeta)) || (replicaId.getDataNodeId()
            .getDatacenterName().equals(datacenterGamma)));
        replicasInFlight.add(replicaId);
        assertTrue(op.mayComplete());
        op.onCorruptResponse(replicaId);
      }
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertTrue(op.isCorrupt());
    }
  }

  @Test
  public void testPutPolicy()
      throws CoordinatorException {
    OperationContext oc = new OperationContext("client1", 1000, true, null, null, sslEnabledDataCenters);
    // Simple success test. Requires 2 successes to complete
    {
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      op.onSuccessfulResponse(replicaId);
      assertFalse(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());

      replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());
    }

    // Failures but still succeed test
    {
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      for (int i = 0; i < 4; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
      }

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      op.onSuccessfulResponse(replicaId);
      assertFalse(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());

      replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced (local replicas then remote); ensure sendMore is correct (2
    // plus 1 for good luck in flight)
    {
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId3);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId4);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertEquals(replicaId5.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId3);
      replicasInFlight.remove(replicaId3);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId4);
      replicasInFlight.remove(replicaId4);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());

      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    oc = new OperationContext("client1", 1000, false, null, null, sslEnabledDataCenters);
    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 3);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());
      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }
  }

  @Test
  public void testDeletePolicy()
      throws CoordinatorException {
    OperationContext oc = new OperationContext("client1", 1000, true, null, null, sslEnabledDataCenters);
    // Simple success test
    {
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      op.onSuccessfulResponse(replicaId);
      assertFalse(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());

      replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterAlpha);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());
    }

    // Failures but still succeed test
    {
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      for (int i = 0; i < 4; i++) {
        ReplicaId replicaId = op.getNextReplicaIdForSend();
        assertTrue(op.mayComplete());
        op.onFailedResponse(replicaId);
      }

      ReplicaId replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      op.onSuccessfulResponse(replicaId);
      assertFalse(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());

      replicaId = op.getNextReplicaIdForSend();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), datacenterBeta);
      op.onSuccessfulResponse(replicaId);
      assertTrue(op.isComplete());
      assertTrue(op.mayComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced (local replicas then remote); ensure sendMore is correct
    {
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId3);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId4);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertEquals(replicaId5.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId3);
      replicasInFlight.remove(replicaId3);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId4);
      replicasInFlight.remove(replicaId4);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());

      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    oc = new OperationContext("client1", 1000, false, null, null, sslEnabledDataCenters);
    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), oc);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 3);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());
      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }
  }

  private void addReplicaToDataCenter(Map<String, List<ReplicaId>> dataCenterToReplicaListMap, ReplicaId replicaId) {
    String dataCenter = replicaId.getDataNodeId().getDatacenterName();
    if (!dataCenterToReplicaListMap.containsKey(dataCenter)) {
      List<ReplicaId> replicaIdList = new ArrayList<ReplicaId>();
      dataCenterToReplicaListMap.put(dataCenter, replicaIdList);
    }
    dataCenterToReplicaListMap.get(dataCenter).add(replicaId);
  }

  class OperationPolicyPartitionId extends PartitionId {
    List<ReplicaId> replicaIds;

    OperationPolicyPartitionId(int replicaCount) {
      this.replicaIds = new ArrayList<ReplicaId>(replicaCount);
      int dataCenterCount = replicaCount / REPLICAS_PER_DATACENTER;
      for (int i = 0; i < replicaCount; i++) {
        replicaIds.add(new OperationPolicyReplicaId(this, i, dataCenters[i % dataCenterCount]));
      }
    }

    @Override
    public byte[] getBytes() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public boolean isEqual(String partitionId) {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public List<ReplicaId> getReplicaIds() {
      return replicaIds;
    }

    @Override
    public PartitionState getPartitionState() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public int compareTo(PartitionId partitionId) {
      throw new IllegalStateException("Should not be invoked.");
    }
  }

  class OperationPolicyReplicaId implements ReplicaId {
    OperationPolicyPartitionId partitionId;
    int index;
    String datacenter;

    OperationPolicyReplicaId(OperationPolicyPartitionId partitionId, int index, String datacenter) {
      this.partitionId = partitionId;
      this.index = index;
      this.datacenter = datacenter;
    }

    @Override
    public PartitionId getPartitionId() {
      return partitionId;
    }

    @Override
    public DataNodeId getDataNodeId() {
      return new OperationPolicyDataNodeId(index, datacenter);
    }

    @Override
    public String getMountPath() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public String getReplicaPath() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public List<ReplicaId> getPeerReplicaIds() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public long getCapacityInBytes() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public DiskId getDiskId() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public boolean isDown() {
      return false;
    }

    @Override
    public String toString() {
      return datacenter + "-" + index;
    }
  }

  class OperationPolicyDataNodeId extends DataNodeId {
    int index;
    String datacenter;

    OperationPolicyDataNodeId(int index, String datacenter) {
      this.index = index;
      this.datacenter = datacenter;
    }

    @Override
    public String getHostname() {
      return datacenter + "-" + index;
    }

    @Override
    public int getPort() {
      return 0;
    }

    @Override
    public int getSSLPort() {
      throw new IllegalStateException("No SSL port exists for localhost");
    }

    @Override
    public boolean hasSSLPort() {
      return false;
    }

    @Override
    @Deprecated
    public Port getPortToConnectTo(ArrayList<String> sslEnabledDataCenters) {
      return new Port(0, PortType.PLAINTEXT);
    }

    @Override
    public Port getPortToConnectTo() {
      return new Port(0, PortType.PLAINTEXT);
    }

    @Override
    public HardwareState getState() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public String getDatacenterName() {
      return datacenter;
    }

    @Override
    public long getRackId() {
      return -1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      OperationPolicyDataNodeId dataNode = (OperationPolicyDataNodeId) o;

      if (index != dataNode.index) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      int result = datacenter.hashCode();
      result = 31 * result + index;
      return result;
    }

    @Override
    public int compareTo(DataNodeId o) {
      if (o == null) {
        throw new NullPointerException("input argument null");
      }

      OperationPolicyDataNodeId other = (OperationPolicyDataNodeId) o;
      int compare = (index < other.index) ? -1 : ((index == other.index) ? 0 : 1);
      if (compare == 0) {
        compare = datacenter.compareTo(other.datacenter);
      }
      return compare;
    }
  }
}
