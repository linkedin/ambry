package com.github.ambry.coordinator;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.Assert;
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

  /**
   * SerialOperationPolicy is used as the Policy for GetOperation
   * @throws CoordinatorException
   */
  @Test
  public void testSerialOperationPolicy()
      throws CoordinatorException {
    // Simple success test
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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

    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), false);

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

    // Corruption test
    {
      OperationPolicy op = new SerialOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), false);

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
      OperationPolicy op = new GetTwoInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
  public void testGetCustomParallelOperationPolicy()
      throws CoordinatorException {
    // Simple success test
    {
      OperationPolicy op = new GetCustomParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6),
          true, 2);

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
      OperationPolicy op = new GetCustomParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6),
          true, 2);

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

    // Many failures but still succeeds
    // A different version of above testcase, where in first remote replica succeeds
    {
      OperationPolicy op = new GetCustomParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6),
          true, 2);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();
      assertTrue(op.sendMoreRequests(replicasInFlight));

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      op.sendMoreRequests(replicasInFlight);
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      op.sendMoreRequests(replicasInFlight);
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      op.sendMoreRequests(replicasInFlight);
      assertTrue(op.mayComplete());

      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
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
      // send more should return true for remote now
      assertTrue(op.sendMoreRequests(replicasInFlight));
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
      OperationPolicy op = new GetCustomParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6),
          true, 2);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      replicasInFlight.add(replicaId0);
      assertTrue(op.mayComplete());

      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      replicasInFlight.add(replicaId1);
      assertTrue(op.mayComplete());

      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      replicasInFlight.add(replicaId2);
      assertTrue(op.mayComplete());

      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
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
      // send more should return true for remote now
      assertTrue(op.sendMoreRequests(replicasInFlight));
      replicasInFlight.add(replicaId3);
      assertTrue(op.mayComplete());

      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      replicasInFlight.add(replicaId4);
      assertTrue(op.mayComplete());

      op.onFailedResponse(replicaId3);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      replicasInFlight.remove(replicaId3);
      assertTrue(op.mayComplete());

      assertFalse(op.sendMoreRequests(replicasInFlight));
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

    // Failure test;  ensure local probe policy is enforced (local replicas then remote); ensure sendMore is correct (2
    // plus 1 for good luck in flight)
    {
      OperationPolicy op = new GetCustomParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6),
          true, 2);

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
      assertFalse(op.sendMoreRequests(replicasInFlight));
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
      OperationPolicy op = new GetCustomParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6),
          true, 2);

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
  public void testPutPolicy()
      throws CoordinatorException {
    // Simple success test. Requires 2 successes to complete
    {
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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

    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op = new PutParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), false);

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
    // Simple success test
    {
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

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
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), true);

      assertFalse(op.isCorrupt());
      assertFalse(op.isComplete());
      assertEquals(op.getReplicaIdCount(), 6);
      assertTrue(op.mayComplete());
      assertTrue(op.sendMoreRequests(new HashSet<ReplicaId>()));

      Set<ReplicaId> replicasInFlight = new HashSet<ReplicaId>();

      System.out.println(op);
      ReplicaId replicaId0 = op.getNextReplicaIdForSend();
      assertEquals(replicaId0.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId0);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      ReplicaId replicaId1 = op.getNextReplicaIdForSend();
      assertEquals(replicaId1.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId1);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      ReplicaId replicaId2 = op.getNextReplicaIdForSend();
      assertEquals(replicaId2.getDataNodeId().getDatacenterName(), datacenterAlpha);
      replicasInFlight.add(replicaId2);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      ReplicaId replicaId3 = op.getNextReplicaIdForSend();
      assertEquals(replicaId3.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId3);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      ReplicaId replicaId4 = op.getNextReplicaIdForSend();
      assertEquals(replicaId4.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId4);
      assertTrue(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      ReplicaId replicaId5 = op.getNextReplicaIdForSend();
      assertEquals(replicaId5.getDataNodeId().getDatacenterName(), datacenterBeta);
      replicasInFlight.add(replicaId5);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      op.onFailedResponse(replicaId0);
      replicasInFlight.remove(replicaId0);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      op.onFailedResponse(replicaId1);
      replicasInFlight.remove(replicaId1);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      op.onFailedResponse(replicaId2);
      replicasInFlight.remove(replicaId2);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      op.onFailedResponse(replicaId3);
      replicasInFlight.remove(replicaId3);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertTrue(op.mayComplete());

      System.out.println(op);
      op.onFailedResponse(replicaId4);
      replicasInFlight.remove(replicaId4);
      assertFalse(op.sendMoreRequests(replicasInFlight));
      assertFalse(op.mayComplete());

      System.out.println(op);
      assertFalse(op.isComplete());
      assertFalse(op.isCorrupt());
    }

    // Failure test;  ensure local probe policy is enforced and remote calls do not happen
    {
      OperationPolicy op = new AllInParallelOperationPolicy(datacenterAlpha, new OperationPolicyPartitionId(6), false);

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

  class OperationPolicyPartitionId extends PartitionId {
    List<ReplicaId> replicaIds;

    OperationPolicyPartitionId(int replicaCount) {
      this.replicaIds = new ArrayList<ReplicaId>(replicaCount);
      for (int i = 0; i < replicaCount; i++) {
        if (i % 2 == 0) {
          replicaIds.add(new OperationPolicyReplicaId(this, i, datacenterAlpha));
        } else {
          replicaIds.add(new OperationPolicyReplicaId(this, i, datacenterBeta));
        }
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
    public HardwareState getState() {
      throw new IllegalStateException("Should not be invoked.");
    }

    @Override
    public String getDatacenterName() {
      return datacenter;
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
