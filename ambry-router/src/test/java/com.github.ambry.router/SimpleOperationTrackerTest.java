package com.github.ambry.router;

import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.Iterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for SimpleOperationTracker.
 *
 * The status of an operation is represented as in the following format:
 *
 * local unsent count-local inflight count-local succeeded count-local failed count;
 * remote unsent count-remote inflight count-remote succeeded count-remote failed count
 *
 * For example: 3-0-0-0; 9-0-0-0
 *
 */
public class SimpleOperationTrackerTest {
  ArrayList<MockDataNodeId> datanodes;
  MockPartitionId mockPartition;
  String localDcName;
  LinkedList<ReplicaId> inflightReplicas;
  OperationTracker ot;

  /**
   * Initialize 4 DCs, each DC has 1 data node, which has 3 replicas.
   */
  private void initialize() {
    int replicaCount = 12;
    ArrayList<Port> portList = new ArrayList<Port>();
    portList.add(new Port(6666, PortType.PLAINTEXT));
    List<String> mountPaths = Arrays.asList("mockMountPath");
    datanodes = new ArrayList<MockDataNodeId>(Arrays.asList(
        new MockDataNodeId[]{new MockDataNodeId(portList, mountPaths, "local-0"), new MockDataNodeId(portList,
            mountPaths, "local-1"), new MockDataNodeId(portList, mountPaths, "local-2"), new MockDataNodeId(portList,
            mountPaths, "local-3")}));
    mockPartition = new MockPartitionId();
    populateReplicaList(mockPartition, replicaCount, datanodes);
    localDcName = datanodes.get(0).getDatacenterName();
    inflightReplicas = new LinkedList<ReplicaId>();
  }

  /**
   * crossColoEnabled = false, successTarget = 2, parallelism = 3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 2 replicas succeeds.
   * 3. Operation succeeds.
   * 4. 1 local fails.
   * 5. Operation remains succeeded.
   */
  @Test
  public void localSucceedTest() {
    initialize();
    ot = new SimpleOperationTracker(localDcName, mockPartition, false, 2, 3);
    // 3-0-0-0; 9-0-0-0
    assertFalse("Operation should not succeed.", ot.hasSucceeded());
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    ReplicaId nextReplica;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull("", nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-3-0-0; 9-0-0-0
    assertEquals(3, inflightReplicas.size());
    assertFalse(ot.hasSucceeded());
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), null);
    }
    // 0-1-2-0; 9-0-0-0
    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());

    ot.onResponse(inflightReplicas.poll(), new Exception());
    // 0-0-2-1; 9-0-0-0
    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * crossColoEnabled = false, successTarget = 2, parallelism = 3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 1 local replicas succeeded, 2 failed.
   * 3. Operation fails.
   */
  @Test
  public void localFailTest() {
    initialize();
    ot = new SimpleOperationTracker(localDcName, mockPartition, false, 2, 3);
    // 3-0-0-0; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    ReplicaId nextReplica;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-3-0-0; 9-0-0-0
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), new Exception());
    }
    assertFalse(ot.hasSucceeded());
    assertTrue(ot.isDone());
    // 0-1-0-2; 9-0-0-0
    //cannot send more request
    ot.onResponse(inflightReplicas.poll(), null);
    // 0-0-1-2; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 1, parallelism = 2.

   * <p/>
   * 1. Get 2 local replicas to send request (and send requests);
   * 2. 1 fails， 1 pending.
   * 3. Get 1 more local replicas to send request (and send requests);
   * 4. 1 succeeds.
   * 5. Operation succeeds.
   */
  @Test
  public void localSucceedWithDifferentParameterTest() {
    initialize();
    ot = new SimpleOperationTracker(localDcName, mockPartition, true, 1, 2);
    // 3-0-0-0; 9-0-0-0
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    ReplicaId nextReplica;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 1-2-0-0; 9-0-0-0

    ot.onResponse(inflightReplicas.poll(), new Exception());
    // 1-1-0-1; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());

    itr = ot.getReplicaIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-2-0-1; 9-0-0-0

    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());

    ot.onResponse(inflightReplicas.poll(), null);
    // 0-1-1-1; 9-0-0-0

    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 1, parallelism = 2.

   * <p/>
   * 1. Get 2 local replicas to send request (and send requests);
   * 2. 1 local replica fails, 1 pending.
   * 3. Get 1 more local replicas to send request (and send requests);
   * 4. 2 local replica fails.
   * 5. Get 1 remote replica from each Dc to send request (and send requests);
   * 6. All fails.
   * 7. Get 1 remote replica from each DC to send request (and send requests);
   * 8. 1 fails, 2 pending.
   * 9. Get 1 remote replica from each DC to send request (and send requests);
   * 10. 2 fails.
   * 11. 1 succeeds.
   * 12. Operation succeeds.
   */
  @Test
  public void remoteReplicaTest() {
    initialize();
    ot = new SimpleOperationTracker(localDcName, mockPartition, true, 1, 2);
    // 3-0-0-0; 9-0-0-0
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    ReplicaId nextReplica;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 1-2-0-0; 9-0-0-0

    ot.onResponse(inflightReplicas.poll(), new Exception());
    // 1-1-0-1; 9-0-0-0

    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    itr = ot.getReplicaIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-2-0-1; 9-0-0-0

    ot.onResponse(inflightReplicas.poll(), new Exception());
    ot.onResponse(inflightReplicas.poll(), new Exception());
    // 0-0-0-3; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    itr = ot.getReplicaIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-0-0-3; 7-2-0-0
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), new Exception());
    }
    // 0-0-0-3; 7-0-0-2
    itr = ot.getReplicaIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-0-0-3; 5-2-0-2
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    ot.onResponse(inflightReplicas.poll(), new Exception());
    assertFalse(ot.isDone());
    // 0-0-0-3; 5-1-0-3
    itr = ot.getReplicaIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    // 0-0-0-3; 4-1-0-3
    ot.onResponse(inflightReplicas.poll(), null);
    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 12, parallelism = 3.
   *
   * This test may be meaningful for DELETE operation.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 3 succeeded.
   * 3. Operation succeeded.
   */
  @Test
  public void fullSuccessTargetTest() {
    initialize();
    ot = new SimpleOperationTracker(localDcName, mockPartition, true, 12, 3);
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    ReplicaId nextReplica;
    while (!ot.hasSucceeded()) {
      while (itr.hasNext()) {
        nextReplica = itr.next();
        assertNotNull(nextReplica);
        sendReplica(nextReplica);
        itr.remove();
      }
      for (int i = 0; i < 3; i++) {
        if (inflightReplicas.size() != 0) {
          ot.onResponse(inflightReplicas.poll(), null);
        }
      }
    }
    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 1, parallelism = 2.
   * Only 4 local replicas
   *
   * 1. Get 1st local replica to send request (and sent);
   * 2. Get 2nd local replica to send request (and failed to send);
   * 3. Get 3rd local replica to send request (and sent);
   * 4. Receive 2 failed responses from the 1st and 3rd replicas;
   * 5. Get again 2nd local replica to send request (and sent);
   * 6. Get 4th local replica to send request (and failed to send);
   * 7. Receive 1 failed responses from the 2nd replicas;
   * 8. Get again 4th local replica to send request (and sent);
   * 9. Receive 1 successful response from the 4th replica;
   * 10. Operation succeeds.
   */
  @Test
  public void useReplicaNotSucceededSendTest() {
    int replicaCount = 4;
    ArrayList<Port> portList = new ArrayList<Port>();
    portList.add(new Port(6666, PortType.PLAINTEXT));
    List<String> mountPaths = Arrays.asList("mockMountPath");
    datanodes = new ArrayList<MockDataNodeId>();
    datanodes.add(new MockDataNodeId(portList, mountPaths, "local-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(mockPartition, replicaCount, datanodes);
    localDcName = datanodes.get(0).getDatacenterName();
    inflightReplicas = new LinkedList<ReplicaId>();
    ot = new SimpleOperationTracker(localDcName, mockPartition, true, 1, 2);
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    ReplicaId nextReplica;
    int oddEvenFlag = 0;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      if (oddEvenFlag % 2 == 0) {
        sendReplica(nextReplica);
        itr.remove();
      }
      oddEvenFlag++;
    }
    ot.onResponse(inflightReplicas.poll(), new Exception());
    ot.onResponse(inflightReplicas.poll(), new Exception());
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    itr = ot.getReplicaIterator();
    oddEvenFlag = 0;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      if (oddEvenFlag % 2 == 0) {
        sendReplica(nextReplica);
        itr.remove();
      }
      oddEvenFlag++;
    }
    ot.onResponse(inflightReplicas.poll(), new Exception());
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    itr = ot.getReplicaIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    ot.onResponse(inflightReplicas.poll(), null);
    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * Send request to a replica.
   * @param replica The replica where a request is sent to.
   */
  private void sendReplica(ReplicaId replica) {
    inflightReplicas.offer(replica);
  }

  /**
   * Populate replicas for a partition.
   *
   * @param mockPartitionId The partitionId to populate its replica list.
   * @param replicaCount The number of replicas to populate.
   * @param datanodes The data nodes where replicates will be.
   */
  private void populateReplicaList(MockPartitionId mockPartitionId, int replicaCount,
      ArrayList<MockDataNodeId> datanodes) {
    int numDc = datanodes.size();
    for (int i = 0; i < replicaCount; i++) {
      mockPartitionId.replicaIds.add(new MockReplicaId(6666, mockPartitionId, datanodes.get(i % numDc), 0));
    }
  }
}

