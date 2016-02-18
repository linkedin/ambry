package com.github.ambry.router;

import com.github.ambry.clustermap.*;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;


/**
 * Unit test for operation tracker.
 */
public class OperationTrackerTest {
  ArrayList<DataNodeId> datacenters;
  PartitionId mockPartition;
  String localDcName;
  LinkedList<ReplicaId> inflightReplicas;
  OperationTracker operationTracker;

  /**
   * Initialize 4 DCs, each DC has 3 replicas.
   */
  @Before
  public void initialize() {
    int replicaCount = 12;
    datacenters = new ArrayList<DataNodeId>(Arrays.asList(
        new MockOpDataNode[]{new MockOpDataNode(0, "local-0"), new MockOpDataNode(1, "remote-1"), new MockOpDataNode(2,
            "remote-2"), new MockOpDataNode(3, "remote-3")}));
    mockPartition = new MockOpPartition(replicaCount, datacenters);
    localDcName = datacenters.get(0).getDatacenterName();
    inflightReplicas = new LinkedList<ReplicaId>();
  }

  /**
   * Inline comment format: number of {@code local unsent-inflight-succeeded-failed};
   * {@code total remote unsent-inflight-succeeded-failed}.
   * E.g., 3-0-0-0; 9-0-0-0.
   */

  /**
   * operationType=PUT, localDcOnly=true, successTarget=2, localParallelism=3.
   *
   * Test when alien replicas are passed to an operation tracker.
   */
  @Test
  public void exceptionTest() {
    operationTracker =
        new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT, true, 2,
            3);
    ReplicaId alienReplica = new MockOpReplica(null, 0, "alien datacenter");
    try {
      receiveSucceededResponse(alienReplica, operationTracker);
    } catch (IllegalStateException e) {
    }
    try {
      receiveFailedResponse(alienReplica, operationTracker);
      fail();
    } catch (IllegalStateException e) {
    }
  }

  /**
   * operationType=PUT, localDcOnly=true, successTarget=2, localParallelism=3.
   *
   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 2 replicas succeeds.
   * 3. PUT operation succeeds.
   * 4. 1 remote fails.
   * 5. PUT operation remains succeed.
   */
  @Test
  public void putLocalSucceedTest() {
    operationTracker =
        new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT, true, 2,
            3);
    //send out requests to local replicas up to the localParallelFactor.
    //3-0-0-0; 9-0-0-0
    assertFalse(operationTracker.hasSucceeded());
    ReplicaId nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-3-0-0; 9-0-0-0
    assertEquals(3, inflightReplicas.size());
    assertFalse(operationTracker.hasSucceeded());
    for (int i = 0; i < 2; i++) {
      receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-1-2-0; 9-0-0-0
    assertTrue(operationTracker.hasSucceeded());
    assertNull(operationTracker.getNextReplica());
    assertNull(operationTracker.getNextReplica());

    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    //0-0-2-1; 9-0-0-0
    assertTrue(operationTracker.hasSucceeded());
    assertNull(operationTracker.getNextReplica());
    assertNull(operationTracker.getNextReplica());
  }

  /**
   * operationType=PUT, localDcOnly=true, successTarget=2, localParallelism=3.
   *
   * <p/>
   * 1. Send 3 parallel requests to local replicas.
   * 2. 1 local replicas succeeded, 2 failed.
   * 3. Put operation fails.
   */
  @Test
  public void putLocalFailTest() {
    operationTracker =
        new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT, true, 2,
            3);
    //send out requests to local replicas up to the localParallelFactor.
    //3-0-0-0; 9-0-0-0
    assertFalse(operationTracker.hasSucceeded());
    ReplicaId nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-3-0-0; 9-0-0-0
    for (int i = 0; i < 2; i++) {
      receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-1-0-2; 9-0-0-0
    //cannot send more because not all local replicas responded
    assertNull(operationTracker.getNextReplica());
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-0-1-2; 9-0-0-0
    assertTrue(operationTracker.hasFailed());
    assertFalse(operationTracker.hasSucceeded());
    assertNull(operationTracker.getNextReplica());
    assertNull(operationTracker.getNextReplica());
  }

  /**
   * operationType=GET, localDcOnly=false, successTarget=1, localParallelism=2.

   * <p/>
   * 1. Send 2 parallel requests to local replicas;
   * 2. 1 fails， 1 pending.
   * 3. Send 1 more request.
   * 4. 1 succeeds.
   * 5. GET operation succeeds.
   */
  @Test
  public void getLocalSucceedTest() {
    operationTracker =
        new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.GET, false, 1,
            2);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    ReplicaId nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //1-2-0-0; 9-0-0-0

    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    //1-1-0-1; 9-0-0-0
    assertFalse(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());

    nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-2-0-1; 9-0-0-0

    assertFalse(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());

    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-1-1-1; 9-0-0-0

    assertTrue(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());
  }

  /**
   * operationType=GET, localDcOnly=false, successTarget=1, localParallelism=2.

   * <p/>
   * 1. Send 2 parallel requests to local replicas.
   * 2. 1 local replica fails, 1 pending.
   * 3. Send 1 request to local replica.
   * 4. 2 local replica fails.
   * 5. Send 1 request to each remote DC
   * 6. All fails.
   * 7. Send 1 request to each remote DC
   * 8. 1 fails, 2 pending.
   * 9. Send 1 request to remote DC.
   * 10. 2 fails.
   * 11. 1 succeeds.
   * 12. GET operation succeeds.
   */
  @Test
  public void getRemoteReplicaTest() {
    operationTracker =
        new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT, false, 1,
            2);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    ReplicaId nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //1-2-0-0; 9-0-0-0

    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    //1-1-0-1; 9-0-0-0

    assertFalse(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());
    nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-2-0-1; 9-0-0-0

    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    //0-0-0-3; 9-0-0-0
    assertFalse(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());

    nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-0-0-3; 6-3-0-0
    assertFalse(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());
    for (int i = 0; i < 3; i++) {
      receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-0-0-3; 6-0-0-3

    nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-0-0-3; 3-3-0-3
    assertFalse(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());
    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    //0-0-0-3; 3-2-0-4

    nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-0-0-3; 2-3-0-4
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    assertTrue(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());
  }

  /**
   * operationType=DELETE, localDcOnly=false, successTarget=2, localParallelism=3.
   *
   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 1 succeeded.
   * 3. 1 failed.
   * 4. 1 succeeded.
   * 3. DELETE operation fails.
   */
  @Test
  public void deleteTest() {
    operationTracker =
        new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.DELETE, false,
            2, 3);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    ReplicaId nextReplica = operationTracker.getNextReplica();
    while (nextReplica != null) {
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
      nextReplica = operationTracker.getNextReplica();
    }
    //0-3-0-0; 9-0-0-0
    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-0-2-1; 9-0-0-0

    assertTrue(operationTracker.hasSucceeded());
    assertFalse(operationTracker.hasFailed());
  }

  void sendReplica(ReplicaId replica, OperationTracker operationPolicy, LinkedList<ReplicaId> inflightList) {
    inflightList.offer(replica);
  }

  void receiveSucceededResponse(ReplicaId replica, OperationTracker operationPolicy) {
    operationPolicy.onResponse(replica, null);
  }

  void receiveFailedResponse(ReplicaId replica, OperationTracker operationTracker) {
    operationTracker.onResponse(replica, new Exception());
  }
}

class MockOpPartition extends PartitionId {
  List<ReplicaId> replicaIds;

  MockOpPartition(int replicaCount, ArrayList<DataNodeId> datacenters) {
    this.replicaIds = new ArrayList<ReplicaId>(replicaCount);
    int numDc = datacenters.size();
    for (int i = 0; i < replicaCount; i++) {
      replicaIds.add(new MockOpReplica(this, i, datacenters.get(i % numDc).getDatacenterName()));
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

class MockOpReplica implements ReplicaId {
  MockOpPartition partitionId;
  int index;
  String datacenter;

  MockOpReplica(MockOpPartition partitionId, int index, String datacenter) {
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
    return new MockOpDataNode(index, datacenter);
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

class MockOpDataNode extends DataNodeId {
  int index;
  String datacenter;

  MockOpDataNode(int index, String datacenter) {
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
  public Port getPortToConnectTo(ArrayList<String> sslEnabledDataCenters) {
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockOpDataNode dataNode = (MockOpDataNode) o;

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

    MockOpDataNode other = (MockOpDataNode) o;
    int compare = (index < other.index) ? -1 : ((index == other.index) ? 0 : 1);
    if (compare == 0) {
      compare = datacenter.compareTo(other.datacenter);
    }
    return compare;
  }
}
