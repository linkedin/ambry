package com.github.ambry.router;

import com.github.ambry.clustermap.*;
import com.github.ambry.config.AmbryPolicyConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;


/**
 * Unit test for operation policy.
 */
public class OperationPolicyTest {
  ArrayList<DataNodeId> datacenters;
  PartitionId mockPartition;
  String localDcName;
  LinkedList<ReplicaId> inflightReplicas;
  OperationTracker operationTracker;

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
   * Inline comment format: number of local unsent-inflight-succeeded-failed;
   * total remote unsent-inflight-succeeded-failed;
   * E.g., 3-0-0-0; 9-0-0-0.
   */

  /**
   * 0. localDcOnly(true), successTarget(2), localParameterFactor(3).
   *
   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 3 remote replicas succeeded.
   */
  @Test
  public void putSimpleLocalTest() {
    operationTracker = new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT,
        true, 2, 3);
    ReplicaId nextReplica = null;
    //send out requests to local replicas up to the localParallelFactor.
    //3-0-0-0; 9-0-0-0
    assertFalse(operationTracker.isComplete());
    assertFalse(operationTracker.isSucceeded());
    while (operationTracker.shouldSendMoreRequests()) {
      nextReplica = operationTracker.getNextReplicaIdForSend();
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
    }
    //0-3-0-0; 9-0-0-0
    assertEquals(3, inflightReplicas.size());
    assertFalse(operationTracker.isComplete());
    assertFalse(operationTracker.isSucceeded());
    for (int i = 0; i < 2; i++) {
      receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-1-2-0; 9-0-0-0
    assertTrue(operationTracker.isComplete());
    assertTrue(operationTracker.isSucceeded());
    assertFalse(operationTracker.shouldSendMoreRequests());
    assertNull(operationTracker.getNextReplicaIdForSend());
    ReplicaId alienReplica = new MockOpReplica(null, 0, "alien datacenter");
    try {
      receiveSucceededResponse(alienReplica, operationTracker);
    } catch (IllegalStateException e) {
    }
    try {
      receiveFailedResponse(alienReplica, operationTracker);
    } catch (IllegalStateException e) {
    }
  }

  /**
   * 0. localDcOnly(true), successTarget(2), localParameterFactor(3).
   *
   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 1 local replicas succeeded, 2 failed;
   * 3. Fail the put operation.
   */
  @Test
  public void putLocalFailedTest() {
    operationTracker = new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT,
        true, 2, 3);
    ReplicaId nextReplica = null;
    //send out requests to local replicas up to the localParallelFactor.
    //3-0-0-0; 9-0-0-0
    assertFalse(operationTracker.isComplete());
    assertFalse(operationTracker.isSucceeded());
    while (operationTracker.shouldSendMoreRequests()) {
      nextReplica = operationTracker.getNextReplicaIdForSend();
      assertNotNull(nextReplica);
      sendReplica(nextReplica, operationTracker, inflightReplicas);
    }
    //0-3-0-0; 9-0-0-0
    for (int i = 0; i < 2; i++) {
      receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-1-0-2; 9-0-0-0
    //cannot send more because not all local replicas responded
    assertFalse(operationTracker.shouldSendMoreRequests());
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-0-1-2; 9-0-0-0
    assertTrue(operationTracker.isFailed());
    assertFalse(operationTracker.isSucceeded());
    assertTrue(operationTracker.isComplete());
    assertFalse(operationTracker.shouldSendMoreRequests());
    assertNull(operationTracker.getNextReplicaIdForSend());
  }

  /**
   * 0. localDcOnly(false), successTarget(2), localParameterFactor(3).

   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 1 local replicas succeeded, 2 failed;
   * 3. Send 1 request to remote replica;
   * 4. 1 remote replica succeeded;
   */
  @Test
  public void putNoLocalBarrierTest() {
    operationTracker = new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT,
        false, 2, 3);
    ReplicaId nextReplica = null;
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    while (operationTracker.shouldSendMoreRequests()) {
      nextReplica = operationTracker.getNextReplicaIdForSend();
      sendReplica(nextReplica, operationTracker, inflightReplicas);
    }
    //0-3-0-0; 8-1-0-0
    //assertEquals(parameterSet.getLocalParallelFactor() + parameterSet.getTotalRemoteParallelFactor(),
    //inflightReplicas.size());
    for (int i = 0; i < 2; i++) {
      receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-1-0-2; 8-1-0-0
    //cannot send to more replicas due to local and total remote parallel factors.
    assertFalse(operationTracker.shouldSendMoreRequests());
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-0-1-2; 8-1-0-0
    assertFalse(operationTracker.isComplete());
    assertFalse(operationTracker.isSucceeded());
    //cannot send to more replicas due to total remote parallel factor.
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-0-1-2; 8-0-1-0
    assertTrue(operationTracker.isComplete());
    assertTrue(operationTracker.isSucceeded());
    assertFalse(operationTracker.shouldSendMoreRequests());
    assertNull(operationTracker.getNextReplicaIdForSend());
  }

  /**
   * 0. localDcOnly(false), localBarrier(true), successTarget(2), localParameterFactor(3),
   * remoteParameterFactor(1), totalRemoteParallelFactor(2).
   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 1 local replicas succeeded, 2 failed;
   * 3. Operation fails
   */
  @Test
  public void putLocalOnlyTest() {
    operationTracker = new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.PUT,
        false, 2, 3);
    ReplicaId nextReplica = null;
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    while (operationTracker.shouldSendMoreRequests()) {
      nextReplica = operationTracker.getNextReplicaIdForSend();
      sendReplica(nextReplica, operationTracker, inflightReplicas);
    }
    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    assertFalse(operationTracker.shouldSendMoreRequests());
    assertTrue(operationTracker.isComplete());
  }

  /**
   * 0. localDcOnly(false), localBarrier(true), successTarget(2), localParameterFactor(3),
   * remoteParameterFactor(1), totalRemoteParallelFactor(2).
   * <p/>
   * 1. Send 3 parallel requests to local replicas;
   * 2. 3 local requests failed;
   * 3. Send 2 to remote replica;
   * 4. 1 remote replica failed;
   * 5. 1 remote replica succeded;
   */
  @Test
  public void getLocalOnlyTest() {
    operationTracker = new RouterOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, OperationType.GET,
        false, 2, 3);
    ReplicaId nextReplica = null;
    //send out requests to local replicas.
    //3-0-0-0; 9-0-0-0
    while (operationTracker.shouldSendMoreRequests()) {
      nextReplica = operationTracker.getNextReplicaIdForSend();
      sendReplica(nextReplica, operationTracker, inflightReplicas);
    }
    //0-3-0-0; 9-0-0-0
    for (int i = 0; i < 3; i++) {
      receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    }
    //0-0-0-3; 9-0-0-0
    while (operationTracker.shouldSendMoreRequests()) {
      nextReplica = operationTracker.getNextReplicaIdForSend();
      sendReplica(nextReplica, operationTracker, inflightReplicas);
    }
    //0-0-0-3; 7-2-0-0
    receiveFailedResponse(inflightReplicas.poll(), operationTracker);
    //0-0-0-3; 7-1-0-1
    assertFalse(operationTracker.isSucceeded());
    receiveSucceededResponse(inflightReplicas.poll(), operationTracker);
    //0-0-0-3; 7-0-1-1
    assertTrue(operationTracker.isSucceeded());
  }

  void sendReplica(ReplicaId replica, OperationTracker operationPolicy, LinkedList<ReplicaId> inflightList) {
    operationPolicy.onSend(replica);
    inflightList.offer(replica);
  }

  void receiveSucceededResponse(ReplicaId replica, OperationTracker operationPolicy) {
    operationPolicy.onResponse(replica, null);
  }

  void receiveFailedResponse(ReplicaId replica, OperationTracker operationPolicy) {
    operationPolicy.onResponse(replica, new Exception());
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