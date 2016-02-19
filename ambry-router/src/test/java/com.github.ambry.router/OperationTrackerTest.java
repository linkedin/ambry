package com.github.ambry.router;

import com.github.ambry.clustermap.*;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.Iterator;
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
  OperationTracker ot;

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
   * localDcOnly=true, successTarget=2, localParallelism=3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 2 replicas succeeds.
   * 3. Operation succeeds.
   * 4. 1 remote fails.
   * 5. Operation remains succeed.
   */
  @Test
  public void putLocalSucceedTest() {
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, true, 2, 3);
    //send out requests to local replicas up to the localParallelFactor.
    //3-0-0-0; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-3-0-0; 9-0-0-0
    assertEquals(3, inflightReplicas.size());
    assertFalse(ot.hasSucceeded());
    for (int i = 0; i < 2; i++) {
      receiveSucceededResponse(inflightReplicas.poll(), ot);
    }
    //0-1-2-0; 9-0-0-0
    assertTrue(ot.hasSucceeded());
    assertFalse(itr.hasNext());

    receiveFailedResponse(inflightReplicas.poll(), ot);
    //0-0-2-1; 9-0-0-0
    assertTrue(ot.hasSucceeded());
    assertFalse(itr.hasNext());
  }

  /**
   * localDcOnly=true, successTarget=2, localParallelism=3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 1 local replicas succeeded, 2 failed.
   * 3. Operation fails.
   */
  @Test
  public void putLocalFailTest() {
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, true, 2, 3);
    //send out requests to local replicas up to the localParallelFactor.
    //3-0-0-0; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-3-0-0; 9-0-0-0
    for (int i = 0; i < 2; i++) {
      receiveFailedResponse(inflightReplicas.poll(), ot);
    }
    //0-1-0-2; 9-0-0-0
    //cannot send more because not all local replicas responded
    assertNull(itr.next());
    receiveSucceededResponse(inflightReplicas.poll(), ot);
    //0-0-1-2; 9-0-0-0
    assertTrue(ot.isDone());
    assertFalse(ot.hasSucceeded());
    assertNull(itr.next());
  }

  /**
   * localDcOnly=false, successTarget=1, localParallelism=2.

   * <p/>
   * 1. Get 2 local replicas to send request (and send requests);
   * 2. 1 fails， 1 pending.
   * 3. Get 1 more local replicas to send request (and send requests);
   * 4. 1 succeeds.
   * 5. Operation succeeds.
   */
  @Test
  public void getLocalSucceedTest() {
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, false, 1, 2);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //1-2-0-0; 9-0-0-0

    receiveFailedResponse(inflightReplicas.poll(), ot);
    //1-1-0-1; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());

    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-2-0-1; 9-0-0-0

    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());

    receiveSucceededResponse(inflightReplicas.poll(), ot);
    //0-1-1-1; 9-0-0-0

    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * localDcOnly=false, successTarget=1, localParallelism=2.

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
  public void getRemoteReplicaTest() {
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, false, 1, 2);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //1-2-0-0; 9-0-0-0

    receiveFailedResponse(inflightReplicas.poll(), ot);
    //1-1-0-1; 9-0-0-0

    assertFalse(ot.hasSucceeded());
    assertFalse(ot.isDone());
    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-2-0-1; 9-0-0-0

    receiveFailedResponse(inflightReplicas.poll(), ot);
    receiveFailedResponse(inflightReplicas.poll(), ot);
    //0-0-0-3; 9-0-0-0
    assertFalse(ot.hasSucceeded());
    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-0-0-3; 7-2-0-0
    assertFalse(ot.hasSucceeded());
    for (int i = 0; i < 2; i++) {
      receiveFailedResponse(inflightReplicas.poll(), ot);
    }
    //0-0-0-3; 7-0-0-2
    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-0-0-3; 5-2-0-2
    assertFalse(ot.hasSucceeded());
    receiveFailedResponse(inflightReplicas.poll(), ot);
    assertFalse(ot.isDone());
    //0-0-0-3; 5-1-0-3
    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-0-0-3; 4-1-0-3
    receiveSucceededResponse(inflightReplicas.poll(), ot);
    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * localDcOnly=false, successTarget=2, localParallelism=3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 1 succeeded.
   * 3. 1 failed.
   * 4. 1 succeeded.
   * 3. DELETE operation succeeded.
   */
  @Test
  public void deleteTest() {
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, false, 2, 3);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    //0-3-0-0; 9-0-0-0
    receiveFailedResponse(inflightReplicas.poll(), ot);
    receiveSucceededResponse(inflightReplicas.poll(), ot);
    receiveSucceededResponse(inflightReplicas.poll(), ot);
    //0-0-2-1; 9-0-0-0

    assertTrue(ot.hasSucceeded());
    assertTrue(ot.isDone());
  }

  /**
   * localDcOnly=false, successTarget=1, localParallelism=2.
   *
   * <p/>
   * 1. Get 2 local replicas to send request;
   * 2. 1 failed to send.
   * 3. get 1 more local replica to send request;
   * 4. 1 succeeded.
   * 3.
   */
  @Test
  public void failedSendTest() {
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, false, 1, 2);
    //send out requests to local AND remote replicas.
    //3-0-0-0; 9-0-0-0
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    int counter = 0;
    while (itr.hasNext()) {
      counter++;
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      if (counter == 2) {
        //fail send
      } else {
        sendReplica(nextReplica);
        itr.remove();
      }
    }
    assertEquals(2, inflightReplicas.size());
    receiveFailedResponse(inflightReplicas.poll(), ot);
    receiveFailedResponse(inflightReplicas.poll(), ot);
    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      itr.remove();
    }
    receiveSucceededResponse(inflightReplicas.poll(), ot);
    assertTrue(ot.hasSucceeded());
  }

  @Test
  public void sequenceTest() {
    int replicaCount = 4;
    datacenters = new ArrayList<DataNodeId>(Arrays.asList(new MockOpDataNode[]{new MockOpDataNode(0, "local-0")}));
    mockPartition = new MockOpPartition(replicaCount, datacenters);
    localDcName = datacenters.get(0).getDatacenterName();
    inflightReplicas = new LinkedList<ReplicaId>();
    ot = new SimpleOperationTracker(datacenters.get(0).getDatacenterName(), mockPartition, false, 1, 2);
    Iterator<ReplicaId> itr = ot.getIterator();
    ReplicaId nextReplica = null;
    ArrayList<Integer> sequence = new ArrayList<Integer>();
    int oddEvenFlag = 0;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      if (oddEvenFlag % 2 == 0) {
        sendReplica(nextReplica);
        sequence.add(((MockOpReplica) nextReplica).index);
        itr.remove();
      }
      oddEvenFlag++;
    }
    receiveFailedResponse(inflightReplicas.poll(), ot);
    receiveFailedResponse(inflightReplicas.poll(), ot);
    itr = ot.getIterator();
    oddEvenFlag = 0;
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      if (oddEvenFlag % 2 == 0) {
        sendReplica(nextReplica);
        sequence.add(((MockOpReplica) nextReplica).index);
        itr.remove();
      }
      oddEvenFlag++;
    }
    receiveFailedResponse(inflightReplicas.poll(), ot);
    itr = ot.getIterator();
    while (itr.hasNext()) {
      nextReplica = itr.next();
      assertNotNull(nextReplica);
      sendReplica(nextReplica);
      sequence.add(((MockOpReplica) nextReplica).index);
      itr.remove();
    }
    receiveSucceededResponse(inflightReplicas.poll(), ot);
    assertTrue(ot.hasSucceeded());
    assertEquals(4, sequence.size());
    assertTrue(ot.isDone());
  }

  private void sendReplica(ReplicaId replica) {
    this.inflightReplicas.offer(replica);
  }

  private void receiveSucceededResponse(ReplicaId replica, OperationTracker ot) {
    ot.onResponse(replica, null);
  }

  private void receiveFailedResponse(ReplicaId replica, OperationTracker ot) {
    ot.onResponse(replica, new Exception());
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
