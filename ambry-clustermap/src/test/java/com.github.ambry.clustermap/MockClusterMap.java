package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock cluster map for unit tests. This sets up a three node cluster
 * with 3 mount points in each. Each mount point has three partitions
 * and each partition has 3 replicas on each node.
 */
public class MockClusterMap implements ClusterMap {

  private final Map<Long, PartitionId> partitions;
  private final List<MockDataNodeId> dataNodes;

  public MockClusterMap() throws IOException {

    // create 3 nodes with each having 3 mount paths
    MockDataNodeId dataNodeId1 = createDataNode(6667, "DC1");
    MockDataNodeId dataNodeId2 = createDataNode(6668, "DC1");
    MockDataNodeId dataNodeId3 = createDataNode(6669, "DC1");

    MockDataNodeId dataNodeId4 = createDataNode(7000, "DC2");
    MockDataNodeId dataNodeId5 = createDataNode(7001, "DC2");
    MockDataNodeId dataNodeId6 = createDataNode(7002, "DC2");

    MockDataNodeId dataNodeId7 = createDataNode(7003, "DC3");
    MockDataNodeId dataNodeId8 = createDataNode(7004, "DC3");
    MockDataNodeId dataNodeId9 = createDataNode(7005, "DC3");

    dataNodes = new ArrayList<MockDataNodeId>(9);
    dataNodes.add(dataNodeId1);
    dataNodes.add(dataNodeId2);
    dataNodes.add(dataNodeId3);
    dataNodes.add(dataNodeId4);
    dataNodes.add(dataNodeId5);
    dataNodes.add(dataNodeId6);
    dataNodes.add(dataNodeId7);
    dataNodes.add(dataNodeId8);
    dataNodes.add(dataNodeId9);
    partitions = new HashMap<Long, PartitionId>();

    // create three partitions on each mount path
    long partitionId = 0;
    for (int i = 0; i < dataNodes.get(0).getMountPaths().size(); i++) {
      for (int j = 0; j < 3; j++) {
        PartitionId id = new MockPartitionId(partitionId, dataNodes, i);
        partitions.put(partitionId, id);
        partitionId++;
      }
    }
  }

  private MockDataNodeId createDataNode(int port, String datacenter) throws IOException {
    File f = null;
    try {
      List<String> mountPaths = new ArrayList<String>(3);
      f = File.createTempFile("ambry", ".tmp");
      File mountFile1 = new File(f.getParent(), "mountpathfile" + port + "0");
      mountFile1.mkdir();
      String mountPath1 = mountFile1.getAbsolutePath();

      File mountFile2 = new File(f.getParent(), "mountpathfile" + port + "1");
      mountFile2.mkdir();
      String mountPath2 = mountFile2.getAbsolutePath();

      File mountFile3 = new File(f.getParent(), "mountpathfile" + port + "2");
      mountFile3.mkdir();
      String mountPath3 = mountFile3.getAbsolutePath();
      mountPaths.add(mountPath1);
      mountPaths.add(mountPath2);
      mountPaths.add(mountPath3);
      MockDataNodeId dataNode = new MockDataNodeId(port, mountPaths, datacenter);
      return dataNode;
    }
    finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream) throws IOException {
    long id = stream.readLong();
    return partitions.get(id);
  }

  @Override
  public long getWritablePartitionIdsCount() {
    return partitions.size();
  }

  @Override
  public PartitionId getWritablePartitionIdAt(long index) {
    if (index < 0  || index >= partitions.size())
      throw new IndexOutOfBoundsException("argument invalid");
    return partitions.get(index);
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return true;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    for (DataNodeId dataNodeId : dataNodes) {
      if (dataNodeId.getHostname().compareTo(hostname) == 0 && dataNodeId.getPort() == port)
        return dataNodeId;
    }
    return null;
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    ArrayList<ReplicaId> replicaIdsToReturn = new ArrayList<ReplicaId>();
    for (PartitionId partitionId : partitions.values()) {
      List<ReplicaId> replicaIds = partitionId.getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        if (replicaId.getDataNodeId().getHostname().compareTo(dataNodeId.getHostname()) == 0 &&
            replicaId.getDataNodeId().getPort() == dataNodeId.getPort()) {
          replicaIdsToReturn.add(replicaId);
        }
      }
    }
    return replicaIdsToReturn;
  }

  @Override
  public List<DataNodeId> getDataNodeIds() {
    return new ArrayList<DataNodeId>(dataNodes);
  }

  public List<MockDataNodeId> getDataNodes() {
    return dataNodes;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    // Each server that calls this mocked interface needs its own metric registry.
    return new MetricRegistry();
  }

  public void cleanup() {
    for (PartitionId partitionId : partitions.values()) {
      MockPartitionId mockPartition = (MockPartitionId)partitionId;
      mockPartition.cleanUp();
    }

    for (DataNodeId dataNode : dataNodes) {
      List<String> mountPaths = ((MockDataNodeId)dataNode).getMountPaths();
      for (String mountPath : mountPaths) {
        File mountPathDir = new File(mountPath);
        for (File file : mountPathDir.listFiles()) {
          file.delete();
        }
        mountPathDir.delete();
      }
    }
  }
}

class MockReplicaId implements ReplicaId {

  private String mountPath;
  private String replicaPath;
  private List<ReplicaId> peerReplicas;
  private PartitionId partitionId;
  private MockDataNodeId dataNodeId;

  public MockReplicaId(int port, PartitionId partitionId, MockDataNodeId dataNodeId, int indexOfMountPathToUse) {
    this.partitionId = partitionId;
    this.dataNodeId = dataNodeId;
    mountPath = dataNodeId.getMountPaths().get(indexOfMountPathToUse);
    File mountFile = new File(mountPath);
    File replicaFile = new File(mountFile, "replica" + port + ((MockPartitionId)partitionId).partition);
    replicaFile.mkdir();
    replicaFile.deleteOnExit();
    replicaPath = replicaFile.getAbsolutePath();
  }

  @Override
  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return dataNodeId;
  }

  @Override
  public String getMountPath() {
    return mountPath;
  }

  @Override
  public String getReplicaPath() {
    return replicaPath;
  }

  @Override
  public List<ReplicaId> getPeerReplicaIds() {
    return peerReplicas;
  }

  public void setPeerReplicas(List<ReplicaId> peerReplicas) {
    this.peerReplicas = new ArrayList<ReplicaId>();
    for (ReplicaId replicaId : peerReplicas) {
      if (!(replicaId.getMountPath().compareTo(mountPath) == 0)) {
        this.peerReplicas.add(replicaId);
      }
    }
  }

  @Override
  public long getCapacityInBytes() {
    return 10000000;
  }

  @Override
  public DiskId getDiskId() {
    return new DiskId() {
      @Override
      public String getMountPath() {
        return mountPath;
      }

      @Override
      public HardwareState getState() {
        return HardwareState.AVAILABLE;
      }

      @Override
      public long getRawCapacityInBytes() {
        return 100000;
      }
    };
  }

  @Override
  public String toString() {
    return "Mount Path " + mountPath + " Replica Path " + replicaPath;
  }

  public void cleanup() {
    File replicaDir = new File(replicaPath);
    for (File replica : replicaDir.listFiles()) {
      replica.delete();
    }
    replicaDir.delete();
  }
}
