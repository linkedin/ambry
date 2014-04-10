package com.github.ambry.clustermap;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock cluster map for unit tests
 */
public class MockClusterMap implements ClusterMap {

  private Map<Long, PartitionId> partitions;
  private List<MockDataNodeId> dataNodes;


  public MockClusterMap() throws IOException {

    // create 3 nodes with each having 3 mount paths
    MockDataNodeId dataNodeId1 = createDataNode(6667);
    MockDataNodeId dataNodeId2 = createDataNode(6668);
    MockDataNodeId dataNodeId3 = createDataNode(6669);
    dataNodes = new ArrayList<MockDataNodeId>(3);
    dataNodes.add(dataNodeId1);
    dataNodes.add(dataNodeId2);
    dataNodes.add(dataNodeId3);
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

  private MockDataNodeId createDataNode(int port) throws IOException {
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
      MockDataNodeId dataNode = new MockDataNodeId(port, mountPaths);
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
    return 100000;
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
      public long getCapacityInBytes() {
        return 100000;
      }
    };
  }

  public void cleanup() {
    File replicaDir = new File(replicaPath);
    for (File replica : replicaDir.listFiles()) {
      replica.delete();
    }
    replicaDir.delete();
  }
}
