package com.github.ambry.clustermap;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock cluster map for unit tests
 */
public class MockClusterMap implements ClusterMap {

  private PartitionId partitionId;
  private List<ReplicaId> replicaIds;

  public MockClusterMap() {
    this.replicaIds = new ArrayList<ReplicaId>(3);
    this.partitionId = new MockPartitionId(replicaIds);
    MockReplicaId replicaId1 = new MockReplicaId(6667, partitionId);
    MockReplicaId replicaId2 = new MockReplicaId(6668, partitionId);
    MockReplicaId replicaId3 = new MockReplicaId(6669, partitionId);
    List<ReplicaId> peerReplicaOfReplicaId1 = new ArrayList<ReplicaId>();
    peerReplicaOfReplicaId1.add(replicaId2);
    peerReplicaOfReplicaId1.add(replicaId3);
    replicaId1.setPeerReplicas(peerReplicaOfReplicaId1);

    List<ReplicaId> peerReplicaOfReplicaId2 = new ArrayList<ReplicaId>();
    peerReplicaOfReplicaId2.add(replicaId1);
    peerReplicaOfReplicaId2.add(replicaId3);
    replicaId2.setPeerReplicas(peerReplicaOfReplicaId2);

    List<ReplicaId> peerReplicaOfReplicaId3 = new ArrayList<ReplicaId>();
    peerReplicaOfReplicaId3.add(replicaId1);
    peerReplicaOfReplicaId3.add(replicaId2);
    replicaId1.setPeerReplicas(peerReplicaOfReplicaId3);

    this.replicaIds.add(replicaId1);
    this.replicaIds.add(replicaId2);
    this.replicaIds.add(replicaId3);
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream) throws IOException {
    stream.readLong();
    return partitionId;
  }

  @Override
  public long getWritablePartitionIdsCount() {
    return 1;
  }

  @Override
  public PartitionId getWritablePartitionIdAt(long index) {
    if (index >= 1 || index < 0)
      throw new IndexOutOfBoundsException("argument invalid");
    return partitionId;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return true;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    for (int i = 0; i < replicaIds.size(); i++) {
      if (replicaIds.get(i).getDataNodeId().getPort() == port)
        return replicaIds.get(i).getDataNodeId();
    }
    return null;
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    ArrayList<ReplicaId> replicaIdsToReturn = new ArrayList<ReplicaId>();
    for (int i = 0; i < replicaIds.size(); i++) {
      if (replicaIds.get(i).getDataNodeId().getPort() == dataNodeId.getPort())
        replicaIdsToReturn.add(replicaIds.get(i));
    }
    return replicaIdsToReturn;
  }

  public void cleanup() {
    for (ReplicaId replicaId : replicaIds) {
      File replicaDir = new File(replicaId.getReplicaPath());
      for (File replica : replicaDir.listFiles()) {
        replica.delete();
      }
      replicaDir.delete();
    }
  }
}

class MockReplicaId implements ReplicaId {

  private String mountPath;
  private String replicaPath;
  private int port;
  private List<ReplicaId> peerReplicas;
  private PartitionId partitionId;

  public MockReplicaId(int port, PartitionId partitionId) {
    this.port = port;
    this.partitionId = partitionId;
    File f = null;
    try {
      f = File.createTempFile("ambry", ".tmp");
      mountPath = f.getParent();
      File mountFile = new File(mountPath);
      File replicaFile = new File(mountFile, "replica" + port);
      replicaFile.mkdir();
      replicaFile.deleteOnExit();
      replicaPath = replicaFile.getAbsolutePath();
    }
    catch (IOException e) {
      // ignore we will fail later in tests
    }
    finally {
      f.delete();
    }
  }

  @Override
  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return new MockDataNodeId(port);
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
    this.peerReplicas = peerReplicas;
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
}
