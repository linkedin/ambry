package com.github.ambry.clustermap;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock cluster map for unit tests
 */
public class MockClusterMap implements ClusterMap {

  private PartitionId partitionId;
  private MockReplicaId replicaId;

  public MockClusterMap() {
    this.partitionId = new MockPartitionId();
    this.replicaId = new MockReplicaId();
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
  public DataNodeId getDataNodeId(String hostname, int port) {
    return new MockDataNodeId();
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    ArrayList<ReplicaId> replicaIds = new ArrayList<ReplicaId>();
    replicaIds.add(replicaId);
    return replicaIds;
  }

  public void shutdown() {
    replicaId.shutdown();
  }
}

class MockDataNodeId implements DataNodeId {

  @Override
  public String getHostname() {
    return "127.0.0.1";
  }

  @Override
  public int getPort() {
    return 6667;
  }

  @Override
  public HardwareState getState() {
    return HardwareState.AVAILABLE;
  }
}

class MockReplicaId implements ReplicaId {

  private String mountPath;
  private String replicaPath;

  public MockReplicaId() {
    File f = null;
    try {
      f = File.createTempFile("ambry", ".tmp");
      mountPath = f.getParent();
      File mountFile = new File(mountPath);
      File replicaFile = new File(mountFile, "replica");
      replicaFile.mkdir();
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
    return new MockPartitionId();
  }

  @Override
  public DataNodeId getDataNodeId() {
    return new MockDataNodeId();
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
    return null;
  }

  @Override
  public long getCapacityGB() {
    return 10000; // This is really in bytes
  }

  public void shutdown() {
    File file = new File(replicaPath);
    for (File inFiles : file.listFiles())
      inFiles.delete();
    file.delete();
  }
}
