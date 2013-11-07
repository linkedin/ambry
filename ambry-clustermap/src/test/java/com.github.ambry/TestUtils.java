package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 *
 */
public class TestUtils {
  static private int basePort = 6666;
  static private int baseMountPathOffset = 0;

  static private PartitionId prevPartitionId = PartitionId.getFirstPartitionId();
  static public final long replicaCapacityGB = 100;
  static public final long diskCapacityGB = 1000;


  // Permit Disk to be constructed with null DataNode
  public static class TestDisk extends Disk {

    // Permit disk to be constructed without a DataNode.
    public TestDisk(JSONObject jsonObject) throws JSONException {
      super(null, jsonObject);
    }

    public TestDisk(DiskId diskId, long capacityGB) {
      super(null, diskId, capacityGB);
    }

    @Override
    public void validateDataNode() {
      // Null DataNode OK for test.
    }
  }

  // Permit DataNode to be constructed with a null Datacenter
  public static class TestDataNode extends DataNode {

    public TestDataNode(JSONObject jsonObject) throws JSONException {
      super(null, jsonObject);
    }

    public TestDataNode(String hostname, int port) {
      super(null, hostname, port);
    }

    @Override
    public void validateDatacenter() {
      // Null datacenter OK for test.
    }
  }

  // Permit Datacenter to be constructed with a null Cluster
  public static class TestDatacenter extends Datacenter {

    public TestDatacenter(JSONObject jsonObject) throws JSONException {
      super(null, jsonObject);
    }

    public TestDatacenter(String name) {
      super(null, name);
    }

    @Override
    public void validateCluster() {
      // Null cluster OK for test.
    }
  }

  // Permit Partition to be constructed with a null Layout
  public static class TestPartition extends Partition {

    TestPartition(PartitionId partitionId, long replicaCapacityGB) {
      super(null, partitionId, replicaCapacityGB);
    }

    TestPartition(JSONObject jsonObject) throws JSONException {
      super(null, jsonObject);
    }

    @Override
    protected void validateLayout() {
      // Null layout OK for test.
    }
  }

  static public int getNewPort() {
    int port = basePort;
    basePort++;
    return port;
  }

  static public String getNewMountPath() {
    String mountPath = "/mnt" + baseMountPathOffset;
    baseMountPathOffset++;
    return mountPath;
  }

  // Return fully constructed DataNodeId
  static public DataNodeId getNewDataNodeId() {
    return new DataNodeId("localhost", getNewPort());
  }

  // Return fully constructed DiskId
  static public DiskId getNewDiskId() {
    return new DiskId(getNewDataNodeId(), getNewMountPath());
  }

  // Return fully constructed Disk for given DataNode.
  static public Disk getNewDisk(DataNode dataNode) {
    return new Disk(dataNode, getNewMountPath(), diskCapacityGB);
  }

  // Return partially constructed Disk. Returned Disk lacks "parent" reference to DataNode.
  static public Disk getNewTestDisk() {
    return new TestDisk(getNewDiskId(), diskCapacityGB);
  }

  // Add two disks to dataNode.
  static public void populateDataNode(DataNode dataNode) {
    dataNode.addDisk(getNewDisk(dataNode));
    dataNode.addDisk(getNewDisk(dataNode));
  }

  // Return fully constructed DataNode with two disks.
  static public DataNode getNewDataNode(Datacenter datacenter) {
    DataNode dataNode = new DataNode(datacenter, "localhost", getNewPort());
    populateDataNode(dataNode);
    return dataNode;
  }

  // Return partially constructed DataNode. Returned DataNode lacks "parent" reference to Datacenter. Has two disks.
  static public DataNode getNewTestDataNode() {
    DataNode dataNode = new TestDataNode("localhost", getNewPort());
    populateDataNode(dataNode);
    return dataNode;
  }

  // Add two DataNodes to Datacenter
  static public void populateDatacenter(Datacenter datacenter) {
    datacenter.addDataNode(getNewDataNode(datacenter));
    datacenter.addDataNode(getNewDataNode(datacenter));
  }

  // Return fully constructed Datacenter populated with DataNodes and Disks.
  static public Datacenter getNewDatacenter(Cluster cluster, String name) {
    Datacenter datacenter = new Datacenter(cluster, name);
    populateDatacenter(datacenter);
    return datacenter;
  }

  // Return partially constructed Datacenter. Returned Datacenter lacks "parent" reference to Cluster. Has two nodes.
  static public Datacenter getNewTestDatacenter(String name) {
    Datacenter datacenter = new TestDatacenter(name);
    populateDatacenter(datacenter);
    return datacenter;
  }

  // Return fully constructed cluster populated with datacenters, datanodes, and disks. Cluster is built using
  // various TestUtils methods.
  static public Cluster getNewCluster(String name) {
    Cluster cluster = new Cluster(name);

    cluster.addDatacenter(getNewDatacenter(cluster, "ELA4"));
    cluster.addDatacenter(getNewDatacenter(cluster, "LVA1"));

    return cluster;
  }

  // Return fully constructed cluster populated with datacenters, datanodes, and disks. Cluster is built using the
  // cluster API.
  public static Cluster buildCluster(String clusterName) {
    Cluster cluster = new Cluster(clusterName);

    cluster.addNewDataCenter("ELA4");

    int port = getNewPort();
    cluster.addNewDataNode("ELA4", "localhost", port);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    port = getNewPort();
    cluster.addNewDataNode("ELA4", "localhost", port);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    cluster.addNewDataCenter("LVA1");

    port = getNewPort();
    cluster.addNewDataNode("LVA1", "localhost", port);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    port = getNewPort();
    cluster.addNewDataNode("LVA1", "localhost", port);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    cluster.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    return cluster;
  }

  // Return fully constructed PartitionId
  static public PartitionId getNewPartitionId() {
    if (prevPartitionId == null) {
      prevPartitionId = PartitionId.getFirstPartitionId();
    } else {
      prevPartitionId = PartitionId.getNewPartitionId(prevPartitionId);
    }
    return prevPartitionId;
  }

  static public Partition getNewTestPartition() {
    return new TestPartition(getNewPartitionId(), replicaCapacityGB);
  }

  // Return fully constructed ReplicaId
  static public ReplicaId getNewReplicaId() {
    return new ReplicaId(getNewPartitionId(), getNewDiskId());
  }

  // Populate disksA and disksB for use in Partition construction. Hacky and ugly interface.
  private static void populateDiskSetsForPartitions(Cluster cluster, ArrayList<Disk> disksA, ArrayList<Disk> disksB) {
    for (Datacenter datacenter : cluster.getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        int count = 0;
        for (Disk disk : dataNode.getDisks()) {
          if (count == 0) {
            disksA.add(disk);
            count++;
          } else if (count == 1) {
            disksB.add(disk);
            count++;
          } else {
            break;
          }
        }
      }
    }
  }

  public static Layout buildLayout(Cluster cluster) {
    Layout layout = new Layout(cluster);

    ArrayList<Disk> disksA = new ArrayList<Disk>();
    ArrayList<Disk> disksB = new ArrayList<Disk>();
    populateDiskSetsForPartitions(cluster, disksA, disksB);

    layout.addNewPartition(disksA, replicaCapacityGB);
    layout.addNewPartition(disksB, replicaCapacityGB);

    return layout;
  }

  public static ClusterMapManager buildClusterMapManagerIndirectly(String name) {
    Cluster cluster = buildCluster(name);
    Layout layout = new Layout(cluster);

    ClusterMapManager clusterMapManager = new ClusterMapManager(cluster, layout);

    ArrayList<Disk> disksA = new ArrayList<Disk>();
    ArrayList<Disk> disksB = new ArrayList<Disk>();
    populateDiskSetsForPartitions(cluster, disksA, disksB);

    clusterMapManager.addNewPartition(disksA, replicaCapacityGB);
    clusterMapManager.addNewPartition(disksB, replicaCapacityGB);

    return clusterMapManager;
  }


  public static ClusterMapManager buildClusterMapManagerWithoutPartitions(String name) {
    Cluster cluster = buildCluster(name);
    Layout layout = new Layout(cluster);

    return new ClusterMapManager(cluster, layout);
  }

  public static ClusterMapManager buildClusterMapManagerDirectly(String clusterName) {
    ClusterMapManager clusterMapManager = new ClusterMapManager(clusterName);

    clusterMapManager.addNewDataCenter("ELA4");

    int port = getNewPort();
    clusterMapManager.addNewDataNode("ELA4", "localhost", port);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    port = getNewPort();
    clusterMapManager.addNewDataNode("ELA4", "localhost", port);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    clusterMapManager.addNewDataCenter("LVA1");

    port = getNewPort();
    clusterMapManager.addNewDataNode("LVA1", "localhost", port);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    port = getNewPort();
    clusterMapManager.addNewDataNode("LVA1", "localhost", port);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);
    clusterMapManager.addNewDisk("localhost", port, getNewMountPath(), diskCapacityGB);

    ArrayList<Disk> disksA = new ArrayList<Disk>();
    ArrayList<Disk> disksB = new ArrayList<Disk>();
    populateDiskSetsForPartitions(clusterMapManager.getCluster(), disksA, disksB);

    clusterMapManager.addNewPartition(disksA, replicaCapacityGB);
    clusterMapManager.addNewPartition(disksB, replicaCapacityGB);

    return clusterMapManager;
  }


}
