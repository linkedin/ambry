package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestUtils {

  public static JSONObject getJsonDisk(String mountPath, HardwareState hardwareState, long capacityGB) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("mountPath", mountPath);
    jsonObject.put("hardwareState", hardwareState);
    jsonObject.put("capacityGB", capacityGB);
    return jsonObject;
  }

  // Appends "index" to baseMountPath to ensure each disk has unique mount path.
  public static JSONArray getJsonArrayDisks(int diskCount, String baseMountPath, HardwareState hardwareState, long capacityGB) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < diskCount; ++i) {
      jsonArray.put(getJsonDisk(baseMountPath + i, hardwareState, capacityGB));
    }
    return jsonArray;
  }

  // Does append anything to mountPath and so generates same disk repeatedly.
  public static JSONArray getJsonArrayDuplicateDisks(int diskCount, String mountPath, HardwareState hardwareState, long capacityGB) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < diskCount; ++i) {
      jsonArray.put(getJsonDisk(mountPath, hardwareState, capacityGB));
    }
    return jsonArray;
  }

  public static JSONObject getJsonDataNode(String hostname, int port, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("hardwareState", hardwareState);
    jsonObject.put("disks", disks);
    return jsonObject;
  }

  // Increments basePort for each DataNode to ensure unique DataNode given same hostname.
  public static JSONArray getJsonArrayDataNodes(int dataNodeCount, String hostname, int basePort, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort + i, hardwareState, disks));
    }
    return jsonArray;
  }

  // Does not increment basePort for each data node...
  public static JSONArray getJsonArrayDuplicateDataNodes(int dataNodeCount, String hostname, int basePort, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort, hardwareState, disks));
    }
    return jsonArray;
  }

  public static JSONObject getJsonDatacenter(String name, JSONArray dataNodes) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("name", name);
    jsonObject.put("dataNodes", dataNodes);
    return jsonObject;
  }

  public static JSONArray getJsonArrayDatacenters(List<String> names, List<JSONArray> dataNodes) throws JSONException {
    if(names.size() != dataNodes.size()) {
      throw new IllegalArgumentException("Size of lists do not match");
    }

    JSONArray datacenterJSONArray = new JSONArray();
    for (int i = 0; i < names.size(); i++) {
      datacenterJSONArray.put(getJsonDatacenter(names.get(i), dataNodes.get(i)));
    }
    return datacenterJSONArray;
  }

  public static JSONObject getJsonHardwareLayout(String clusterName, JSONArray datacenters) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("clusterName", clusterName);
    jsonObject.put("datacenters", datacenters);
    return jsonObject;
  }

  public static JSONObject getJsonReplica(String hostname, int port, String mountPath) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("mountPath", mountPath);
    return jsonObject;
  }

  public static JSONObject getJsonReplica(Disk disk) throws JSONException {
    return getJsonReplica(disk.getDataNode().getHostname(), disk.getDataNode().getPort(), disk.getMountPath());
  }

  public static JSONArray getJsonArrayReplicas(List<Disk> disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (Disk disk : disks) {
      jsonArray.put(getJsonReplica(disk));
    }
    return jsonArray;
  }

  public static JSONObject getJsonPartition(long id, PartitionState partitionState, long replicaCapacityGB, JSONArray replicas) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("id", id);
    jsonObject.put("partitionState", partitionState);
    jsonObject.put("replicaCapacityGB", replicaCapacityGB);
    jsonObject.put("replicas", replicas);
    return jsonObject;
  }

  public static JSONArray getJsonPartitions(long partitionCount, PartitionState partitionState, long replicaCapacityGB, int replicaCount, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for(int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCount));
      jsonArray.put(getJsonPartition(i, partitionState, replicaCapacityGB, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONArray getJsonDuplicatePartitions(long partitionCount, PartitionState partitionState, long replicaCapacityGB, int replicaCount, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for(int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCount));
      jsonArray.put(getJsonPartition(0, partitionState, replicaCapacityGB, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONArray getJsonDuplicateReplicas(long partitionCount, PartitionState partitionState, long replicaCapacityGB, int replicaCount, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for(int i = 0; i < partitionCount; i++) {
      ArrayList<Disk> disks = new ArrayList<Disk>(replicaCount);
      Disk randomDisk = testHardwareLayout.getRandomDisk();
      for(int j=0; j<replicaCount; j++) {
        disks.add(randomDisk);
      }
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(disks);
      jsonArray.put(getJsonPartition(i, partitionState, replicaCapacityGB, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONArray getJsonPartitionsWithBadIds(long partitionCount, PartitionState partitionState, long replicaCapacityGB, int replicaCount, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for(int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCount));
      jsonArray.put(getJsonPartition(i+10, partitionState, replicaCapacityGB, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONObject getJsonPartitionLayout(String clusterName, long partitionCount, JSONArray partitions) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("clusterName", clusterName);
    jsonObject.put("partitionIdFactory", partitionCount);
    jsonObject.put("partitions", partitions);
    return jsonObject;
  }

  // TODO: Better name? Extract into own file?
  public static class TestHardwareLayout {
    private static final int defaultDiskCount = 10; // per DataNode
    private static final long defaultDiskCapacityGB = 1000;
    private static final int defaultDataNodeCount = 4; // per Datacenter
    private static final int defaultDatacenterCount = 3;
    private static final int defaultBasePort = 6666;

    private int diskCount;
    private long diskCapacityGB;
    private int dataNodeCount;
    private int datacenterCount;
    private int basePort;

    private HardwareLayout hardwareLayout;

    protected JSONArray getDisks() throws JSONException {
      return getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityGB);
    }

    protected JSONArray getDataNodes(int basePort, JSONArray disks) throws JSONException {
      return getJsonArrayDataNodes(dataNodeCount, "localhost", basePort, HardwareState.AVAILABLE, disks);
    }

    protected JSONArray getDatacenters() throws JSONException {
      List<String> names = new ArrayList<String>(datacenterCount);
      List<JSONArray> dataNodes = new ArrayList<JSONArray>(datacenterCount);

      int curBasePort = basePort;
      for (int i=0; i<datacenterCount; i++) {
        names.add(i, "DC"+i);
        dataNodes.add(i, getDataNodes(curBasePort, getDisks()));
        curBasePort += dataNodeCount;
      }

      return TestUtils.getJsonArrayDatacenters(names, dataNodes);
    }

    public TestHardwareLayout(String clusterName, int diskCount, long diskCapacityGB, int dataNodeCount, int datacenterCount, int basePort) throws JSONException {
      this.diskCount = diskCount;
      this.diskCapacityGB = diskCapacityGB;
      this.dataNodeCount = dataNodeCount;
      this.datacenterCount =datacenterCount;
      this.basePort = basePort;

      this.hardwareLayout = new HardwareLayout(getJsonHardwareLayout(clusterName, getDatacenters()));
    }

    public TestHardwareLayout(String clusterName) throws JSONException {
      this(clusterName, defaultDiskCount, defaultDiskCapacityGB, defaultDataNodeCount, defaultDatacenterCount, defaultBasePort);
    }

    public HardwareLayout getHardwareLayout() {
      return hardwareLayout;
    }

    public int getDiskCount() {
      return diskCount;
    }

    public long getDiskCapacityGB() {
      return diskCapacityGB;
    }

    public int getDataNodeCount() {
      return dataNodeCount;
    }

    public int getDatacenterCount() {
      return datacenterCount;
    }

    public Datacenter getRandomDatacenter() {
      if (hardwareLayout.getDatacenters().size()==0) {
        return null;
      }
      return hardwareLayout.getDatacenters().get(new Random().nextInt(hardwareLayout.getDatacenters().size()));
    }

    public DataNode getRandomDataNode() {
      Datacenter datacenter = getRandomDatacenter();
      if (datacenter == null || datacenter.getDataNodes().size()==0) {
        return null;
      }
      return datacenter.getDataNodes().get(new Random().nextInt(datacenter.getDataNodes().size()));
    }

    public List<DataNode> getRandomDataNodes(int dataNodeCount) {
      List<DataNode> dataNodes = new ArrayList<DataNode>();
      for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        dataNodes.addAll(datacenter.getDataNodes());
      }
      if (dataNodes.size() < dataNodeCount || dataNodeCount < 0) {
        throw new IndexOutOfBoundsException("dataNodeCount out of bounds:" + dataNodeCount);
      }
      Collections.shuffle(dataNodes);
      return dataNodes.subList(0, dataNodeCount);
    }

    public Disk getRandomDisk() {
      DataNode dataNode = getRandomDataNode();
      if (dataNode == null || dataNode.getDisks().size()==0) {
        return null;
      }
      return dataNode.getDisks().get(new Random().nextInt(dataNode.getDisks().size()));
    }

    // Finds diskCount disks, each on distinct random datanodes.
    public List<Disk> getIndependentDisks(int diskCount) {
      List<DataNode> dataNodes = getRandomDataNodes(diskCount);
      List<Disk> disks = new ArrayList<Disk>(diskCount);
      for (DataNode dataNode : dataNodes) {
        disks.add(dataNode.getDisks().get(new Random().nextInt(dataNode.getDisks().size())));
      }
      return disks;
    }

    // Finds disks that share a DataNode
    public List<Disk> getDependentDisks(int diskCount) {
      List<Disk> disks = new ArrayList<Disk>(diskCount);
      if (diskCount < 2) {
        throw new IllegalArgumentException("diskcount does not make sense:" + diskCount);
      }

      // Add 2 random disks from same DataNode
      for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        for (DataNode dataNode : datacenter.getDataNodes()) {
          int numDisks = dataNode.getDisks().size();
          if (numDisks > 1) {
            diskCount -= 2;
            int rndDiskA = new Random().nextInt(numDisks);
            int rndDiskB = (rndDiskA + new Random().nextInt(numDisks - 1)) % numDisks;
            disks.add(dataNode.getDisks().get(rndDiskA));
            disks.add(dataNode.getDisks().get(rndDiskB));
          }
        }
      }

      // Add more disks, possibly adding same disk multiple times.
      while(diskCount > 0) {
        disks.add(getRandomDisk());
        diskCount--;
      }

      return disks;
    }
  }

  // TODO: Better name? Extract into own file?
  public static class TestPartitionLayout {
    protected static final int defaultPartitionCount = 10;
    protected static final PartitionState defaultPartitionState = PartitionState.READ_WRITE;
    protected static final long defaultReplicaCapacityGB = 100;
    protected static final int defaultReplicaCount = 6; // Per Partition

    protected int partitionCount;
    protected PartitionState partitionState;
    protected long replicaCapacityGB;
    protected int replicaCount;

    protected TestHardwareLayout testHardwareLayout;
    protected PartitionLayout partitionLayout;

    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      JSONArray jsonPartitions = getJsonPartitions(partitionCount, partitionState, replicaCapacityGB, replicaCount, testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), partitionCount, jsonPartitions);
    }

    public TestPartitionLayout(TestHardwareLayout testHardwareLayout, int partitionCount, PartitionState partitionState, long replicaCapacityGB, int replicaCount) throws JSONException {
      this.partitionCount = partitionCount;
      this.partitionState = partitionState;
      this.replicaCapacityGB = replicaCapacityGB;
      this.replicaCount = replicaCount;

      this.testHardwareLayout = testHardwareLayout;
      this.partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), makeJsonPartitionLayout());
    }

    public TestPartitionLayout(TestHardwareLayout testHardwareLayout) throws JSONException {
      this(testHardwareLayout, defaultPartitionCount, defaultPartitionState, defaultReplicaCapacityGB, defaultReplicaCount);
    }

    public PartitionLayout getPartitionLayout() {
      return partitionLayout;
    }

    public int getPartitionCount() {
      return partitionCount;
    }

    public int getReplicaCount() {
      return replicaCount;
    }

    public long getCapacityGB() {
      return partitionCount * replicaCount * replicaCapacityGB;
    }
  }

  public static class TestPartitionLayoutWithDuplicatePartitions  extends TestPartitionLayout {
    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      JSONArray jsonPartitions = getJsonDuplicatePartitions(partitionCount, partitionState, replicaCapacityGB, replicaCount, testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), partitionCount, jsonPartitions);
    }

    public TestPartitionLayoutWithDuplicatePartitions(TestHardwareLayout testHardwareLayout) throws JSONException {
      super(testHardwareLayout);
    }
  }

  public static class TestPartitionLayoutWithDuplicateReplicas  extends TestPartitionLayout {
    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      JSONArray jsonPartitions = getJsonDuplicateReplicas(partitionCount, partitionState, replicaCapacityGB, replicaCount, testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), partitionCount, jsonPartitions);
    }

    public TestPartitionLayoutWithDuplicateReplicas(TestHardwareLayout testHardwareLayout) throws JSONException {
      super(testHardwareLayout);
    }
  }

  public static ClusterMapManager getTestClusterMap(int partitionCount, int replicaCountPerDatacenter, long replicaCapacityGB) throws JSONException {

    TestUtils.TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);
    List<PartitionId> allocatedPartitions;

    allocatedPartitions = clusterMapManager.allocatePartitions(partitionCount, replicaCountPerDatacenter, replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 5);

    return clusterMapManager;
  }

  public static ClusterMapManager getTestClusterMap() throws JSONException {
    int numPartitions = 5;
    int replicaCountPerDatacenter = 2;
    long replicaCapacityGB = 100;

    return getTestClusterMap(numPartitions, replicaCountPerDatacenter, replicaCapacityGB);
  }

}

