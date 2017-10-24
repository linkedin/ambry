/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static org.junit.Assert.*;


public class TestUtils {

  public static String getLocalHost() {
    try {
      return InetAddress.getByName("localhost").getCanonicalHostName().toLowerCase();
    } catch (UnknownHostException e) {
      // this should never happen
      return null;
    }
  }

  public static JSONObject getJsonDisk(String mountPath, HardwareState hardwareState, long capacityInBytes)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("mountPath", mountPath);
    jsonObject.put("hardwareState", hardwareState);
    jsonObject.put("capacityInBytes", capacityInBytes);
    return jsonObject;
  }

  // Appends "index" to baseMountPath to ensure each disk has unique mount path.
  public static JSONArray getJsonArrayDisks(int diskCount, String baseMountPath, HardwareState hardwareState,
      long capacityInBytes) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < diskCount; ++i) {
      jsonArray.put(getJsonDisk(baseMountPath + i, hardwareState, capacityInBytes));
    }
    return jsonArray;
  }

  // Does append anything to mountPath and so generates same disk repeatedly.
  public static JSONArray getJsonArrayDuplicateDisks(int diskCount, String mountPath, HardwareState hardwareState,
      long capacityInBytes) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < diskCount; ++i) {
      jsonArray.put(getJsonDisk(mountPath, hardwareState, capacityInBytes));
    }
    return jsonArray;
  }

  public static JSONObject getJsonDataNode(String hostname, int port, HardwareState hardwareState, JSONArray disks)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("hardwareState", hardwareState);
    jsonObject.put("disks", disks);
    return jsonObject;
  }

  public static JSONObject getJsonDataNode(String hostname, int port, int sslPort, HardwareState hardwareState,
      JSONArray disks) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("sslport", sslPort);
    jsonObject.put("hardwareState", hardwareState);
    jsonObject.put("disks", disks);
    return jsonObject;
  }

  /**
   * Generate a JSON data node object with a defined {@code rackId}.
   *
   * @param hostname the hostname for the node
   * @param port the plaintext port number for the node
   * @param sslPort the ssl port number for the node
   * @param rackId the rack ID for the node
   * @param hardwareState A {@link HardwareState} value for the node
   * @param disks an array of disks belonging to the node
   * @return a {@link JSONObject) representing the node with the properties passed into the function
   * @throws JSONException
   */
  public static JSONObject getJsonDataNode(String hostname, int port, int sslPort, long rackId,
      HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("sslport", sslPort);
    jsonObject.put("rackId", rackId);
    jsonObject.put("hardwareState", hardwareState);
    jsonObject.put("disks", disks);
    return jsonObject;
  }

  /**
   * Generates an array of JSON data node objects.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *
   * @param dataNodeCount how many data nodes to generate
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   * @return a {@link JSONArray} of nodes
   */
  static JSONArray getJsonArrayDataNodes(int dataNodeCount, String hostname, int basePort, int sslPort,
      HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort + i, sslPort + i, hardwareState, disks));
    }
    return jsonArray;
  }

  /**
   * Updates the given array of JSON data node objects by adding new entries to reach the total datanode count.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *
   * @param dataNodeJsonArray the datanode JSONArray to update.
   * @param dataNodeCount how many total datanode entries the JSONArray should have.
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   */
  private static void updateJsonArrayDataNodes(JSONArray dataNodeJsonArray, int dataNodeCount, String hostname,
      int basePort, int sslPort, HardwareState hardwareState, JSONArray disks) throws JSONException {
    for (int i = dataNodeJsonArray.length(); i < dataNodeCount; ++i) {
      dataNodeJsonArray.put(getJsonDataNode(hostname, basePort + i, sslPort + i, hardwareState, disks));
    }
  }

  /**
   * Generates an array of JSON data node objects, each with a defined rack ID.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *
   * @param dataNodeCount how many data nodes to generate
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param numRacks how many distinct racks the data nodes should use
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   * @return a {@link JSONArray} of nodes
   */
  static JSONArray getJsonArrayDataNodesRackAware(int dataNodeCount, String hostname, int basePort, int sslPort,
      int numRacks, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort + i, sslPort + i, i % numRacks, hardwareState, disks));
    }
    return jsonArray;
  }

  /**
   * Updates the given array of JSON data node objects by adding new entries to reach the total datanode count.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *
   * @param dataNodeJsonArray the datanode JSONArray to update.
   * @param dataNodeCount how many total datanode entries the JSONArray should have.
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param numRacks how many distinct racks the data nodes should use
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   */
  private static void updateJsonArrayDataNodesRackAware(JSONArray dataNodeJsonArray, int dataNodeCount, String hostname,
      int basePort, int sslPort, int numRacks, HardwareState hardwareState, JSONArray disks) throws JSONException {
    for (int i = dataNodeJsonArray.length(); i < dataNodeCount; ++i) {
      dataNodeJsonArray.put(getJsonDataNode(hostname, basePort + i, sslPort + i, i % numRacks, hardwareState, disks));
    }
  }

  /**
   * Generates an array of JSON data node objects.
   * The nodes at even indices in the array will have unique, defined rack IDs, while the ones
   * at odd indices will have undefined rack IDs.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *
   * @param dataNodeCount how many data nodes to generate
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   * @return a {@link JSONArray} of nodes
   * @throws JSONException
   */
  public static JSONArray getJsonArrayDataNodesPartiallyRackAware(int dataNodeCount, String hostname, int basePort,
      int sslPort, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      JSONObject jsonDataNode =
          (i % 2 == 0) ? getJsonDataNode(hostname, basePort + i, sslPort + i, i, hardwareState, disks)
              : getJsonDataNode(hostname, basePort + i, sslPort + i, hardwareState, disks);
      jsonArray.put(jsonDataNode);
    }
    return jsonArray;
  }

  // Does not increment basePort for each data node...
  public static JSONArray getJsonArrayDuplicateDataNodes(int dataNodeCount, String hostname, int basePort,
      HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort, hardwareState, disks));
    }
    return jsonArray;
  }

  // Does not increment basePort and sslPort for each data node...
  public static JSONArray getJsonArrayDuplicateDataNodes(int dataNodeCount, String hostname, int basePort, int sslPort,
      HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort, sslPort, hardwareState, disks));
    }
    return jsonArray;
  }

  public static JSONObject getJsonDatacenter(String name, byte id, JSONArray dataNodes) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("name", name);
    jsonObject.put("id", id);
    jsonObject.put("dataNodes", dataNodes);
    return jsonObject;
  }

  public static JSONArray getJsonArrayDatacenters(List<String> names, List<JSONArray> dataNodes) throws JSONException {
    if (names.size() != dataNodes.size()) {
      throw new IllegalArgumentException("Size of lists do not match");
    }

    JSONArray datacenterJSONArray = new JSONArray();
    for (int i = 0; i < names.size(); i++) {
      datacenterJSONArray.put(getJsonDatacenter(names.get(i), (byte) i, dataNodes.get(i)));
    }
    return datacenterJSONArray;
  }

  public static final long defaultHardwareLayoutVersion = 321;

  public static JSONObject getJsonHardwareLayout(String clusterName, JSONArray datacenters) throws JSONException {
    return getJsonHardwareLayout(clusterName, defaultHardwareLayoutVersion, datacenters);
  }

  public static JSONObject getJsonHardwareLayout(String clusterName, long version, JSONArray datacenters)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("clusterName", clusterName);
    jsonObject.put("version", version);
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

  public static JSONObject getJsonPartition(long id, PartitionState partitionState, long replicaCapacityInBytes,
      JSONArray replicas) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("id", id);
    jsonObject.put("partitionState", partitionState);
    jsonObject.put("replicaCapacityInBytes", replicaCapacityInBytes);
    jsonObject.put("replicas", replicas);
    return jsonObject;
  }

  public static JSONArray getJsonPartitions(long partitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCountPerDc, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas =
          TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCountPerDc));
      jsonArray.put(getJsonPartition(i, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
    return jsonArray;
  }

  /**
   * Update the given JSONArray of partitions by adding new partitions to maintain the partitionCount, if the
   * partitionCount is greater.
   * @param jsonArray The {@link JSONArray} to update.
   * @param newPartitionCount The count of partitions to maintain.
   * @param partitionState The state in which the new partitions are to be added.
   * @param replicaCapacityInBytes The capacity with which the replicas need to be configured.
   * @param replicaCountPerDc The number of replicas for this partition in each and every datacenter.
   * @param testHardwareLayout The {@link TestHardwareLayout} containing the layout of disks and datanodes.
   * @throws JSONException if there is an exception parsing or constructing JSON.
   */
  static void updateJsonPartitions(JSONArray jsonArray, long newPartitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCountPerDc, TestHardwareLayout testHardwareLayout) throws JSONException {
    int currentPartitionCount = jsonArray.length();
    for (long i = currentPartitionCount; i < newPartitionCount; i++) {
      JSONArray jsonReplicas =
          TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCountPerDc));
      jsonArray.put(getJsonPartition(i, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
  }

  public static JSONArray getJsonDuplicatePartitions(long partitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCountPerDc, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas =
          TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCountPerDc));
      jsonArray.put(getJsonPartition(0, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONArray getJsonDuplicateReplicas(long partitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCountPerDc, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      ArrayList<Disk> disks = new ArrayList<Disk>(replicaCountPerDc);
      Disk randomDisk = testHardwareLayout.getRandomDisk();
      for (int j = 0; j < replicaCountPerDc; j++) {
        disks.add(randomDisk);
      }
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(disks);
      jsonArray.put(getJsonPartition(i, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONArray getJsonPartitionsWithBadIds(long partitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCount, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCount));
      jsonArray.put(getJsonPartition(i + 10, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONObject getJsonPartitionLayout(String clusterName, long version, long partitionCount,
      JSONArray partitions) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("clusterName", clusterName);
    jsonObject.put("version", version);
    jsonObject.put("partitionIdFactory", partitionCount);
    jsonObject.put("partitions", partitions);
    return jsonObject;
  }

  public static class TestHardwareLayout {
    private static final int DEFAULT_DISK_COUNT = 10; // per DataNode
    private static final long DEFAULT_DISK_CAPACITY_IN_BYTES = 1000 * 1024 * 1024 * 1024L;
    private static final int DEFAULT_DATA_NODE_COUNT = 4; // per Datacenter
    private static final int DEFAULT_DATACENTER_COUNT = 3;
    private static final int DEFAULT_BASE_PORT = 6666;
    private static final int DEFAULT_NUM_RACKS = 3;

    private long version;
    private int diskCount;
    private long diskCapacityInBytes;
    private int dataNodeCount;
    private int datacenterCount;
    private int basePort;
    private int numRacks;
    private boolean rackAware;
    private String clusterName;
    private List<JSONArray> datanodeJSONArrays;
    private Properties properties;

    private HardwareLayout hardwareLayout;

    protected JSONArray getDisks() throws JSONException {
      return getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
    }

    protected JSONArray getDataNodes(int basePort, int sslPort, JSONArray disks) throws JSONException {
      if (rackAware) {
        return getJsonArrayDataNodesRackAware(dataNodeCount, getLocalHost(), basePort, sslPort, numRacks,
            HardwareState.AVAILABLE, disks);
      }
      return getJsonArrayDataNodes(dataNodeCount, getLocalHost(), basePort, sslPort, HardwareState.AVAILABLE, disks);
    }

    protected void updateDataNodeJsonArray(JSONArray dataNodeJsonArray, int basePort, int sslPort, JSONArray disks)
        throws JSONException {
      if (rackAware) {
        updateJsonArrayDataNodesRackAware(dataNodeJsonArray, dataNodeCount, getLocalHost(), basePort, sslPort, numRacks,
            HardwareState.AVAILABLE, disks);
      } else {
        updateJsonArrayDataNodes(dataNodeJsonArray, dataNodeCount, getLocalHost(), basePort, sslPort,
            HardwareState.AVAILABLE, disks);
      }
    }

    protected JSONArray getDatacenters(boolean createNew) throws JSONException {
      List<String> names = new ArrayList<String>(datacenterCount);
      if (createNew) {
        datanodeJSONArrays = new ArrayList<JSONArray>(datacenterCount);
      }

      int curBasePort = basePort;
      int sslPort = curBasePort + 10000;
      for (int i = 0; i < datacenterCount; i++) {
        names.add(i, "DC" + i);
        if (createNew) {
          datanodeJSONArrays.add(i, getDataNodes(curBasePort, sslPort, getDisks()));
        } else {
          updateDataNodeJsonArray(datanodeJSONArrays.get(i), curBasePort, sslPort, getDisks());
        }
        curBasePort += dataNodeCount;
      }

      basePort += dataNodeCount;
      return TestUtils.getJsonArrayDatacenters(names, datanodeJSONArrays);
    }

    public TestHardwareLayout(String clusterName, int diskCount, long diskCapacityInBytes, int dataNodeCount,
        int datacenterCount, int basePort, int numRacks, boolean rackAware) throws JSONException {
      this.diskCount = diskCount;
      this.diskCapacityInBytes = diskCapacityInBytes;
      this.dataNodeCount = dataNodeCount;
      this.datacenterCount = datacenterCount;
      this.basePort = basePort;
      this.numRacks = numRacks;
      this.rackAware = rackAware;
      this.clusterName = clusterName;
      this.properties = new Properties();
      properties.setProperty("clustermap.cluster.name", "test");
      properties.setProperty("clustermap.datacenter.name", "dc1");
      properties.setProperty("clustermap.host.name", "localhost");
      this.hardwareLayout = new HardwareLayout(getJsonHardwareLayout(clusterName, getDatacenters(true)),
          new ClusterMapConfig(new VerifiableProperties(properties)));
    }

    void addNewDataNodes(int i) throws JSONException {
      this.dataNodeCount += i;
      this.hardwareLayout = new HardwareLayout(getJsonHardwareLayout(clusterName, getDatacenters(false)),
          new ClusterMapConfig(new VerifiableProperties(properties)));
    }

    /**
     * Construct a default hardware layout.
     *
     * @param clusterName the name of the cluster generated
     * @param rackAware {@code true} if the cluster should have defined rack IDs
     * @throws JSONException
     */
    public TestHardwareLayout(String clusterName, boolean rackAware) throws JSONException {
      this(clusterName, DEFAULT_DISK_COUNT, DEFAULT_DISK_CAPACITY_IN_BYTES, DEFAULT_DATA_NODE_COUNT,
          DEFAULT_DATACENTER_COUNT, DEFAULT_BASE_PORT, DEFAULT_NUM_RACKS, rackAware);
    }

    /**
     * Construct a default hardware layout without defined rack IDs.
     * @param clusterName the name of the cluster generated
     * @throws JSONException
     */
    public TestHardwareLayout(String clusterName) throws JSONException {
      this(clusterName, false);
    }

    public HardwareLayout getHardwareLayout() {
      return hardwareLayout;
    }

    public int getDiskCount() {
      return diskCount;
    }

    public long getDiskCapacityInBytes() {
      return diskCapacityInBytes;
    }

    public int getDataNodeCount() {
      return dataNodeCount;
    }

    public int getDatacenterCount() {
      return datacenterCount;
    }

    public boolean isRackAware() {
      return rackAware;
    }

    public Datacenter getRandomDatacenter() {
      if (hardwareLayout.getDatacenters().size() == 0) {
        return null;
      }
      return hardwareLayout.getDatacenters().get(new Random().nextInt(hardwareLayout.getDatacenters().size()));
    }

    public DataNode getRandomDataNode() {
      Datacenter datacenter = getRandomDatacenter();
      if (datacenter == null || datacenter.getDataNodes().size() == 0) {
        return null;
      }
      return datacenter.getDataNodes().get(new Random().nextInt(datacenter.getDataNodes().size()));
    }

    /**
     * Get given number of datanodes from each datacenter.
     * @param dataNodeCountPerDc number of datanodes to get from each datacenter.
     * @return a list of datanodes.
     */
    List<DataNode> getIndependentDataNodes(int dataNodeCountPerDc) {
      List<DataNode> dataNodesToReturn = new ArrayList<DataNode>();
      for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        List<DataNode> dataNodesInThisDc = new ArrayList<>();
        dataNodesInThisDc.addAll(datacenter.getDataNodes());
        if (dataNodeCountPerDc < 0 || dataNodeCountPerDc > dataNodesInThisDc.size()) {
          throw new IndexOutOfBoundsException("dataNodeCount out of bounds:" + dataNodeCountPerDc);
        }
        Collections.shuffle(dataNodesInThisDc);
        dataNodesToReturn.addAll(dataNodesInThisDc.subList(0, dataNodeCountPerDc));
      }
      return dataNodesToReturn;
    }

    public Disk getRandomDisk() {
      DataNode dataNode = getRandomDataNode();
      if (dataNode == null || dataNode.getDisks().size() == 0) {
        return null;
      }
      return dataNode.getDisks().get(new Random().nextInt(dataNode.getDisks().size()));
    }

    // Finds diskCount disks, each on distinct random datanodes.
    public List<Disk> getIndependentDisks(int diskCount) {
      List<DataNode> dataNodes = getIndependentDataNodes(diskCount);
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
      while (diskCount > 0) {
        disks.add(getRandomDisk());
        diskCount--;
      }

      return disks;
    }
  }

  public static class TestPartitionLayout {
    protected static final long defaultVersion = 2468;
    protected static final int defaultPartitionCount = 10;
    protected static final PartitionState defaultPartitionState = PartitionState.READ_WRITE;
    protected static final long defaultReplicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;
    protected static final int defaultReplicaCount = 3; // Per Partition

    protected int partitionCount;
    protected PartitionState partitionState;
    protected long replicaCapacityInBytes;
    protected int replicaCountPerDc;
    protected long dcCount;

    protected TestHardwareLayout testHardwareLayout;
    protected PartitionLayout partitionLayout;
    private JSONArray jsonPartitions;
    private long version;

    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      version = defaultVersion;
      jsonPartitions = getJsonPartitions(partitionCount, partitionState, replicaCapacityInBytes, replicaCountPerDc,
          testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
          jsonPartitions);
    }

    /**
     * Return the partition layout as a {@link JSONObject}
     * @return the partition layout as a {@link JSONObject}
     * @throws JSONException if there is an exception parsing or constructing JSON.
     */
    private JSONObject updateJsonPartitionLayout() throws JSONException {
      version += 1;
      updateJsonPartitions(jsonPartitions, partitionCount, partitionState, replicaCapacityInBytes, replicaCountPerDc,
          testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
          jsonPartitions);
    }

    public TestPartitionLayout(TestHardwareLayout testHardwareLayout, int partitionCount, PartitionState partitionState,
        long replicaCapacityInBytes, int replicaCountPerDc) throws JSONException {
      this.partitionCount = partitionCount;
      this.partitionState = partitionState;
      this.replicaCapacityInBytes = replicaCapacityInBytes;
      this.replicaCountPerDc = replicaCountPerDc;

      this.testHardwareLayout = testHardwareLayout;
      this.partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), makeJsonPartitionLayout());
      this.dcCount = testHardwareLayout.getHardwareLayout().getDatacenterCount();
    }

    public TestPartitionLayout(TestHardwareLayout testHardwareLayout) throws JSONException {
      this(testHardwareLayout, defaultPartitionCount, defaultPartitionState, defaultReplicaCapacityInBytes,
          defaultReplicaCount);
    }

    void addNewPartitions(int i) throws JSONException {
      this.partitionCount += i;
      this.partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), updateJsonPartitionLayout());
    }

    public PartitionLayout getPartitionLayout() {
      return partitionLayout;
    }

    public int getPartitionCount() {
      return partitionCount;
    }

    public int getTotalReplicaCount() {
      return replicaCountPerDc * (int) dcCount;
    }

    public int getReplicaCountPerDc() {
      return replicaCountPerDc;
    }

    public long getAllocatedRawCapacityInBytes() {
      return partitionCount * replicaCountPerDc * dcCount * replicaCapacityInBytes;
    }

    public long getAllocatedUsableCapacityInBytes() {
      return partitionCount * replicaCapacityInBytes;
    }

    public long countPartitionsInState(PartitionState partitionStateToCount) {
      if (partitionState == partitionStateToCount) {
        return partitionCount;
      }
      return 0;
    }
  }

  public static class TestPartitionLayoutWithDuplicatePartitions extends TestPartitionLayout {
    @Override
    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      JSONArray jsonPartitions =
          getJsonDuplicatePartitions(partitionCount, partitionState, replicaCapacityInBytes, replicaCountPerDc,
              testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), defaultVersion,
          partitionCount, jsonPartitions);
    }

    public TestPartitionLayoutWithDuplicatePartitions(TestHardwareLayout testHardwareLayout) throws JSONException {
      super(testHardwareLayout);
    }
  }

  public static class TestPartitionLayoutWithDuplicateReplicas extends TestPartitionLayout {
    @Override
    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      JSONArray jsonPartitions =
          getJsonDuplicateReplicas(partitionCount, partitionState, replicaCapacityInBytes, replicaCountPerDc,
              testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), defaultVersion,
          partitionCount, jsonPartitions);
    }

    public TestPartitionLayoutWithDuplicateReplicas(TestHardwareLayout testHardwareLayout) throws JSONException {
      super(testHardwareLayout);
    }
  }

  public static StaticClusterManager getTestClusterMap(int partitionCount, int replicaCountPerDatacenter,
      long replicaCapacityInBytes) throws JSONException {

    TestUtils.TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    StaticClusterManager clusterMapManager = new StaticClusterManager(partitionLayout, null, new MetricRegistry());
    List<PartitionId> allocatedPartitions;

    allocatedPartitions =
        clusterMapManager.allocatePartitions(partitionCount, replicaCountPerDatacenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 5);

    return clusterMapManager;
  }

  public static StaticClusterManager getTestClusterMap() throws JSONException {
    int numPartitions = 5;
    int replicaCountPerDatacenter = 2;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    return getTestClusterMap(numPartitions, replicaCountPerDatacenter, replicaCapacityInBytes);
  }

  /**
   * Construct a ZK layout JSON using predetermined information.
   * @return the constructed JSON.
   */
  public static JSONObject constructZkLayoutJSON(Collection<com.github.ambry.utils.TestUtils.ZkInfo> zkInfos)
      throws JSONException {
    JSONArray zkInfosJson = new JSONArray();
    for (com.github.ambry.utils.TestUtils.ZkInfo zkInfo : zkInfos) {
      JSONObject zkInfoJson = new JSONObject();
      zkInfoJson.put(ClusterMapUtils.DATACENTER_STR, zkInfo.getDcName());
      zkInfoJson.put(ClusterMapUtils.DATACENTER_ID_STR, zkInfo.getId());
      zkInfoJson.put(ClusterMapUtils.ZKCONNECTSTR_STR, "localhost:" + zkInfo.getPort());
      zkInfosJson.put(zkInfoJson);
    }
    return new JSONObject().put(ClusterMapUtils.ZKINFO_STR, zkInfosJson);
  }

  /**
   * Construct a {@link TestHardwareLayout}
   * @return return the constructed layout.
   */
  static TestHardwareLayout constructInitialHardwareLayoutJSON(String clusterName) throws JSONException {
    return new TestHardwareLayout(clusterName, 6, 100L * 1024 * 1024 * 1024, 6, 2, 18088, 20, false);
  }

  /**
   * Construct a {@link TestPartitionLayout}
   * @return return the constructed layout.
   */
  static TestPartitionLayout constructInitialPartitionLayoutJSON(TestHardwareLayout testHardwareLayout,
      int partitionCount) throws JSONException {
    return new TestPartitionLayout(testHardwareLayout, partitionCount, PartitionState.READ_WRITE, 1024L * 1024 * 1024,
        3);
  }

  /**
   * For use when the the actual values in {@link ClusterMapConfig} are unimportant.
   * @return a {@link ClusterMapConfig} with some default values.
   */
  static ClusterMapConfig getDummyConfig() {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "cluster");
    props.setProperty("clustermap.datacenter.name", "");
    return new ClusterMapConfig(new VerifiableProperties(props));
  }
}

