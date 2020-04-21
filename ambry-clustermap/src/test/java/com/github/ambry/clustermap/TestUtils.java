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
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.ServerErrorCode;
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

  static final String DEFAULT_PARTITION_CLASS = "defaultPartitionClass";
  static final int DEFAULT_XID = 64;

  enum ReplicaStateType {
    // @formatter:off
    SealedState,
    StoppedState
    // @formatter:om
  }

  /**
   * Resource state associated with datanode, disk and replica.
   */
  enum ResourceState {
    // @formatter:off
    Node_Up,
    Node_Down,
    Disk_Up,
    Disk_Down,
    Replica_Up,
    Replica_Down
    // @formatter:on
  }

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
    jsonObject.put("hardwareState", hardwareState.name());
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
    jsonObject.put("xid", DEFAULT_XID);
    jsonObject.put("hardwareState", hardwareState.name());
    jsonObject.put("disks", disks);
    return jsonObject;
  }

  public static JSONObject getJsonDataNode(String hostname, int port, int sslPort, int http2Port,
      HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("sslport", sslPort);
    jsonObject.put("http2port", http2Port);
    jsonObject.put("xid", DEFAULT_XID);
    jsonObject.put("hardwareState", hardwareState.name());
    jsonObject.put("disks", disks);
    return jsonObject;
  }

  /**
   * Generate a JSON data node object with a defined {@code rackId}.
   *
   * @param hostname the hostname for the node
   * @param port the plaintext port number for the node
   * @param sslPort the ssl port number for the node
   * @param http2Port the http2 port number for the node
   * @param rackId the rack ID for the node
   * @param xid the xid for the node
   * @param disks an array of disks belonging to the node
   * @param hardwareState A {@link HardwareState} value for the node
   * @return a {@link JSONObject) representing the node with the properties passed into the function
   * @throws JSONException
   */
  public static JSONObject getJsonDataNode(String hostname, int port, int sslPort, int http2Port, long rackId, long xid,
      JSONArray disks, HardwareState hardwareState) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("hostname", hostname);
    jsonObject.put("port", port);
    jsonObject.put("sslport", sslPort);
    jsonObject.put("http2port", http2Port);
    jsonObject.put("rackId", Long.toString(rackId));
    jsonObject.put("xid", xid);
    jsonObject.put("hardwareState", hardwareState.name());
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
   * @param http2Port the starting HTTP2 port number for nodes generated
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   * @return a {@link JSONArray} of nodes
   */
  static JSONArray getJsonArrayDataNodes(int dataNodeCount, String hostname, int basePort, int sslPort, int http2Port,
      HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort + i, sslPort + i, http2Port + i, hardwareState, disks));
    }
    return jsonArray;
  }

  /**
   * Updates the given array of JSON data node objects by adding new entries to reach the total datanode count.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *  @param dataNodeJsonArray the datanode JSONArray to update.
   * @param dataNodeCount how many total datanode entries the JSONArray should have.
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param http2Port the starting HTTP2 port number for nodes generated
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   */
  private static void updateJsonArrayDataNodes(JSONArray dataNodeJsonArray, int dataNodeCount, String hostname,
      int basePort, int sslPort, int http2Port, HardwareState hardwareState, JSONArray disks) throws JSONException {
    for (int i = dataNodeJsonArray.length(); i < dataNodeCount; ++i) {
      dataNodeJsonArray.put(getJsonDataNode(hostname, basePort + i, sslPort + i, http2Port + i, hardwareState, disks));
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
   * @param http2Port the starting HTTP2 port number for nodes generated
   * @param numRacks how many distinct racks the data nodes should use
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @param disks a {@link JSONArray} of disks for each node
   * @return a {@link JSONArray} of nodes
   */
  static JSONArray getJsonArrayDataNodesRackAware(int dataNodeCount, String hostname, int basePort, int sslPort,
      int http2Port, int numRacks, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(
          getJsonDataNode(hostname, basePort + i, sslPort + i, http2Port + i, i % numRacks, DEFAULT_XID, disks,
              hardwareState));
    }
    return jsonArray;
  }

  /**
   * Updates the given array of JSON data node objects by adding new entries to reach the total datanode count.
   * Increments basePort and sslPort for each node to ensure unique DataNode given same hostname.
   *  @param dataNodeJsonArray the datanode JSONArray to update.
   * @param dataNodeCount how many total datanode entries the JSONArray should have.
   * @param hostname the hostname for each node in the array
   * @param basePort the starting standard port number for nodes generated
   * @param sslPort the starting SSL port number for nodes generated
   * @param http2Port the starting HTTP2 port number for nodes generated
   * @param numRacks how many distinct racks the data nodes should use
   * @param disks a {@link JSONArray} of disks for each node
   * @param hardwareState a {@link HardwareLayout} value for each node
   */
  private static void updateJsonArrayDataNodesRackAware(JSONArray dataNodeJsonArray, int dataNodeCount, String hostname,
      int basePort, int sslPort, int http2Port, int numRacks, JSONArray disks, HardwareState hardwareState)
      throws JSONException {
    for (int i = dataNodeJsonArray.length(); i < dataNodeCount; ++i) {
      dataNodeJsonArray.put(
          getJsonDataNode(hostname, basePort + i, sslPort + i, http2Port + i, i % numRacks, DEFAULT_XID, disks,
              hardwareState));
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
   * @param http2Port the starting HTTP2 port number for nodes generated
   * @param disks a {@link JSONArray} of disks for each node
   * @param hardwareState a {@link HardwareLayout} value for each node
   * @return a {@link JSONArray} of nodes
   * @throws JSONException
   */
  public static JSONArray getJsonArrayDataNodesPartiallyRackAware(int dataNodeCount, String hostname, int basePort,
      int sslPort, int http2Port, JSONArray disks, HardwareState hardwareState) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      JSONObject jsonDataNode =
          (i % 2 == 0) ? getJsonDataNode(hostname, basePort + i, sslPort + i, http2Port + i, i, DEFAULT_XID, disks,
              hardwareState)
              : getJsonDataNode(hostname, basePort + i, sslPort + i, http2Port + i, hardwareState, disks);
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
      int http2Port, HardwareState hardwareState, JSONArray disks) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataNodeCount; ++i) {
      jsonArray.put(getJsonDataNode(hostname, basePort, sslPort, http2Port, hardwareState, disks));
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

  public static JSONObject getJsonPartition(long id, String partitionClass, PartitionState partitionState,
      long replicaCapacityInBytes, JSONArray replicas) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("id", id);
    jsonObject.put("partitionClass", partitionClass);
    jsonObject.put("partitionState", partitionState.name());
    jsonObject.put("replicaCapacityInBytes", replicaCapacityInBytes);
    jsonObject.put("replicas", replicas);
    return jsonObject;
  }

  public static JSONArray getJsonPartitions(long partitionCount, String partitionClass, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCountPerDc, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas =
          TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCountPerDc));
      jsonArray.put(getJsonPartition(i, partitionClass, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
    return jsonArray;
  }

  /**
   * Update the given JSONArray of partitions by adding new partitions to maintain the partitionCount, if the
   * partitionCount is greater.
   * @param jsonArray The {@link JSONArray} to update.
   * @param newPartitionCount The count of partitions to maintain.
   * @param partitionClass The class to which the new partitions are to be added.
   * @param partitionState The state in which the new partitions are to be added.
   * @param replicaCapacityInBytes The capacity with which the replicas need to be configured.
   * @param replicaCountPerDc The number of replicas for this partition in each and every datacenter.
   * @param testHardwareLayout The {@link TestHardwareLayout} containing the layout of disks and datanodes.
   * @throws JSONException if there is an exception parsing or constructing JSON.
   */
  static void updateJsonPartitions(JSONArray jsonArray, long newPartitionCount, String partitionClass,
      PartitionState partitionState, long replicaCapacityInBytes, int replicaCountPerDc,
      TestHardwareLayout testHardwareLayout) throws JSONException {
    int currentPartitionCount = jsonArray.length();
    for (long i = currentPartitionCount; i < newPartitionCount; i++) {
      JSONArray jsonReplicas =
          TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCountPerDc));
      jsonArray.put(getJsonPartition(i, partitionClass, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
  }

  public static JSONArray getJsonDuplicatePartitions(long partitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCountPerDc, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas =
          TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCountPerDc));
      jsonArray.put(getJsonPartition(0, DEFAULT_PARTITION_CLASS, partitionState, replicaCapacityInBytes, jsonReplicas));
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
      jsonArray.put(getJsonPartition(i, DEFAULT_PARTITION_CLASS, partitionState, replicaCapacityInBytes, jsonReplicas));
    }
    return jsonArray;
  }

  public static JSONArray getJsonPartitionsWithBadIds(long partitionCount, PartitionState partitionState,
      long replicaCapacityInBytes, int replicaCount, TestHardwareLayout testHardwareLayout) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < partitionCount; i++) {
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(testHardwareLayout.getIndependentDisks(replicaCount));
      jsonArray.put(
          getJsonPartition(i + 10, DEFAULT_PARTITION_CLASS, partitionState, replicaCapacityInBytes, jsonReplicas));
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

  /**
   * Checks that each of the partitions in {@code partitionIds} belongs to one of the ranges defined by
   * {@code checkParamsList}.
   * @param partitionIds the partitions to check.
   * @param checkParamsList the list of ranges/types that the partitions in {@code partitionIds} can belong to.
   */
  static void checkReturnedPartitions(List<? extends PartitionId> partitionIds,
      List<PartitionRangeCheckParams> checkParamsList) {
    int expectedPartitionCount = 0;
    for (PartitionRangeCheckParams checkParams : checkParamsList) {
      expectedPartitionCount += checkParams.count;
    }
    assertEquals("Returned partition count not as expected", expectedPartitionCount, partitionIds.size());
    for (PartitionId partitionId : partitionIds) {
      int partId = Integer.parseInt(partitionId.toPathString());
      PartitionRangeCheckParams selectedCheckParams = null;
      for (PartitionRangeCheckParams checkParams : checkParamsList) {
        if (partId >= checkParams.rangeStart && partId <= checkParams.rangeEnd) {
          selectedCheckParams = checkParams;
          break;
        }
      }
      assertNotNull("Partition ID [" + partId + "] is not in any of the specified ranges", selectedCheckParams);
      assertEquals("Partition class not as expected", selectedCheckParams.expectedClass,
          partitionId.getPartitionClass());
      assertEquals("Partition state not as expected", selectedCheckParams.expectedState,
          partitionId.getPartitionState());
    }
  }

  /**
   * The helper method sets up initial states for datanode, disk and replica. Then it triggers specified server event and
   * verifies the states of datanode, disk and replica are expected after event.
   * @param clusterManager the {@link ClusterMap} to use.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param initialStates the initial states for datanode, disk and replica (default order).
   * @param serverErrorCode the {@link ServerErrorCode} received for mocking event.
   * @param expectedStates the expected states for datanode, disk and replica (default order).
   */
  static void mockServerEventsAndVerify(ClusterMap clusterManager, ClusterMapConfig clusterMapConfig,
      ResourceState[] initialStates, ServerErrorCode serverErrorCode, ResourceState[] expectedStates) {
    ResponseHandler handler = new ResponseHandler(clusterManager);
    // choose a disk backed replica
    ReplicaId replica = clusterManager.getWritablePartitionIds(null)
        .get(0)
        .getReplicaIds()
        .stream()
        .filter(replicaId -> replicaId.getReplicaType() != ReplicaType.CLOUD_BACKED)
        .findFirst()
        .get();
    DataNodeId dataNode = replica.getDataNodeId();
    assertTrue(clusterManager.getReplicaIds(dataNode).contains(replica));
    DiskId disk = replica.getDiskId();

    // Verify that everything is up in the beginning.
    assertFalse(replica.isDown());
    assertEquals(HardwareState.AVAILABLE, dataNode.getState());
    assertEquals(HardwareState.AVAILABLE, disk.getState());

    // Mock initial states for node, disk and replica
    if (initialStates[0] == ResourceState.Node_Down) {
      for (int i = 0; i < clusterMapConfig.clusterMapFixedTimeoutDatanodeErrorThreshold; i++) {
        clusterManager.onReplicaEvent(replica, ReplicaEventType.Node_Timeout);
      }
    }
    if (initialStates[1] == ResourceState.Disk_Down) {
      for (int i = 0; i < clusterMapConfig.clusterMapFixedTimeoutDiskErrorThreshold; i++) {
        clusterManager.onReplicaEvent(replica, ReplicaEventType.Disk_Error);
      }
    }
    if (initialStates[2] == ResourceState.Replica_Down) {
      for (int i = 0; i < clusterMapConfig.clusterMapFixedTimeoutReplicaErrorThreshold; i++) {
        clusterManager.onReplicaEvent(replica, ReplicaEventType.Replica_Unavailable);
      }
    }

    // Make sure node, disk and replica match specified initial states
    if (dataNode.getState() == HardwareState.AVAILABLE && disk.getState() == HardwareState.AVAILABLE) {
      // Since replica.isDown() will check the state of disk, if we try to mock disk is down and replica is up, we should
      // skip this check for initial state. Only when node and disk are up, we check the initial state of replica.
      assertEquals(initialStates[2], replica.isDown() ? ResourceState.Replica_Down : ResourceState.Replica_Up);
    }
    if (dataNode.getState() == HardwareState.AVAILABLE) {
      assertEquals(initialStates[1],
          disk.getState() == HardwareState.UNAVAILABLE ? ResourceState.Disk_Down : ResourceState.Disk_Up);
    }
    assertEquals(initialStates[0],
        dataNode.getState() == HardwareState.UNAVAILABLE ? ResourceState.Node_Down : ResourceState.Node_Up);

    // Trigger server event
    handler.onEvent(replica, serverErrorCode);

    // Verify node, disk and replica match expected states after server event
    assertEquals(expectedStates[2], replica.isDown() ? ResourceState.Replica_Down : ResourceState.Replica_Up);
    assertEquals(expectedStates[1],
        disk.getState() == HardwareState.UNAVAILABLE ? ResourceState.Disk_Down : ResourceState.Disk_Up);
    assertEquals(expectedStates[0],
        dataNode.getState() == HardwareState.UNAVAILABLE ? ResourceState.Node_Down : ResourceState.Node_Up);
  }

  /**
   * Class that represents the params to check for a given partition.
   */
  static class PartitionRangeCheckParams {
    final int rangeStart;
    final int rangeEnd;
    final int count;
    final String expectedClass;
    final PartitionState expectedState;

    /**
     * @param rangeStart start of the range of partitions.
     * @param count the number of partitions in the range.
     * @param expectedClass the expected class of all of the partitions
     * @param expectedState the expected state of all of the partitions
     */
    PartitionRangeCheckParams(int rangeStart, int count, String expectedClass, PartitionState expectedState) {
      this.rangeStart = rangeStart;
      this.rangeEnd = rangeStart + count - 1;
      this.count = count;
      this.expectedClass = expectedClass;
      this.expectedState = expectedState;
    }
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

    ClusterMapConfig clusterMapConfig;

    protected JSONArray getDisks() throws JSONException {
      return getJsonArrayDisks(diskCount, "/mnt", HardwareState.AVAILABLE, diskCapacityInBytes);
    }

    protected JSONArray getDataNodes(int basePort, int sslPort, int http2Port, JSONArray disks) throws JSONException {
      if (rackAware) {
        return getJsonArrayDataNodesRackAware(dataNodeCount, getLocalHost(), basePort, sslPort, http2Port, numRacks,
            HardwareState.AVAILABLE, disks);
      }
      return getJsonArrayDataNodes(dataNodeCount, getLocalHost(), basePort, sslPort, http2Port, HardwareState.AVAILABLE,
          disks);
    }

    protected void updateDataNodeJsonArray(JSONArray dataNodeJsonArray, int basePort, int sslPort, int http2Port,
        JSONArray disks) throws JSONException {
      if (rackAware) {
        updateJsonArrayDataNodesRackAware(dataNodeJsonArray, dataNodeCount, getLocalHost(), basePort, sslPort,
            http2Port, numRacks, disks, HardwareState.AVAILABLE);
      } else {
        updateJsonArrayDataNodes(dataNodeJsonArray, dataNodeCount, getLocalHost(), basePort, sslPort, http2Port,
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
      int http2Port = sslPort + 10000;
      for (int i = 0; i < datacenterCount; i++) {
        names.add(i, "DC" + i);
        if (createNew) {
          datanodeJSONArrays.add(i, getDataNodes(curBasePort, sslPort, http2Port, getDisks()));
        } else {
          updateDataNodeJsonArray(datanodeJSONArrays.get(i), curBasePort, sslPort, http2Port, getDisks());
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
      clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
      this.hardwareLayout =
          new HardwareLayout(getJsonHardwareLayout(clusterName, getDatacenters(true)), clusterMapConfig);
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

    /**
     * @return all existing nodes in hardware layout when this method is being invoked.
     */
    public List<DataNode> getAllExistingDataNodes() {
      List<DataNode> dataNodes = new ArrayList<>();
      for (Datacenter dcObj : hardwareLayout.getDatacenters()) {
        dataNodes.addAll(dcObj.getDataNodes());
      }
      return Collections.unmodifiableList(dataNodes);
    }

    public Datacenter getRandomDatacenter() {
      if (hardwareLayout.getDatacenters().size() == 0) {
        return null;
      }
      return hardwareLayout.getDatacenters().get(new Random().nextInt(hardwareLayout.getDatacenters().size()));
    }

    public DataNode getRandomDataNodeFromDc(String dc) {
      Datacenter datacenter = null;
      for (Datacenter dcObj : hardwareLayout.getDatacenters()) {
        if (dcObj.getName().equals(dc)) {
          datacenter = dcObj;
          break;
        }
      }
      if (datacenter == null || datacenter.getDataNodes().size() == 0) {
        return null;
      }
      return datacenter.getDataNodes().get(new Random().nextInt(datacenter.getDataNodes().size()));
    }

    /**
     * Get all data nodes that are from given data center.
     * @param dc the data center to get nodes from.
     * @return a list of nodes from given data center.
     */
    public List<DataNode> getAllDataNodesFromDc(String dc) {
      List<DataNode> dataNodes = new ArrayList<>();
      for (Datacenter dcObj : hardwareLayout.getDatacenters()) {
        if (dcObj.getName().equals(dc)) {
          dataNodes.addAll(dcObj.getDataNodes());
          break;
        }
      }
      return dataNodes;
    }

    public DataNode getRandomDataNode() {
      return getRandomDataNodeFromDc(getRandomDatacenter().getName());
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
    private ClusterMapConfig clusterMapConfig;

    protected JSONObject makeJsonPartitionLayout() throws JSONException {
      return makeJsonPartitionLayout(DEFAULT_PARTITION_CLASS);
    }

    protected JSONObject makeJsonPartitionLayout(String partitionClass) throws JSONException {
      version = defaultVersion;
      jsonPartitions =
          getJsonPartitions(partitionCount, partitionClass, partitionState, replicaCapacityInBytes, replicaCountPerDc,
              testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
          jsonPartitions);
    }

    /**
     * Return the partition layout as a {@link JSONObject}
     * @param partitionClass the partition class that these partitions must belong to
     * @param partitionState the state of the newly created partitions
     * @return the partition layout as a {@link JSONObject}
     * @throws JSONException if there is an exception parsing or constructing JSON.
     */
    private JSONObject updateJsonPartitionLayout(String partitionClass, PartitionState partitionState)
        throws JSONException {
      version += 1;
      updateJsonPartitions(jsonPartitions, partitionCount, partitionClass, partitionState, replicaCapacityInBytes,
          replicaCountPerDc, testHardwareLayout);
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
          jsonPartitions);
    }

    public TestPartitionLayout(TestHardwareLayout testHardwareLayout, int partitionCount, PartitionState partitionState,
        long replicaCapacityInBytes, int replicaCountPerDc, String localDc) throws JSONException {
      this.partitionCount = partitionCount;
      this.partitionState = partitionState;
      this.replicaCapacityInBytes = replicaCapacityInBytes;
      this.replicaCountPerDc = replicaCountPerDc;

      this.testHardwareLayout = testHardwareLayout;
      this.dcCount = testHardwareLayout.getHardwareLayout().getDatacenterCount();
      Properties props = new Properties();
      props.setProperty("clustermap.host.name", "localhost");
      props.setProperty("clustermap.cluster.name", "cluster");
      props.setProperty("clustermap.datacenter.name", localDc == null ? "" : localDc);
      clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
      this.partitionLayout =
          new PartitionLayout(testHardwareLayout.getHardwareLayout(), makeJsonPartitionLayout(), clusterMapConfig);
    }

    public TestPartitionLayout(TestHardwareLayout testHardwareLayout, String localDc) throws JSONException {
      this(testHardwareLayout, defaultPartitionCount, defaultPartitionState, defaultReplicaCapacityInBytes,
          defaultReplicaCount, localDc);
    }

    void addNewPartitions(int i, String partitionClass, PartitionState partitionState, String localDc)
        throws JSONException {
      this.partitionCount += i;
      this.partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(),
          updateJsonPartitionLayout(partitionClass, partitionState), clusterMapConfig);
    }

    void addNewPartition(TestHardwareLayout testHardwareLayout, List<DataNode> dataNodes, String partitionClass) {
      partitionCount += 1;
      partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(),
          addPartitionAndUpdateJsonPartitionLayout(partitionClass, dataNodes), clusterMapConfig);
    }

    void removeReplicaFromPartition(Replica replicaToRemove) {
      version += 1;
      String partitionName = replicaToRemove.getPartitionId().toPathString();
      int index;
      for (index = 0; index < jsonPartitions.length(); ++index) {
        String partitionId = String.valueOf(((JSONObject) jsonPartitions.get(index)).get("id"));
        if (partitionId.equals(partitionName)) {
          break;
        }
      }
      JSONObject jsonPartition = (JSONObject) jsonPartitions.get(index);
      JSONArray jsonReplicas = (JSONArray) jsonPartition.get("replicas");
      int replicaIdx;
      DataNode targetNode = (DataNode) replicaToRemove.getDataNodeId();
      for (replicaIdx = 0; replicaIdx < jsonReplicas.length(); ++replicaIdx) {
        JSONObject jsonReplica = (JSONObject) jsonReplicas.get(replicaIdx);
        String hostname = (String) jsonReplica.get("hostname");
        int port = (int) jsonReplica.get("port");
        if (hostname.equals(targetNode.getHostname()) && port == targetNode.getPort()) {
          break;
        }
      }
      // remove given replica from replicas
      jsonReplicas.remove(replicaIdx);
      jsonPartition.put("replicas", jsonReplicas);
      jsonPartitions.put(index, jsonPartition);
      JSONObject jsonPartitionLayout =
          getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
              jsonPartitions);
      this.partitionLayout =
          new PartitionLayout(testHardwareLayout.getHardwareLayout(), jsonPartitionLayout, clusterMapConfig);
    }

    void addReplicaToPartition(DataNode newReplicaNode, Partition partition) {
      version += 1;
      int index;
      for (index = 0; index < jsonPartitions.length(); ++index) {
        String partitionId = String.valueOf(((JSONObject) jsonPartitions.get(index)).get("id"));
        if (partitionId.equals(partition.toPathString())) {
          break;
        }
      }
      JSONObject jsonPartition = (JSONObject) jsonPartitions.get(index);
      JSONArray jsonReplicas = (JSONArray) jsonPartition.get("replicas");
      List<Disk> disks = newReplicaNode.getDisks();
      jsonReplicas.put(getJsonReplica(disks.get((new Random()).nextInt(disks.size()))));
      jsonPartition.put("replicas", jsonReplicas);
      jsonPartitions.put(index, jsonPartition);
      JSONObject jsonPartitionLayout =
          getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
              jsonPartitions);
      this.partitionLayout =
          new PartitionLayout(testHardwareLayout.getHardwareLayout(), jsonPartitionLayout, clusterMapConfig);
    }

    private JSONObject addPartitionAndUpdateJsonPartitionLayout(String partitionClass,
        List<DataNode> nodesToHostNewPartition) {
      version += 1;
      int nextPartitionIndex = jsonPartitions.length();
      List<Disk> disksToPlaceNewPartition = new ArrayList<>();
      Random random = new Random();
      nodesToHostNewPartition.forEach(
          node -> disksToPlaceNewPartition.add(node.getDisks().get(random.nextInt(node.getDisks().size()))));
      JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(disksToPlaceNewPartition);
      jsonPartitions.put(
          getJsonPartition(nextPartitionIndex, partitionClass, partitionState, replicaCapacityInBytes, jsonReplicas));
      return getJsonPartitionLayout(testHardwareLayout.getHardwareLayout().getClusterName(), version, partitionCount,
          jsonPartitions);
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
      super(testHardwareLayout, null);
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
      super(testHardwareLayout, null);
    }
  }

  public static StaticClusterManager getTestClusterMap(int partitionCount, int replicaCountPerDatacenter,
      long replicaCapacityInBytes, ClusterMapConfig clusterMapConfig) throws JSONException {

    TestUtils.TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), clusterMapConfig);

    StaticClusterManager clusterMapManager = new StaticClusterManager(partitionLayout, null, new MetricRegistry());
    List<PartitionId> allocatedPartitions;

    allocatedPartitions = clusterMapManager.allocatePartitions(partitionCount, MockClusterMap.DEFAULT_PARTITION_CLASS,
        replicaCountPerDatacenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 5);

    return clusterMapManager;
  }

  public static StaticClusterManager getTestClusterMap(ClusterMapConfig clusterMapConfig) throws JSONException {
    int numPartitions = 5;
    int replicaCountPerDatacenter = 2;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    return getTestClusterMap(numPartitions, replicaCountPerDatacenter, replicaCapacityInBytes, clusterMapConfig);
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
      zkInfoJson.put(ClusterMapUtils.ZKCONNECT_STR, "localhost:" + zkInfo.getPort());
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
      int partitionCount, String localDc) throws JSONException {
    return new TestPartitionLayout(testHardwareLayout, partitionCount, PartitionState.READ_WRITE, 1024L * 1024 * 1024,
        3, localDc);
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
    props.setProperty("clustermap.resolve.hostnames", "false");
    return new ClusterMapConfig(new VerifiableProperties(props));
  }
}

