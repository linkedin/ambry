/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Mock cluster map for unit tests.
 */
public class MockClusterMap implements ClusterMap {
  public static final String DEFAULT_PARTITION_CLASS = "defaultPartitionClass";
  public static final String SPECIAL_PARTITION_CLASS = "specialPartitionClass";

  protected final boolean enableSSLPorts;
  protected final Map<Long, PartitionId> partitions;
  protected final List<MockDataNodeId> dataNodes;
  protected final int numMountPointsPerNode;
  protected final List<String> dataCentersInClusterMap = new ArrayList<>();
  private final ClusterMapUtils.PartitionSelectionHelper partitionSelectionHelper;
  protected boolean partitionsUnavailable = false;
  private boolean createNewRegistry = true;
  private MetricRegistry metricRegistry;
  // needs to be thread safe
  private List<String> lastRequestedPartitionClasses = new CopyOnWriteArrayList<>();

  // allow this to be changed to support some tests
  private String localDatacenterName;

  private final MockPartitionId specialPartition;

  /**
   * The default constructor sets up a 9 node cluster with 3 mount points in each, with 3 default partitions/replicas
   * per mount point. It will also add replicas for a "special" partition. The distribution of these replicas will be
   * 3 in the chosen "local" datacenter and 2 everywhere else. This will amount to a total of 9 or 10 replicas per node
   * and 88 replicas across the cluster.
   *
   * If this cluster map is going to be used to start a cluster, use it judiciously to avoid resource consumption issues
   * on the test machine.
   */
  public MockClusterMap() throws IOException {
    this(false, 9, 3, 3, false);
  }

  /**
   * Creates and returns a mock cluster map.
   * <p>
   * The parameters to this method determine the number of mock datanodes that will be created in the cluster, the
   * number of mount points that will be created on each of these mock datanodes, and the number of "default" stores
   * that will be created on each mount point and whether there will be stores that host a "special" partition. Stores
   * correspond to replicas, so the number of stores also determines the number of replicas that will be created on a
   * node (which is going to be the number of mount points per node multiplied by the number of default stores per mount
   * point and possibly an extra store if a replica of the special partition resides on the node). Every default
   * partition is available on every node however the special partition is only available on a subset. These parameters
   * determine resource consumption so tests that start a cluster or a server should therefore keep these parameters to
   * the minimum required for testing intended functionality correctly (however, tests that only create the
   * MockClusterMap but do not start the cluster or servers will not end up using any significant resources, and should
   * be fine).
   * <p/>
   * The "special" partition will be created only if
   * 1. The parameter {@code createOnlyDefaultPartitionClass} is {@code false}.
   * 2. {@code numNodes} >= 4
   * 3. There are at least 3 nodes in the designated "local" datacenter.
   * To determine whether a "special" partition was created, use the function {@link #getSpecialPartition()}.
   * The "special" partition has 3 replicas in the designated "local" datacenter and 2 replicas in all other datacenters
   * @param numNodes number of mock datanodes that will be created (every 3 of which will be put in a separate
   *                 datacenter).
   * @param numMountPointsPerNode number of mount points (mocking disks) that will be created in each datanode.
   * @param numDefaultStoresPerMountPoint the number of stores that will be created on each mount point.
   * @param createOnlyDefaultPartitionClass if {@code true}, does not attempt to create the "special" partition. If
   *                                        {@code false}, attempts to do so. See javadoc of function for more details
   */
  public MockClusterMap(boolean enableSSLPorts, int numNodes, int numMountPointsPerNode,
      int numDefaultStoresPerMountPoint, boolean createOnlyDefaultPartitionClass) throws IOException {
    this.enableSSLPorts = enableSSLPorts;
    this.numMountPointsPerNode = numMountPointsPerNode;
    dataNodes = new ArrayList<MockDataNodeId>(numNodes);
    //Every group of 3 nodes will be put in the same DC.
    int dcIndex = 0;
    String dcName = null;
    int currentPlainTextPort = 62000;
    int currentSSLPort = 63000;
    Map<String, List<MockDataNodeId>> dcToDataNodes = new HashMap<>();
    for (int i = 0; i < numNodes; i++) {
      if (i % 3 == 0) {
        dcIndex++;
        dcName = "DC" + dcIndex;
        dataCentersInClusterMap.add(dcName);
      }
      MockDataNodeId dataNodeId;
      if (enableSSLPorts) {
        dataNodeId = createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), dcName);
      } else {
        dataNodeId = createDataNode(getListOfPorts(currentPlainTextPort++), dcName);
      }
      dataNodes.add(dataNodeId);
      dcToDataNodes.computeIfAbsent(dcName, name -> new ArrayList<>()).add(dataNodeId);
      localDatacenterName = dcName;
    }
    partitions = new HashMap<Long, PartitionId>();

    // create partitions
    long partitionId = 0;
    for (int i = 0; i < dataNodes.get(0).getMountPaths().size(); i++) {
      for (int j = 0; j < numDefaultStoresPerMountPoint; j++) {
        PartitionId id = new MockPartitionId(partitionId, DEFAULT_PARTITION_CLASS, dataNodes, i);
        partitions.put(partitionId, id);
        partitionId++;
      }
    }
    if (!createOnlyDefaultPartitionClass && numNodes >= 4 && dcToDataNodes.get(localDatacenterName).size() >= 3) {
      // create one "special partition" that has 3 replicas in the local datacenter (as configured on startup) and
      // 2 everywhere else
      List<MockDataNodeId> nodeIds = new ArrayList<>();
      dcToDataNodes.forEach(
          (s, mockDataNodeIds) -> nodeIds.addAll(mockDataNodeIds.subList(0, localDatacenterName.equals(s) ? 3 : 2)));
      MockPartitionId id = new MockPartitionId(partitionId, SPECIAL_PARTITION_CLASS, nodeIds, 0);
      partitions.put(partitionId, id);
      specialPartition = id;
    } else {
      specialPartition = null;
    }
    partitionSelectionHelper = new ClusterMapUtils.PartitionSelectionHelper(partitions.values(), localDatacenterName);
  }

  protected ArrayList<Port> getListOfPorts(int port) {
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(port, PortType.PLAINTEXT));
    return ports;
  }

  protected ArrayList<Port> getListOfPorts(int port, int sslPort) {
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(port, PortType.PLAINTEXT));
    ports.add(new Port(sslPort, PortType.SSL));
    return ports;
  }

  protected int getPlainTextPort(ArrayList<Port> ports) {
    for (Port port : ports) {
      if (port.getPortType() == PortType.PLAINTEXT) {
        return port.getPort();
      }
    }
    throw new IllegalArgumentException("No PlainText port found ");
  }

  protected MockDataNodeId createDataNode(ArrayList<Port> ports, String datacenter) throws IOException {
    File f = null;
    int port = getPlainTextPort(ports);
    try {
      List<String> mountPaths = new ArrayList<String>(numMountPointsPerNode);
      f = File.createTempFile("ambry", ".tmp");
      for (int i = 0; i < numMountPointsPerNode; i++) {
        File mountFile = new File(f.getParent(), "mountpathfile" + port + i);
        deleteFileOrDirectory(mountFile);
        mountFile.mkdir();
        mountPaths.add(mountFile.getAbsolutePath());
      }
      return new MockDataNodeId(ports, mountPaths, datacenter);
    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  /**
   * Return if ssl ports are enabled in this cluster.
   */
  public boolean isSslPortsEnabled() {
    return enableSSLPorts;
  }

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    DataInputStream dataInputStream = new DataInputStream(stream);
    short version = dataInputStream.readShort();
    long id = dataInputStream.readLong();
    return partitions.get(id);
  }

  @Override
  public List<PartitionId> getWritablePartitionIds(String partitionClass) {
    lastRequestedPartitionClasses.add(partitionClass);
    List<PartitionId> partitionIdList = Collections.emptyList();
    if (!partitionsUnavailable) {
      partitionIdList = partitionSelectionHelper.getWritablePartitions(partitionClass);
    }
    return partitionIdList;
  }

  @Override
  public List<PartitionId> getAllPartitionIds(String partitionClass) {
    lastRequestedPartitionClasses.add(partitionClass);
    return partitionSelectionHelper.getPartitions(partitionClass);
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return dataCentersInClusterMap.contains(datacenterName);
  }

  @Override
  public byte getLocalDatacenterId() {
    return (byte) dataCentersInClusterMap.indexOf(localDatacenterName);
  }

  @Override
  public String getDatacenterName(byte id) {
    int idx = (int) id;
    if (idx < 0 || idx >= dataCentersInClusterMap.size()) {
      return null;
    }
    return dataCentersInClusterMap.get(idx);
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    for (DataNodeId dataNodeId : dataNodes) {
      if (dataNodeId.getHostname().compareTo(hostname) == 0 && dataNodeId.getPort() == port) {
        return dataNodeId;
      }
    }
    return null;
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    ArrayList<ReplicaId> replicaIdsToReturn = new ArrayList<ReplicaId>();
    for (PartitionId partitionId : partitions.values()) {
      List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        if (replicaId.getDataNodeId().getHostname().compareTo(dataNodeId.getHostname()) == 0
            && replicaId.getDataNodeId().getPort() == dataNodeId.getPort()) {
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
    if (createNewRegistry) {
      metricRegistry = new MetricRegistry();
    }
    return metricRegistry;
  }

  /**
   * @return the special {@link MockPartitionId} if it was created.
   */
  public MockPartitionId getSpecialPartition() {
    return specialPartition;
  }

  /**
   * Create a {@link MetricRegistry} and ensure that this is the one that is returned by {@link #getMetricRegistry()}
   */
  public void createAndSetPermanentMetricRegistry() {
    setPermanentMetricRegistry(new MetricRegistry());
  }

  /**
   * Set the registry as {@code toSet} ensure that this is the one that is returned by {@link #getMetricRegistry()}
   * @param toSet the metric registry to set to.
   */
  public void setPermanentMetricRegistry(MetricRegistry toSet) {
    metricRegistry = toSet;
    createNewRegistry = false;
  }

  public void cleanup() throws IOException {
    for (PartitionId partitionId : partitions.values()) {
      MockPartitionId mockPartition = (MockPartitionId) partitionId;
      mockPartition.cleanUp();
    }

    for (DataNodeId dataNode : dataNodes) {
      List<String> mountPaths = ((MockDataNodeId) dataNode).getMountPaths();
      for (String mountPath : mountPaths) {
        File mountPathDir = new File(mountPath);
        deleteFileOrDirectory(mountPathDir);
      }
    }
  }

  protected static boolean deleteFileOrDirectory(File f) throws IOException {
    if (f.exists()) {
      if (f.isDirectory()) {
        File[] children = f.listFiles();
        if (children == null) {
          throw new IOException("Error listing files of the directory");
        }
        for (File c : children) {
          deleteFileOrDirectory(c);
        }
      }
      if (!f.delete()) {
        throw new IOException("Failed to delete file: " + f);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Mark all partitions as unavailable.
   */
  public void markAllPartitionsUnavailable() {
    partitionsUnavailable = true;
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    switch (event) {
      case Disk_Error:
        ((MockDiskId) replicaId.getDiskId()).onDiskError();
        break;
      case Disk_Ok:
        ((MockDiskId) replicaId.getDiskId()).onDiskOk();
        break;
      case Node_Timeout:
        ((MockDataNodeId) replicaId.getDataNodeId()).onNodeTimeout();
        break;
      case Node_Response:
        ((MockDataNodeId) replicaId.getDataNodeId()).onNodeResponse();
        break;
      case Partition_ReadOnly:
        ((MockPartitionId) replicaId.getPartitionId()).onPartitionReadOnly();
        break;
    }
  }

  @Override
  public void close() {
    // No-op.
  }

  /**
   * Sets the local datacenter name and changes the views of the partition classes. Not thread safe.
   * @param localDatacenterName the name of the local datacenter
   */
  public void setLocalDatacenterName(String localDatacenterName) {
    if (!hasDatacenter(localDatacenterName)) {
      throw new IllegalArgumentException("Clustermap has no DC named " + localDatacenterName);
    }
    this.localDatacenterName = localDatacenterName;
    partitionSelectionHelper.updatePartitions(partitions.values(), localDatacenterName);
  }

  /**
   * @return the partition classes requested for in the late N calls to {@link #getAllPartitionIds(String)} or
   * {@link #getWritablePartitionIds(String)}.
   */
  public List<String> getLastNRequestedPartitionClasses() {
    return lastRequestedPartitionClasses;
  }

  /**
   * Sets the last request partition class to {@code null}. Not thread safe.
   */
  public void clearLastNRequestedPartitionClasses() {
    lastRequestedPartitionClasses.clear();
  }
}

