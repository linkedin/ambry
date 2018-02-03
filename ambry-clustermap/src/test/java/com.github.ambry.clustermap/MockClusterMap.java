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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * Mock cluster map for unit tests.
 */
public class MockClusterMap implements ClusterMap {

  protected final boolean enableSSLPorts;
  protected final Map<Long, PartitionId> partitions;
  protected final List<MockDataNodeId> dataNodes;
  protected final int numMountPointsPerNode;
  protected final int numStoresPerMountPoint;
  protected final HashSet<String> dataCentersInClusterMap = new HashSet<>();
  protected boolean partitionsUnavailable = false;
  private boolean createNewRegistry = true;
  private MetricRegistry metricRegistry;

  /**
   * The default constructor sets up a 9 node cluster with 3 mount points in each, with 3 partitions/replicas per
   * mount point which amount to a total of 9 replicas per node and 81 replicas across the cluster. If this cluster map
   * is going to be used to start a cluster, use it judiciously to avoid resource consumption issues on the test
   * machine.
   */
  public MockClusterMap() throws IOException {
    this(false, 9, 3, 3);
  }

  /**
   * Creates and returns a mock cluster map.
   * <p>
   *
   * In doing so, this actually creates the mock cluster itself, by setting up nodes,
   * mount points and replicas for partitions on these nodes. (The mock nodes will not be started automatically,
   * however).
   * <p>
   *
   * The parameters to this method determine the number of mock datanodes that will be created in the cluster,
   * the number of mount points that will be created on each of these mock datanodes,
   * and the number of stores that will be created on each mount point. Stores correspond to replicas,
   * so the number of stores also determines the number of replicas that will be created on a node (which is
   * going to be the number of mount points per node multiplied by the number of stores per mount point). Since every
   * partition will be present on every datanode, this is also the number of partitions that will be created across the
   * cluster. The total number of replicas across the cluster will therefore be this number multiplied by the
   * number of nodes. This is therefore the number of stores that will be created by the test on the machine on which
   * it runs - and the resource consumption on the machine is a function of this number. Tests that start a cluster or
   * a server should therefore keep these parameters to the minimum required for testing intended functionality
   * correctly (however, tests that only create the MockClusterMap but do not start the cluster or servers will not
   * end up using any significant resources, and should be fine).
   *
   * @param numNodes number of mock datanodes that will be created (every 3 of which will be put in a separate
   *                 datacenter).
   * @param numMountPointsPerNode number of mount points (mocking disks) that will be created in each datanode.
   * @param numStoresPerMountPoint the number of stores that will be created on each mount point.
   */
  public MockClusterMap(boolean enableSSLPorts, int numNodes, int numMountPointsPerNode, int numStoresPerMountPoint)
      throws IOException {
    this.enableSSLPorts = enableSSLPorts;
    this.numMountPointsPerNode = numMountPointsPerNode;
    this.numStoresPerMountPoint = numStoresPerMountPoint;
    dataNodes = new ArrayList<MockDataNodeId>(numNodes);
    //Every group of 3 nodes will be put in the same DC.
    int dcIndex = 0;
    int currentPlainTextPort = 62000;
    int currentSSLPort = 63000;
    for (int i = 0; i < numNodes; i++) {
      if (i % 3 == 0) {
        dcIndex++;
      }
      String dcName = "DC" + dcIndex;
      dataCentersInClusterMap.add(dcName);
      if (enableSSLPorts) {
        dataNodes.add(createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), dcName));
      } else {
        dataNodes.add(createDataNode(getListOfPorts(currentPlainTextPort++), dcName));
      }
    }
    partitions = new HashMap<Long, PartitionId>();

    // create partitions
    long partitionId = 0;
    for (int i = 0; i < dataNodes.get(0).getMountPaths().size(); i++) {
      for (int j = 0; j < numStoresPerMountPoint; j++) {
        PartitionId id = new MockPartitionId(partitionId, dataNodes, i);
        partitions.put(partitionId, id);
        partitionId++;
      }
    }
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

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    DataInputStream dataInputStream = new DataInputStream(stream);
    short version = dataInputStream.readShort();
    long id = dataInputStream.readLong();
    return partitions.get(id);
  }

  @Override
  public List<PartitionId> getWritablePartitionIds() {
    List<PartitionId> partitionIdList = new ArrayList<PartitionId>();
    if (!partitionsUnavailable) {
      for (PartitionId partitionId : partitions.values()) {
        partitionIdList.add(partitionId);
      }
    }
    return partitionIdList;
  }

  @Override
  public List<PartitionId> getAllPartitionIds() {
    return new ArrayList<>(partitions.values());
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return dataCentersInClusterMap.contains(datacenterName);
  }

  @Override
  public byte getLocalDatacenterId() {
    return UNKNOWN_DATACENTER_ID;
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
}

