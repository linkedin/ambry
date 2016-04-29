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

import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
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
  private int currentPlainTextPort = 62000;
  private int currentSSLPort = 63000;

  public MockClusterMap()
      throws IOException {
    this(false);
  }

  public MockClusterMap(boolean enableSSLPorts)
      throws IOException {

    dataNodes = new ArrayList<MockDataNodeId>(9);
    // create 3 nodes with each having 3 mount paths
    if (enableSSLPorts) {
      MockDataNodeId dataNodeId1 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC1");
      MockDataNodeId dataNodeId2 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC1");
      MockDataNodeId dataNodeId3 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC1");

      MockDataNodeId dataNodeId4 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC2");
      MockDataNodeId dataNodeId5 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC2");
      MockDataNodeId dataNodeId6 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC2");

      MockDataNodeId dataNodeId7 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC3");
      MockDataNodeId dataNodeId8 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC3");
      MockDataNodeId dataNodeId9 =
          createDataNode(getListOfPorts(currentPlainTextPort++, currentSSLPort++), "DC3");

      dataNodes.add(dataNodeId1);
      dataNodes.add(dataNodeId2);
      dataNodes.add(dataNodeId3);
      dataNodes.add(dataNodeId4);
      dataNodes.add(dataNodeId5);
      dataNodes.add(dataNodeId6);
      dataNodes.add(dataNodeId7);
      dataNodes.add(dataNodeId8);
      dataNodes.add(dataNodeId9);
    } else {
      MockDataNodeId dataNodeId1 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC1");
      MockDataNodeId dataNodeId2 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC1");
      MockDataNodeId dataNodeId3 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC1");

      MockDataNodeId dataNodeId4 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC2");
      MockDataNodeId dataNodeId5 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC2");
      MockDataNodeId dataNodeId6 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC2");

      MockDataNodeId dataNodeId7 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC3");
      MockDataNodeId dataNodeId8 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC3");
      MockDataNodeId dataNodeId9 = createDataNode(getListOfPorts(currentPlainTextPort++), "DC3");

      dataNodes.add(dataNodeId1);
      dataNodes.add(dataNodeId2);
      dataNodes.add(dataNodeId3);
      dataNodes.add(dataNodeId4);
      dataNodes.add(dataNodeId5);
      dataNodes.add(dataNodeId6);
      dataNodes.add(dataNodeId7);
      dataNodes.add(dataNodeId8);
      dataNodes.add(dataNodeId9);
    }
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

  ArrayList<Port> getListOfPorts(int port) {
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(port, PortType.PLAINTEXT));
    return ports;
  }

  ArrayList<Port> getListOfPorts(int port, int sslPort) {
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(port, PortType.PLAINTEXT));
    ports.add(new Port(sslPort, PortType.SSL));
    return ports;
  }

  private int getPlainTextPort(ArrayList<Port> ports) {
    for (Port port : ports) {
      if (port.getPortType() == PortType.PLAINTEXT) {
        return port.getPort();
      }
    }
    throw new IllegalArgumentException("No PlainText port found ");
  }

  private MockDataNodeId createDataNode(ArrayList<Port> ports, String datacenter)
      throws IOException {
    File f = null;
    int port = getPlainTextPort(ports);
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
      MockDataNodeId dataNode = new MockDataNodeId(ports, mountPaths, datacenter);
      return dataNode;
    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream)
      throws IOException {
    short version = stream.readShort();
    long id = stream.readLong();
    return partitions.get(id);
  }

  @Override
  public List<PartitionId> getWritablePartitionIds() {
    List<PartitionId> partitionIdList = new ArrayList<PartitionId>();
    for (PartitionId partitionId : partitions.values()) {
      partitionIdList.add(partitionId);
    }
    return partitionIdList;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return true;
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
      List<ReplicaId> replicaIds = partitionId.getReplicaIds();
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
    return new MetricRegistry();
  }

  public void cleanup() {
    for (PartitionId partitionId : partitions.values()) {
      MockPartitionId mockPartition = (MockPartitionId) partitionId;
      mockPartition.cleanUp();
    }

    for (DataNodeId dataNode : dataNodes) {
      List<String> mountPaths = ((MockDataNodeId) dataNode).getMountPaths();
      for (String mountPath : mountPaths) {
        File mountPathDir = new File(mountPath);
        for (File file : mountPathDir.listFiles()) {
          file.delete();
        }
        mountPathDir.delete();
      }
    }
  }

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
}
