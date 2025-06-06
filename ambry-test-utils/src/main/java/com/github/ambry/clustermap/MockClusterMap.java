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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static org.mockito.Mockito.*;


/**
 * Mock cluster map for unit tests.
 */
public class MockClusterMap implements ClusterMap {
  public static final String DEFAULT_PARTITION_CLASS = "defaultPartitionClass";
  public static final String SPECIAL_PARTITION_CLASS = "specialPartitionClass";
  public static final int PLAIN_TEXT_PORT_START_NUMBER = 62000;
  public static final int SSL_PORT_START_NUMBER = 63000;
  public static final int HTTP2_PORT_START_NUMBER = 64000;

  protected final boolean enableSSLPorts;
  protected final Map<Long, PartitionId> partitions;
  protected final List<MockDataNodeId> dataNodes;
  protected final int numMountPointsPerNode;
  protected boolean enableHttp2Ports = true;
  private final List<String> dataCentersInClusterMap = new ArrayList<>();
  private final Map<String, List<MockDataNodeId>> dcToDataNodes = new HashMap<>();
  private final ClusterMapUtils.PartitionSelectionHelper partitionSelectionHelper;
  protected boolean partitionsUnavailable = false;
  private boolean createNewRegistry = true;
  private MetricRegistry metricRegistry;
  // needs to be thread safe
  private List<String> lastRequestedPartitionClasses = new CopyOnWriteArrayList<>();
  // expose current port numbers to other methods for dynamic nodes addition
  private int currentPlainTextPort = PLAIN_TEXT_PORT_START_NUMBER;
  private int currentSSLPort = SSL_PORT_START_NUMBER;
  private int currentHttp2Port = HTTP2_PORT_START_NUMBER;
  public String cloudDatacenterName;
  // allow this to be changed to support some tests
  private String localDatacenterName;

  private final MockPartitionId specialPartition;
  private ClusterMapChangeListener clusterMapChangeListener = null;
  private ClusterManagerQueryHelper mockClusterManagerQueryHelper = Mockito.mock(ClusterManagerQueryHelper.class);

  private RuntimeException exceptionOnSnapshot = null;
  private volatile boolean shouldDataNodeBeInFullAuto = false;
  private DataNodeId currentDataNodeId;

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
    this(false, true, 9, 3, 3, false, false, null);
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
   * @param  enableSSLPorts whether to enable SSL ports.
   * @param enableHttp2Ports whether to enable Http2 ports.
   * @param numNodes number of mock datanodes that will be created (every 3 of which will be put in a separate
   *                 datacenter).
   * @param numMountPointsPerNode number of mount points (mocking disks) that will be created in each datanode.
   * @param numDefaultStoresPerMountPoint the number of stores that will be created on each mount point.
   * @param createOnlyDefaultPartitionClass if {@code true}, does not attempt to create the "special" partition. If
   *                                        {@code false}, attempts to do so. See javadoc of function for more details
   * @param includeCloudDc {@code true} to make DC1 a "cloud" DC: one with a single {@link ReplicaType#CLOUD_BACKED}
   *                       replica for each partition in the cluster map. The virtual datanode created for the cloud DC
   *                       does not count against {@code numNodes}.
   * @param localDcName name of the local data center
   */
  public MockClusterMap(boolean enableSSLPorts, boolean enableHttp2Ports, int numNodes, int numMountPointsPerNode,
      int numDefaultStoresPerMountPoint, boolean createOnlyDefaultPartitionClass, boolean includeCloudDc, String localDcName)
      throws IOException {
    this.enableSSLPorts = enableSSLPorts;
    this.enableHttp2Ports = enableHttp2Ports;
    this.numMountPointsPerNode = numMountPointsPerNode;
    dataNodes = new ArrayList<>(numNodes);
    //Every group of 3 nodes will be put in the same DC.
    int dcIndex = 0;
    String dcName = null;
    String cloudDc;
    if (includeCloudDc) {
      dcIndex++;
      dcName = "DC" + dcIndex;
      dataCentersInClusterMap.add(dcName);
      MockDataNodeId virtualNode = createDataNode(getListOfPorts(DataNodeId.UNKNOWN_PORT, null, null), dcName, 0);
      dataNodes.add(virtualNode);
      dcToDataNodes.computeIfAbsent(dcName, name -> new ArrayList<>()).add(virtualNode);
      cloudDatacenterName = dcName;
    } else {
      cloudDatacenterName = null;
    }
    for (int i = 0; i < numNodes; i++) {
      if (i % 3 == 0) {
        dcIndex++;
        dcName = "DC" + dcIndex;
        dataCentersInClusterMap.add(dcName);
      }
      MockDataNodeId dataNodeId;
      if (enableSSLPorts) {
        dataNodeId = createDataNode(
            getListOfPorts(currentPlainTextPort++, currentSSLPort++, enableHttp2Ports ? currentHttp2Port++ : null),
            dcName, numMountPointsPerNode);
      } else {
        dataNodeId =
            createDataNode(getListOfPorts(currentPlainTextPort++, null, enableHttp2Ports ? currentHttp2Port++ : null),
                dcName, numMountPointsPerNode);
      }
      dataNodes.add(dataNodeId);
      dcToDataNodes.computeIfAbsent(dcName, name -> new ArrayList<>()).add(dataNodeId);
      localDatacenterName = dcName;
    }
    if (localDcName != null) {
      // if caller specifies the local data center name, use the one specified.
      localDatacenterName = localDcName;
      dataCentersInClusterMap.add(localDcName);
      currentDataNodeId = createDataNode(
          getListOfPorts(currentPlainTextPort++, currentSSLPort++, enableHttp2Ports ? currentHttp2Port++ : null),
          localDcName, numMountPointsPerNode);
    }
    partitions = new HashMap<>();

    // create partitions
    long partitionId = 0;
    for (int i = 0; i < numMountPointsPerNode; i++) {
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
      dcToDataNodes.forEach((dc, mockDataNodeIds) -> {
        if (!dc.equals(cloudDatacenterName)) {
          nodeIds.addAll(mockDataNodeIds.subList(0, localDatacenterName.equals(dc) ? 3 : 2));
        }
      });
      MockPartitionId id = new MockPartitionId(partitionId, SPECIAL_PARTITION_CLASS, nodeIds, 0);
      partitions.put(partitionId, id);
      specialPartition = id;
    } else {
      specialPartition = null;
    }
    // find a partition belong to DEFAULT_PARTITION_CLASS
    PartitionId defaultPartition = partitions.values()
        .stream()
        .filter(p -> p.getPartitionClass().equals(DEFAULT_PARTITION_CLASS))
        .findFirst()
        .get();
    doReturn(partitions.values()).when(mockClusterManagerQueryHelper).getPartitions();
    doReturn(false).when(mockClusterManagerQueryHelper).isPartitionFilteringEnabled();
    partitionSelectionHelper =
        new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, localDatacenterName,
            Math.min(defaultPartition.getReplicaIds().size(), 3), DEFAULT_PARTITION_CLASS, null);
  }

  public DataNodeId getCurrentDataNodeId() {
    return currentDataNodeId;
  }
  /**
   * Creates a mock cluster map with given list of data nodes and partitions.
   * @param enableSSLPorts whether to enable SSL port.
   * @param datanodes the list of data nodes created in this mock cluster map.
   * @param numMountPointsPerNode number of mount points (mocking disks) that will be created in each data node
   * @param partitionIdList the list of partitions created in this cluster map.
   * @param localDatacenterName the name of local datacenter.
   */
  public MockClusterMap(boolean enableSSLPorts, List<MockDataNodeId> datanodes, int numMountPointsPerNode,
      List<PartitionId> partitionIdList, String localDatacenterName) {
    this.enableSSLPorts = enableSSLPorts;
    this.dataNodes = datanodes;
    this.numMountPointsPerNode = numMountPointsPerNode;
    partitions = new HashMap<>();
    partitionIdList.forEach(p -> partitions.put(Long.valueOf(p.toPathString()), p));
    this.localDatacenterName = localDatacenterName;
    doReturn(partitions.values()).when(mockClusterManagerQueryHelper).getPartitions();
    partitionSelectionHelper =
        new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, localDatacenterName,
            Math.min(partitionIdList.get(0).getReplicaIds().size(), 3), DEFAULT_PARTITION_CLASS, null);
    Set<String> dcNames = new HashSet<>();
    datanodes.forEach(node -> dcNames.add(node.getDatacenterName()));
    dataCentersInClusterMap.addAll(dcNames);
    specialPartition = null;
  }

  /**
   * Create a cluster map for recovery from the given {@code vcrNode} and {@code recoveryNode}.
   * The cluster is created such that {@code recoveryNode} has {@code vcrNode}'s replicas as peer replicas.
   * @param recoveryNode The data node.
   * @param vcrNode The vcr node.
   * @param dcName Name of the datacenter.
   */
  private MockClusterMap(MockDataNodeId recoveryNode, MockDataNodeId vcrNode, String dcName) {
    this.enableSSLPorts = false;
    this.numMountPointsPerNode = 1;
    dataNodes = new ArrayList<>();
    dataNodes.add(recoveryNode);
    dataNodes.add(vcrNode);

    dataCentersInClusterMap.add(dcName);
    localDatacenterName = dcName;
    dcToDataNodes.computeIfAbsent(dcName, name -> new ArrayList<>()).add(recoveryNode);
    dcToDataNodes.computeIfAbsent(dcName, name -> new ArrayList<>()).add(vcrNode);
    partitions = new HashMap<>();

    // create partitions
    MockPartitionId mockPartitionId = new MockPartitionId();
    List<ReplicaId> replicaIds = new ArrayList<>(dataNodes.size());
    MockReplicaId recoveryReplica = new MockReplicaId(recoveryNode.getPort(), mockPartitionId, recoveryNode, 0);
    replicaIds.add(recoveryReplica);
    MockReplicaId vcrReplica = new MockReplicaId(vcrNode.getPort(), mockPartitionId, vcrNode, 0);
    replicaIds.add(vcrReplica);
    mockPartitionId.replicaIds = replicaIds;

    // Set only vcrReplica as peer of recovery replica.
    recoveryReplica.setPeerReplicas(Collections.singletonList(vcrReplica));
    partitions.put(mockPartitionId.partition, mockPartitionId);
    doReturn(partitions.values()).when(mockClusterManagerQueryHelper).getPartitions();
    partitionSelectionHelper =
        new ClusterMapUtils.PartitionSelectionHelper(mockClusterManagerQueryHelper, localDatacenterName,
            Math.min(mockPartitionId.getReplicaIds().size(), 3), DEFAULT_PARTITION_CLASS, null);
    specialPartition = null;
  }

  /**
   * Create a cluster map for recovery from the given {@code vcrNode} and {@code recoveryNode}.
   * The cluster is created such that {@code recoveryNode} has {@code vcrNode}'s replicas as peer replicas.
   * @param recoveryNode The data node.
   * @param vcrNode The vcr node.
   * @param dcName Name of the datacenter.
   * @return {@link MockClusterMap} object.
   */
  public static MockClusterMap createOneNodeRecoveryClusterMap(MockDataNodeId recoveryNode, MockDataNodeId vcrNode,
      String dcName) {
    return new MockClusterMap(recoveryNode, vcrNode, dcName);
  }

  public static ArrayList<Port> getListOfPorts(int port, Integer sslPort, Integer http2Port) {
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(port, PortType.PLAINTEXT));
    if (sslPort != null) {
      ports.add(new Port(sslPort, PortType.SSL));
    }
    if (http2Port != null) {
      ports.add(new Port(http2Port, PortType.HTTP2));
    }
    return ports;
  }

  public static int getPlainTextPort(ArrayList<Port> ports) {
    for (Port port : ports) {
      if (port.getPortType() == PortType.PLAINTEXT) {
        return port.getPort();
      }
    }
    throw new IllegalArgumentException("No PlainText port found ");
  }

  public static MockDataNodeId createDataNode(ArrayList<Port> ports, String datacenter, int numMountPointsPerNode)
      throws IOException {
    File f = null;
    int port = getPlainTextPort(ports);
    try {
      List<String> mountPaths = new ArrayList<>(numMountPointsPerNode);
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
   * Create a number of new nodes in given data center and add them into mock clustermap.
   * @param numNewNodes the number of new nodes to create
   * @param dcName the data center where new nodes reside
   * @return a list of created new nodes
   */
  public List<MockDataNodeId> createNewDataNodes(int numNewNodes, String dcName) throws Exception {
    List<MockDataNodeId> createdNodes = new ArrayList<>();
    for (int i = 0; i < numNewNodes; ++i) {
      MockDataNodeId dataNodeId =
          createDataNode(getListOfPorts(currentPlainTextPort++, null, currentHttp2Port++), dcName,
              numMountPointsPerNode);
      dataNodes.add(dataNodeId);
      dcToDataNodes.computeIfAbsent(dcName, name -> new ArrayList<>()).add(dataNodeId);
      createdNodes.add(dataNodeId);
    }
    return createdNodes;
  }

  /**
   * Create a new partition and add it to mock clustermap.
   * @param dataNodes the replicas of new partition should be placed on the given data nodes only.
   * @return new {@link PartitionId}
   */
  public PartitionId createNewPartition(List<MockDataNodeId> dataNodes) {
    int mountPathSize;
    if (cloudDatacenterName == null) {
      mountPathSize = this.dataNodes.get(0).getMountPaths().size();
    } else {
      // the get(0) is cloud DC which doesn't have mount path. So we use get(1) DC.
      mountPathSize = this.dataNodes.get(1).getMountPaths().size();
    }

    int mountPathIndexToUse = (new Random()).nextInt(mountPathSize);
    return createNewPartition(dataNodes, mountPathIndexToUse);
  }

  /**
   * Create a new partition at given mount path on given nodes.
   * @param dataNodes the nodes on which replicas of new partition should reside.
   * @param mountPathIndexToUse the mount path index to use when creating new partition
   * @return new {@link PartitionId}
   */
  public PartitionId createNewPartition(List<MockDataNodeId> dataNodes, int mountPathIndexToUse) {
    MockPartitionId partitionId =
        new MockPartitionId(partitions.size(), DEFAULT_PARTITION_CLASS, dataNodes, mountPathIndexToUse);
    partitions.put((long) partitions.size(), partitionId);
    // make partitionSelectionHelper re-populate internal maps because new replicas(partition) are added
    partitionSelectionHelper.onReplicaAddedOrRemoved(partitionId.getReplicaIds(), Collections.emptyList());
    return partitionId;
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
  public PartitionId getPartitionIdByName(String partitionIdStr) {
    byte[] partitionBytes =
        ClusterMapUtils.serializeShortAndLong(AmbryPartition.CURRENT_VERSION, Long.parseLong(partitionIdStr));
    try {
      return getPartitionIdFromStream(new ByteBufferInputStream(ByteBuffer.wrap(partitionBytes)));
    } catch (IOException e) {
      return null;
    }
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
  public List<? extends PartitionId> getFullyWritablePartitionIds(String partitionClass) {
    lastRequestedPartitionClasses.add(partitionClass);
    List<PartitionId> partitionIdList = Collections.emptyList();
    if (!partitionsUnavailable) {
      partitionIdList = partitionSelectionHelper.getFullyWritablePartitions(partitionClass);
    }
    return partitionIdList;
  }

  @Override
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    lastRequestedPartitionClasses.add(partitionClass);
    if (!partitionsUnavailable) {
      return partitionSelectionHelper.getRandomWritablePartition(partitionClass, partitionsToExclude);
    }
    return null;
  }

  @Override
  public PartitionId getRandomFullyWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    lastRequestedPartitionClasses.add(partitionClass);
    if (!partitionsUnavailable) {
      return partitionSelectionHelper.getRandomFullyWritablePartition(partitionClass, partitionsToExclude);
    }
    return null;
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
    ArrayList<ReplicaId> replicaIdsToReturn = new ArrayList<>();
    for (PartitionId partitionId : partitions.values()) {
      List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
      for (ReplicaId replicaId : replicaIds) {
        if (replicaId.getDataNodeId().compareTo(dataNodeId) == 0) {
          replicaIdsToReturn.add(replicaId);
        }
      }
    }
    return replicaIdsToReturn;
  }

  @Override
  public List<DataNodeId> getDataNodeIds() {
    return new ArrayList<>(dataNodes);
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

  @Override
  public boolean isDataNodeInFullAutoMode(DataNodeId dn) {
    return shouldDataNodeBeInFullAuto;
  }

  public void shouldDataNodeBeInFullAuto(boolean b) {
    this.shouldDataNodeBeInFullAuto = b;
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

  public static boolean deleteFileOrDirectory(File f) throws IOException {
    if (f.exists()) {
      if (f.isDirectory()) {
        File[] children = f.listFiles();
        if (children == null) {
          throw new IOException("Error listing files of the directory " + f.getAbsolutePath());
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
    if (replicaId instanceof MockReplicaId) {
      switch (event) {
        case Disk_Error:
          if (replicaId.getReplicaType() == ReplicaType.DISK_BACKED) {
            ((MockDiskId) replicaId.getDiskId()).onDiskError();
          }
          break;
        case Disk_Ok:
          if (replicaId.getReplicaType() == ReplicaType.DISK_BACKED) {
            ((MockDiskId) replicaId.getDiskId()).onDiskOk();
          }
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

  @Override
  public JSONObject getSnapshot() {
    if (exceptionOnSnapshot != null) {
      throw exceptionOnSnapshot;
    }
    JSONObject snapshot = new JSONObject();
    snapshot.put(IMPLEMENTATION, MockClusterMap.class.getName());
    snapshot.put(CLUSTER_NAME, MockClusterMap.class.getSimpleName());
    snapshot.put(TIMESTAMP_MS, SystemTime.getInstance().milliseconds());
    JSONArray datacentersJsonArray = new JSONArray();
    dcToDataNodes.forEach((dc, mockDataNodeIds) -> {
      JSONObject data = new JSONObject();
      data.put(DATACENTER_NAME, dc);
      data.put(DATACENTER_ID, dataCentersInClusterMap.indexOf(dc));
      JSONArray datanodesInDc = new JSONArray();
      mockDataNodeIds.forEach(mockDataNodeId -> datanodesInDc.put(mockDataNodeId.getSnapshot()));
      data.put(DATACENTER_NODES, datanodesInDc);
      datacentersJsonArray.put(data);
    });
    snapshot.put(DATACENTERS, datacentersJsonArray);
    JSONArray partitionsJsonArray = new JSONArray();
    partitions.values().forEach(partitionId -> partitionsJsonArray.put(partitionId.getSnapshot()));
    snapshot.put(PARTITIONS, partitionsJsonArray);
    return snapshot;
  }

  @Override
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
    ReplicaId newReplica = null;
    PartitionId partition = partitions.get(Long.valueOf(partitionIdStr));
    if (partition != null) {
      for (ReplicaId replicaId : partition.getReplicaIds()) {
        if (replicaId.getDataNodeId().compareTo(dataNodeId) == 0) {
          newReplica = replicaId;
          // Decrease available disk capacity in bytes. This is checked in StorageManagerTest#updateDiskSpaceOnReplicaAdditionTest()
          newReplica.getDiskId().decreaseAvailableSpaceInBytes(replicaId.getCapacityInBytes());
          break;
        }
      }
    }
    return newReplica;
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    this.clusterMapChangeListener = clusterMapChangeListener;
  }

  @Override
  public boolean hasEnoughEligibleReplicasAvailableForPut(PartitionId partitionId, int requiredEligibleReplicaCount,
      boolean checkLocalDcOnly) {
    if (!partitionsUnavailable) {
      return partitionSelectionHelper.hasEnoughEligibleReplicasAvailableForPut(partitionId,
          requiredEligibleReplicaCount, checkLocalDcOnly);
    }
    return false;
  }

  @Override
  public void close() {
    // No-op.
  }

  /**
   * @return {@link ClusterMapChangeListener} registered to this cluster map.
   */
  public ClusterMapChangeListener getClusterMapChangeListener() {
    return clusterMapChangeListener;
  }

  /**
   * Invoke clustermap change listener. This method doesn't really remove the datanode.
   * @param dataNodeId The removed data node
   */
  public void invokeListenerForDataNodeRemoval(DataNodeId dataNodeId) {
    if (dataNodes.contains(dataNodeId) && clusterMapChangeListener != null) {
      clusterMapChangeListener.onDataNodeRemoved(dataNodeId);
    }
  }

  /**
   * Invoke clustermap change listener for Data node configs
   * @param dataNodeConfigs List of dataNodeConfigs
   */
  public void invokeListenerForDataNodeChange(List<DataNodeConfig> dataNodeConfigs) {
    clusterMapChangeListener.onDataNodeConfigChange(dataNodeConfigs);
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

  /**
   * @param e the {@link RuntimeException} to throw when {@link #getSnapshot()} is invoked.
   */
  public void setExceptionOnSnapshot(RuntimeException e) {
    exceptionOnSnapshot = e;
  }
}
