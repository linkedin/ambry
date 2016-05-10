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
import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import static com.github.ambry.utils.Utils.readStringFromFile;
import static com.github.ambry.utils.Utils.writeJsonToFile;


/**
 * ClusterMapManager allows components in Ambry to query the topology. This covers the {@link HardwareLayout} and the
 * {@link PartitionLayout}.
 */
public class ClusterMapManager implements ClusterMap {
  protected final HardwareLayout hardwareLayout;
  protected final PartitionLayout partitionLayout;
  private final MetricRegistry metricRegistry;
  private final ClusterMapMetrics clusterMapMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public ClusterMapManager(PartitionLayout partitionLayout) {
    if (logger.isTraceEnabled()) {
      logger.trace("ClusterMapManager " + partitionLayout);
    }
    this.hardwareLayout = partitionLayout.getHardwareLayout();
    this.partitionLayout = partitionLayout;
    this.metricRegistry = new MetricRegistry();
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
  }

  public ClusterMapManager(String hardwareLayoutPath, String partitionLayoutPath, ClusterMapConfig clusterMapConfig)
      throws IOException, JSONException {
    logger.trace("ClusterMapManager " + hardwareLayoutPath + ", " + partitionLayoutPath);
    this.hardwareLayout = new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutPath)), clusterMapConfig);
    this.partitionLayout = new PartitionLayout(hardwareLayout, new JSONObject(readStringFromFile(partitionLayoutPath)));
    this.metricRegistry = new MetricRegistry();
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
  }

  public void persist(String hardwareLayoutPath, String partitionLayoutPath)
      throws IOException, JSONException {
    logger.trace("persist " + hardwareLayoutPath + ", " + partitionLayoutPath);
    writeJsonToFile(hardwareLayout.toJSONObject(), hardwareLayoutPath);
    writeJsonToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
  }

  public List<PartitionId> getAllPartitions() {
    return partitionLayout.getPartitions();
  }

  // Implementation of ClusterMap interface
  // --------------------------------------

  @Override
  public List<PartitionId> getWritablePartitionIds() {
    return partitionLayout.getWritablePartitions();
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream)
      throws IOException {
    PartitionId partitionId = partitionLayout.getPartition(stream);
    if (partitionId == null) {
      throw new IOException("Partition id from stream is null");
    }
    return partitionId;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return hardwareLayout.findDatacenter(datacenterName) != null;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return hardwareLayout.findDataNode(hostname, port);
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    List<Replica> replicas = getReplicas(dataNodeId);
    return new ArrayList<ReplicaId>(replicas);
  }

  public List<Replica> getReplicas(DataNodeId dataNodeId) {
    List<Replica> replicas = new ArrayList<Replica>();
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        if (replica.getDataNodeId().equals(dataNodeId)) {
          replicas.add(replica);
        }
      }
    }
    return replicas;
  }

  @Override
  public List<DataNodeId> getDataNodeIds() {
    List<DataNodeId> dataNodeIds = new ArrayList<DataNodeId>();
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      dataNodeIds.addAll(datacenter.getDataNodes());
    }
    return dataNodeIds;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  // Administrative API
  // -----------------------

  public long getRawCapacityInBytes() {
    return hardwareLayout.getRawCapacityInBytes();
  }

  public long getAllocatedRawCapacityInBytes() {
    return partitionLayout.getAllocatedRawCapacityInBytes();
  }

  public long getAllocatedUsableCapacityInBytes() {
    return partitionLayout.getAllocatedUsableCapacityInBytes();
  }

  public long getAllocatedRawCapacityInBytes(Datacenter datacenter) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().getDatacenter().equals(datacenter)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedRawCapacityInBytes(DataNodeId dataNode) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().equals(dataNode)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedRawCapacityInBytes(Disk disk) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk currentDisk = (Disk) replica.getDiskId();
        if (currentDisk.equals(disk)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getUnallocatedRawCapacityInBytes() {
    return getRawCapacityInBytes() - getAllocatedRawCapacityInBytes();
  }

  public long getUnallocatedRawCapacityInBytes(Datacenter datacenter) {
    return datacenter.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(datacenter);
  }

  public long getUnallocatedRawCapacityInBytes(DataNode dataNode) {
    return dataNode.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(dataNode);
  }

  public long getUnallocatedRawCapacityInBytes(Disk disk) {
    return disk.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(disk);
  }

  public DataNode getDataNodeWithMostUnallocatedRawCapacity(Datacenter dc, Set nodesToExclude) {
    DataNode maxCapacityNode = null;
    List<DataNode> dataNodes = dc.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      if (!nodesToExclude.contains(dataNode) && (maxCapacityNode == null
          || getUnallocatedRawCapacityInBytes(dataNode) > getUnallocatedRawCapacityInBytes(maxCapacityNode))) {
        maxCapacityNode = dataNode;
      }
    }
    return maxCapacityNode;
  }

  public Disk getDiskWithMostUnallocatedRawCapacity(DataNode node, long minCapacity) {
    Disk maxCapacityDisk = null;
    List<Disk> disks = node.getDisks();
    for (Disk disk : disks) {
      if ((maxCapacityDisk == null || getUnallocatedRawCapacityInBytes(disk) > getUnallocatedRawCapacityInBytes(
          maxCapacityDisk)) && getUnallocatedRawCapacityInBytes(disk) >= minCapacity) {
        maxCapacityDisk = disk;
      }
    }
    return maxCapacityDisk;
  }

  public PartitionId addNewPartition(List<Disk> disks, long replicaCapacityInBytes) {
    return partitionLayout.addNewPartition(disks, replicaCapacityInBytes);
  }

  // Determine if there is enough capacity to allocate a PartitionId.
  private boolean checkEnoughUnallocatedRawCapacity(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      if (getUnallocatedRawCapacityInBytes(datacenter) < replicaCountPerDatacenter * replicaCapacityInBytes) {
        logger.warn("Insufficient unallocated space in datacenter {} ({} bytes unallocated)", datacenter.getName(),
            getUnallocatedRawCapacityInBytes(datacenter));
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getUnallocatedRawCapacityInBytes(disk) >= replicaCapacityInBytes) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with unallocated space in datacenter {} for {} Replicas)", rcpd,
            datacenter.getName(), replicaCountPerDatacenter);
        return false;
      }
    }

    return true;
  }

  // Allocate unique datanode.disks for each replica in each datacenter up to replicaCountPerDatacenter (hard-code all
  // datacenters to have same number of replicas for now).
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes,
      List<Datacenter> dataCenters) {
    ArrayList<Disk> allocatedDisks = new ArrayList<Disk>();

    for (Datacenter datacenter : dataCenters) {
      int rcpd = replicaCountPerDatacenter;
      Set<DataNode> nodesToExclude = new HashSet<DataNode>();
      for (int i = 0; i < rcpd; i++) {
        DataNode nodeWithMostCapacity = getDataNodeWithMostUnallocatedRawCapacity(datacenter, nodesToExclude);
        Disk diskWithMostCapacity = getDiskWithMostUnallocatedRawCapacity(nodeWithMostCapacity, replicaCapacityInBytes);
        allocatedDisks.add(diskWithMostCapacity);
        nodesToExclude.add(nodeWithMostCapacity);
      }
    }
    return allocatedDisks;
  }

  // Best effort (or less) allocation of partitions. I.e., size of returned list may be less than numPartitions.
  public List<PartitionId> allocatePartitions(int numPartitions, int replicaCountPerDatacenter,
      long replicaCapacityInBytes) {
    ArrayList<PartitionId> partitions = new ArrayList<PartitionId>(numPartitions);

    while (checkEnoughUnallocatedRawCapacity(replicaCountPerDatacenter, replicaCapacityInBytes) && numPartitions > 0) {
      List<Disk> disks =
          allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, hardwareLayout.getDatacenters());
      if (disks.size() == 0) {
        System.err.println("numPartitions: " + numPartitions);
        break;
      }
      partitions.add(partitionLayout.addNewPartition(disks, replicaCapacityInBytes));
      numPartitions--;
    }

    return partitions;
  }

  public void addReplicas(PartitionId partitionId, String dataCenterName) {
    List<ReplicaId> replicaIds = partitionId.getReplicaIds();
    Map<String, Integer> replicaCountByDatacenter = new HashMap<String, Integer>();
    long capacityOfReplicasInBytes = 0;
    // we ensure that the datacenter provided does not have any replicas
    for (ReplicaId replicaId : replicaIds) {
      if (replicaId.getDataNodeId().getDatacenterName().compareToIgnoreCase(dataCenterName) == 0) {
        throw new IllegalArgumentException("Data center " + dataCenterName +
            " provided already contains replica for partition " + partitionId);
      }
      capacityOfReplicasInBytes = replicaId.getCapacityInBytes();
      Integer numberOfReplicas = replicaCountByDatacenter.get(replicaId.getDataNodeId().getDatacenterName());
      if (numberOfReplicas == null) {
        numberOfReplicas = new Integer(0);
      }
      numberOfReplicas++;
      replicaCountByDatacenter.put(replicaId.getDataNodeId().getDatacenterName(), numberOfReplicas);
    }
    if (replicaCountByDatacenter.size() == 0) {
      throw new IllegalArgumentException("No existing replicas present for partition " + partitionId + " in cluster.");
    }
    // verify that all data centers have the same replica
    int numberOfReplicasPerDatacenter = 0;
    int index = 0;
    for (Map.Entry<String, Integer> entry : replicaCountByDatacenter.entrySet()) {
      if (index == 0) {
        numberOfReplicasPerDatacenter = entry.getValue();
      }
      if (numberOfReplicasPerDatacenter != entry.getValue()) {
        throw new IllegalStateException("Datacenters have different replicas for partition " + partitionId);
      }
      index++;
    }
    Datacenter datacenterToAdd = hardwareLayout.findDatacenter(dataCenterName);
    List<Datacenter> datacentersToAdd = new ArrayList<Datacenter>();
    datacentersToAdd.add(datacenterToAdd);
    List<Disk> disksForReplicas =
        allocateDisksForPartition(numberOfReplicasPerDatacenter, capacityOfReplicasInBytes, datacentersToAdd);
    partitionLayout.addNewReplicas((Partition) partitionId, disksForReplicas);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ClusterMapManager that = (ClusterMapManager) o;

    if (hardwareLayout != null ? !hardwareLayout.equals(that.hardwareLayout) : that.hardwareLayout != null) {
      return false;
    }
    return !(partitionLayout != null ? !partitionLayout.equals(that.partitionLayout) : that.partitionLayout != null);
  }

  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    switch (event) {
      case Disk_Error:
        ((Disk) replicaId.getDiskId()).onDiskError();
        break;
      case Disk_Ok:
        ((Disk) replicaId.getDiskId()).onDiskOk();
        break;
      case Node_Timeout:
        ((DataNode) replicaId.getDataNodeId()).onNodeTimeout();
        break;
      case Node_Response:
        ((DataNode) replicaId.getDataNodeId()).onNodeResponse();
        break;
      case Partition_ReadOnly:
        ((Partition) replicaId.getPartitionId()).onPartitionReadOnly();
        break;
    }
  }
}
