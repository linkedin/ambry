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

import com.github.ambry.config.ClusterMapConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Ambry hardwareLayout consists of a set of {@link Datacenter}s.
 */
class HardwareLayout {
  private final String clusterName;
  private final long version;
  private final ArrayList<Datacenter> datacenters;
  private final Map<Byte, Datacenter> datacenterById;
  private final long rawCapacityInBytes;
  private final long dataNodeCount;
  private final long diskCount;
  private final Map<HardwareState, Long> dataNodeInHardStateCount;
  private final Map<HardwareState, Long> diskInHardStateCount;
  private final ClusterMapConfig clusterMapConfig;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public HardwareLayout(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("HardwareLayout " + jsonObject.toString());
    }
    this.clusterName = jsonObject.getString("clusterName");
    this.version = jsonObject.getLong("version");
    this.clusterMapConfig = clusterMapConfig;

    this.datacenters = new ArrayList<Datacenter>(jsonObject.getJSONArray("datacenters").length());
    this.datacenterById = new HashMap<>();
    for (int i = 0; i < jsonObject.getJSONArray("datacenters").length(); ++i) {
      Datacenter datacenter =
          new Datacenter(this, jsonObject.getJSONArray("datacenters").getJSONObject(i), clusterMapConfig);
      this.datacenters.add(datacenter);
      datacenterById.put(datacenter.getId(), datacenter);
    }

    this.rawCapacityInBytes = calculateRawCapacityInBytes();
    this.dataNodeCount = calculateDataNodeCount();
    this.diskCount = calculateDiskCount();
    this.dataNodeInHardStateCount = calculateDataNodeInHardStateCount();
    this.diskInHardStateCount = calculateDiskInHardStateCount();

    validate();
  }

  public String getClusterName() {
    return clusterName;
  }

  public long getVersion() {
    return version;
  }

  public List<Datacenter> getDatacenters() {
    return datacenters;
  }

  public long getRawCapacityInBytes() {
    return rawCapacityInBytes;
  }

  private long calculateRawCapacityInBytes() {
    long capacityInBytes = 0;
    for (Datacenter datacenter : datacenters) {
      capacityInBytes += datacenter.getRawCapacityInBytes();
    }
    return capacityInBytes;
  }

  public long getDatacenterCount() {
    return datacenters.size();
  }

  public long getDataNodeCount() {
    return dataNodeCount;
  }

  private long calculateDataNodeCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      count += datacenter.getDataNodes().size();
    }
    return count;
  }

  public long getDiskCount() {
    return diskCount;
  }

  private long calculateDiskCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        count += dataNode.getDisks().size();
      }
    }
    return count;
  }

  public long getDataNodeInHardStateCount(HardwareState hardwareState) {
    return dataNodeInHardStateCount.get(hardwareState);
  }

  ClusterMapConfig getClusterMapConfig() {
    return clusterMapConfig;
  }

  private Map<HardwareState, Long> calculateDataNodeInHardStateCount() {
    Map<HardwareState, Long> dataNodeInStateCount = new HashMap<HardwareState, Long>();
    for (HardwareState hardwareState : HardwareState.values()) {
      dataNodeInStateCount.put(hardwareState, new Long(0));
    }
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        dataNodeInStateCount.put(dataNode.getState(), dataNodeInStateCount.get(dataNode.getState()) + 1);
      }
    }
    return dataNodeInStateCount;
  }

  public long calculateUnavailableDataNodeCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (dataNode.isDown()) {
          count++;
        }
      }
    }
    return count;
  }

  public long getDiskInHardStateCount(HardwareState hardwareState) {
    return diskInHardStateCount.get(hardwareState);
  }

  private Map<HardwareState, Long> calculateDiskInHardStateCount() {
    Map<HardwareState, Long> diskInStateCount = new HashMap<HardwareState, Long>();
    for (HardwareState hardwareState : HardwareState.values()) {
      diskInStateCount.put(hardwareState, new Long(0));
    }
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          diskInStateCount.put(disk.getState(), diskInStateCount.get(disk.getState()) + 1);
        }
      }
    }
    return diskInStateCount;
  }

  public long calculateUnavailableDiskCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (disk.isDown()) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /**
   * Finds Datacenter by name
   *
   * @param datacenterName name of datacenter to be found
   * @return Datacenter or null if not found.
   */
  public Datacenter findDatacenter(String datacenterName) {
    for (Datacenter datacenter : datacenters) {
      if (datacenter.getName().compareToIgnoreCase(datacenterName) == 0) {
        return datacenter;
      }
    }
    return null;
  }

  /**
   * Finds Datacenter by id
   *
   * @param id id of datacenter to find
   * @return Datacenter or null if not found.
   */
  public Datacenter findDatacenter(byte id) {
    return datacenterById.get(id);
  }

  /**
   * Finds DataNode by hostname and port. Note that hostname is converted to canonical hostname for comparison.
   *
   * @param hostname of datanode
   * @param port of datanode
   * @return DataNode or null if not found.
   */
  public DataNode findDataNode(String hostname, int port) {
    String canonicalHostname =
        clusterMapConfig.clusterMapResolveHostnames ? ClusterMapUtils.getFullyQualifiedDomainName(hostname) : hostname;
    logger.trace("host to find host {} port {}", canonicalHostname, port);
    for (Datacenter datacenter : datacenters) {
      logger.trace("datacenter {}", datacenter.getName());
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (dataNode.getHostname().equals(canonicalHostname) && (dataNode.getPort() == port)) {
          return dataNode;
        }
      }
    }
    return null;
  }

  /**
   * Finds Disk by hostname, port, and mount path.
   *
   * @param hostname of datanode
   * @param port of datanode
   * @param mountPath of disk
   * @return Disk or null if not found.
   */
  public Disk findDisk(String hostname, int port, String mountPath) {
    DataNode dataNode = findDataNode(hostname, port);
    if (dataNode != null) {
      for (Disk disk : dataNode.getDisks()) {
        if (disk.getMountPath().equals(mountPath)) {
          return disk;
        }
      }
    }
    return null;
  }

  protected void validateClusterName() {
    if (clusterName == null) {
      throw new IllegalStateException("HardwareLayout clusterName cannot be null.");
    } else if (clusterName.length() == 0) {
      throw new IllegalStateException("HardwareLayout clusterName cannot be zero length.");
    }
  }

  // Validate each hardware component (Datacenter, DataNode, and Disk) are unique
  protected void validateUniqueness() throws IllegalStateException {
    logger.trace("begin validateUniqueness.");
    HashSet<Datacenter> datacenterSet = new HashSet<Datacenter>();
    HashSet<DataNode> dataNodeSet = new HashSet<DataNode>();
    HashSet<Disk> diskSet = new HashSet<Disk>();

    for (Datacenter datacenter : datacenters) {
      if (!datacenterSet.add(datacenter)) {
        throw new IllegalStateException("Duplicate Datacenter detected: " + datacenter.toString());
      }
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (!dataNodeSet.add(dataNode)) {
          throw new IllegalStateException("Duplicate DataNode detected: " + dataNode.toString());
        }
        for (Disk disk : dataNode.getDisks()) {
          if (!diskSet.add(disk)) {
            throw new IllegalStateException("Duplicate Disk detected: " + disk.toString());
          }
        }
      }
    }
    logger.trace("complete validateUniqueness.");
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateClusterName();
    validateUniqueness();
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject =
        new JSONObject().put("clusterName", clusterName).put("version", version).put("datacenters", new JSONArray());
    for (Datacenter datacenter : datacenters) {
      jsonObject.accumulate("datacenters", datacenter.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString(2);
    } catch (JSONException e) {
      logger.error("JSONException caught in toString: {}", e.getCause());
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HardwareLayout that = (HardwareLayout) o;

    if (!clusterName.equals(that.clusterName)) {
      return false;
    }
    return datacenters.equals(that.datacenters);
  }
}
