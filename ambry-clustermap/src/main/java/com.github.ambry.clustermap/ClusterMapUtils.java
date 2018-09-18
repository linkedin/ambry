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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * A class with clustermap related utility methods for use by other classes.
 */
public class ClusterMapUtils {
  // datacenterId == UNKNOWN_DATACENTER_ID indicate datacenterId is not available at the time when this blobId is formed.
  public static final byte UNKNOWN_DATACENTER_ID = -1;
  public static final String ZNODE_NAME = "PartitionOverride";
  public static final String ZNODE_PATH = "/ClusterConfigs/" + ZNODE_NAME;
  public static final String PROPERTYSTORE_ZNODE_PATH = "/PROPERTYSTORE/ClusterConfigs/" + ZNODE_NAME;
  static final String DISK_CAPACITY_STR = "capacityInBytes";
  static final String DISK_STATE = "diskState";
  static final String PARTITION_STATE = "state";
  static final String REPLICAS_STR = "Replicas";
  static final String REPLICAS_DELIM_STR = ",";
  static final String REPLICAS_STR_SEPARATOR = ":";
  static final String SSLPORT_STR = "sslPort";
  static final String RACKID_STR = "rackId";
  static final String SEALED_STR = "SEALED";
  static final String STOPPED_REPLICAS_STR = "STOPPED";
  static final String AVAILABLE_STR = "AVAILABLE";
  static final String READ_ONLY_STR = "RO";
  static final String READ_WRITE_STR = "RW";
  static final String UNAVAILABLE_STR = "UNAVAILABLE";
  static final String ZKCONNECTSTR_STR = "zkConnectStr";
  static final String ZKINFO_STR = "zkInfo";
  static final String DATACENTER_STR = "datacenter";
  static final String DATACENTER_ID_STR = "id";
  static final String SCHEMA_VERSION_STR = "schemaVersion";
  static final String XID_STR = "xid";
  static final long DEFAULT_XID = Long.MIN_VALUE;
  static final int MIN_PORT = 1025;
  static final int MAX_PORT = 65535;
  static final long MIN_REPLICA_CAPACITY_IN_BYTES = 1024 * 1024 * 1024L;
  static final long MAX_REPLICA_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024 * 1024;
  static final long MIN_DISK_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024;
  static final long MAX_DISK_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024 * 1024;
  static final int CURRENT_SCHEMA_VERSION = 0;

  /**
   * Stores all zk related info for a DC.
   */
  public static class DcZkInfo {
    private final String dcName;
    private final byte dcId;
    private final String zkConnectStr;

    /**
     * Construct a DcInfo object with the given parameters.
     * @param dcName the associated datacenter name.
     * @param dcId the associated datacenter ID.
     * @param zkConnectStr the associated ZK connect string for this datacenter.
     */
    DcZkInfo(String dcName, byte dcId, String zkConnectStr) {
      this.dcName = dcName;
      this.dcId = dcId;
      this.zkConnectStr = zkConnectStr;
    }

    public String getDcName() {
      return dcName;
    }

    public byte getDcId() {
      return dcId;
    }

    public String getZkConnectStr() {
      return zkConnectStr;
    }
  }

  /**
   * Construct and return the instance name given the host and port.
   * @param host the hostname of the instance.
   * @param port the port of the instance. Can be null.
   * @return the constructed instance name.
   */
  public static String getInstanceName(String host, Integer port) {
    return port == null ? host : host + "_" + port;
  }

  /**
   * Parses DC information JSON string and returns a map of datacenter name to {@link DcZkInfo}.
   * @param dcInfoJsonString the string containing the DC info.
   * @return a map of dcName -> DcInfo.
   * @throws JSONException if there is an error parsing the JSON.
   */
  public static Map<String, DcZkInfo> parseDcJsonAndPopulateDcInfo(String dcInfoJsonString) throws JSONException {
    Map<String, DcZkInfo> dataCenterToZkAddress = new HashMap<>();
    JSONObject root = new JSONObject(dcInfoJsonString);
    JSONArray all = root.getJSONArray(ZKINFO_STR);
    for (int i = 0; i < all.length(); i++) {
      JSONObject entry = all.getJSONObject(i);
      byte id = (byte) entry.getInt(DATACENTER_ID_STR);
      DcZkInfo dcZkInfo = new DcZkInfo(entry.getString(DATACENTER_STR), id, entry.getString(ZKCONNECTSTR_STR));
      dataCenterToZkAddress.put(dcZkInfo.dcName, dcZkInfo);
    }
    return dataCenterToZkAddress;
  }

  /**
   * Get the schema version associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the schema version of the information stored. If the field is absent in the InstanceConfig, the version
   *         is assumed to be 0, and 0 is returned.
   */
  static int getSchemaVersion(InstanceConfig instanceConfig) {
    String schemaVersionStr = instanceConfig.getRecord().getSimpleField(SCHEMA_VERSION_STR);
    return schemaVersionStr == null ? 0 : Integer.valueOf(schemaVersionStr);
  }

  /**
   * Get the list of sealed replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no sealed replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of sealed replicas.
   */
  static List<String> getSealedReplicas(InstanceConfig instanceConfig) {
    List<String> sealedReplicas = instanceConfig.getRecord().getListField(ClusterMapUtils.SEALED_STR);
    return sealedReplicas == null ? Collections.emptyList() : sealedReplicas;
  }

  /**
   * Get the list of stopped replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no stopped replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of stopped replicas.
   */
  static List<String> getStoppedReplicas(InstanceConfig instanceConfig) {
    List<String> stoppedReplicas = instanceConfig.getRecord().getListField(ClusterMapUtils.STOPPED_REPLICAS_STR);
    return stoppedReplicas == null ? Collections.emptyList() : stoppedReplicas;
  }

  /**
   * Get the rack id associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the rack id associated with the given instance.
   */
  static String getRackId(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getSimpleField(RACKID_STR);
  }

  /**
   * Get the datacenter name associated with the given instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the datacenter name associated with the given instance.
   */
  static String getDcName(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getSimpleField(DATACENTER_STR);
  }

  /**
   * Get the ssl port associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the ssl port associated with the given instance.
   */
  static Integer getSslPortStr(InstanceConfig instanceConfig) {
    String sslPortStr = instanceConfig.getRecord().getSimpleField(SSLPORT_STR);
    return sslPortStr == null ? null : Integer.valueOf(sslPortStr);
  }

  /**
   * Get the xid associated with this instance. The xid is like a timestamp or a change number, so if it is absent,
   * a value representing the earliest point in time is returned.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the xid associated with the given instance.
   */
  static long getXid(InstanceConfig instanceConfig) {
    String xid = instanceConfig.getRecord().getSimpleField(XID_STR);
    return xid == null ? DEFAULT_XID : Long.valueOf(xid);
  }

  /**
   * Converts a hostname into a canonical hostname.
   *
   * @param unqualifiedHostname hostname to be fully qualified
   * @return canonical hostname that can be compared with DataNode.getHostname()
   */
  static String getFullyQualifiedDomainName(String unqualifiedHostname) {
    if (unqualifiedHostname == null) {
      throw new IllegalStateException("Hostname cannot be null.");
    } else if (unqualifiedHostname.length() == 0) {
      throw new IllegalStateException("Hostname cannot be zero length.");
    }

    try {
      return InetAddress.getByName(unqualifiedHostname).getCanonicalHostName().toLowerCase();
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
          "Host (" + unqualifiedHostname + ") is unknown so cannot determine fully qualified domain name.");
    }
  }

  /**
   * Serialize and return the arguments.
   * @param shortValue a Short value.
   * @param longValue a Long value.
   * @return the serialized byte array.
   */
  static byte[] serializeShortAndLong(Short shortValue, Long longValue) {
    ByteBuffer buffer = ByteBuffer.allocate(Short.SIZE / Byte.SIZE + Long.SIZE / Byte.SIZE);
    buffer.putShort(shortValue);
    buffer.putLong(longValue);
    return buffer.array();
  }

  /**
   * Validate the replica capacity.
   * @param replicaCapacityInBytes the replica capacity to validate.
   */
  static void validateReplicaCapacityInBytes(long replicaCapacityInBytes) {
    if (replicaCapacityInBytes < MIN_REPLICA_CAPACITY_IN_BYTES) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + replicaCapacityInBytes + " is less than " + MIN_REPLICA_CAPACITY_IN_BYTES);
    } else if (replicaCapacityInBytes > MAX_REPLICA_CAPACITY_IN_BYTES) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + replicaCapacityInBytes + " is more than " + MAX_REPLICA_CAPACITY_IN_BYTES);
    }
  }

  /**
   * Validate the disk capacity.
   * @param diskCapacityInBytes the disk capacity to validate.
   */
  static void validateDiskCapacity(long diskCapacityInBytes) {
    if (diskCapacityInBytes < MIN_DISK_CAPACITY_IN_BYTES) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + diskCapacityInBytes + " is less than " + MIN_DISK_CAPACITY_IN_BYTES);
    } else if (diskCapacityInBytes > MAX_DISK_CAPACITY_IN_BYTES) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + diskCapacityInBytes + " is more than " + MAX_DISK_CAPACITY_IN_BYTES);
    }
  }

  /**
   * Check whether all replicas of the given {@link PartitionId} are up.
   * @param partition the {@link PartitionId} to check.
   * @return true if all associated replicas are up; false otherwise.
   */
  static boolean areAllReplicasForPartitionUp(PartitionId partition) {
    for (ReplicaId replica : partition.getReplicaIds()) {
      if (replica.isDown()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Helper class to perform common operations like maintaining partitions by partition class and returning all/writable
   * partitions.
   * <p/>
   * Not thread safe.
   */
  static class PartitionSelectionHelper {
    private Collection<? extends PartitionId> allPartitions;
    private Map<String, SortedMap<Integer, List<PartitionId>>> partitionIdsByClassAndLocalReplicaCount;

    /**
     * @param allPartitions the list of all {@link PartitionId}s
     * @param localDatacenterName the name of the local datacenter. Can be null if datacenter specific replica counts
     *                            are not required.
     */
    PartitionSelectionHelper(Collection<? extends PartitionId> allPartitions, String localDatacenterName) {
      updatePartitions(allPartitions, localDatacenterName);
    }

    /**
     * Updates the partitions tracked by this helper.
     * @param allPartitions the list of all {@link PartitionId}s
     * @param localDatacenterName the name of the local datacenter. Can be null if datacenter specific replica counts
     *                            are not required.
     */
    void updatePartitions(Collection<? extends PartitionId> allPartitions, String localDatacenterName) {
      this.allPartitions = allPartitions;
      partitionIdsByClassAndLocalReplicaCount = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (PartitionId partition : allPartitions) {
        String partitionClass = partition.getPartitionClass();
        int localReplicaCount = 0;
        for (ReplicaId replicaId : partition.getReplicaIds()) {
          if (localDatacenterName != null && !localDatacenterName.isEmpty() && replicaId.getDataNodeId()
              .getDatacenterName()
              .equals(localDatacenterName)) {
            localReplicaCount++;
          }
        }
        SortedMap<Integer, List<PartitionId>> replicaCountToPartitionIds =
            partitionIdsByClassAndLocalReplicaCount.computeIfAbsent(partitionClass, key -> new TreeMap<>());
        replicaCountToPartitionIds.computeIfAbsent(localReplicaCount, key -> new ArrayList<>()).add(partition);
      }
    }

    /**
     * Gets all partitions belonging to the {@code paritionClass} (all partitions if it is {@code null}).
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @return all the partitions in {@code partitionClass} (all partitions if it is {@code null})
     */
    List<PartitionId> getPartitions(String partitionClass) {
      return getPartitionsInClass(partitionClass, false);
    }

    /**
     * Returns all the partitions that are in state {@link PartitionState#READ_WRITE} AND have the highest number of
     * replicas in the local datacenter for the given {@code partitionClass}.
     * <p/>
     * Also attempts to return only partitions with healthy replicas but if no such partitions are found, returns all
     * the eligible partitions irrespective of replica health.
     * <p/>
     * If {@code partitionClass} is {@code null}, gets writable partitions from all partition classes.
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @return all the writable partitions in {@code partitionClass} with the highest replica count in the local
     * datacenter (all writable partitions if it is {@code null}).
     */
    List<PartitionId> getWritablePartitions(String partitionClass) {
      List<PartitionId> writablePartitions = new ArrayList<>();
      List<PartitionId> healthyWritablePartitions = new ArrayList<>();
      for (PartitionId partition : getPartitionsInClass(partitionClass, true)) {
        if (partition.getPartitionState() == PartitionState.READ_WRITE) {
          writablePartitions.add(partition);
          if (areAllReplicasForPartitionUp((partition))) {
            healthyWritablePartitions.add(partition);
          }
        }
      }
      return healthyWritablePartitions.isEmpty() ? writablePartitions : healthyWritablePartitions;
    }

    /**
     * Returns the partitions belonging to the {@code partitionClass}. Returns all partitions if {@code partitionClass}
     * is {@code null}.
     * @param partitionClass the class of the partitions desired.
     * @param highestReplicaCountOnly if {@code true}, returns only the partitions with the highest number of replicas
     *                                in the local datacenter.
     * @return the partitions belonging to the {@code partitionClass}. Returns all partitions if {@code partitionClass}
     * is {@code null}.
     */
    private List<PartitionId> getPartitionsInClass(String partitionClass, boolean highestReplicaCountOnly) {
      List<PartitionId> toReturn = new ArrayList<>();
      if (partitionClass == null) {
        toReturn.addAll(allPartitions);
      } else if (partitionIdsByClassAndLocalReplicaCount.containsKey(partitionClass)) {
        SortedMap<Integer, List<PartitionId>> partitionsByReplicaCount =
            partitionIdsByClassAndLocalReplicaCount.get(partitionClass);
        if (highestReplicaCountOnly) {
          toReturn.addAll(partitionsByReplicaCount.get(partitionsByReplicaCount.lastKey()));
        } else {
          for (List<PartitionId> partitionIds : partitionIdsByClassAndLocalReplicaCount.get(partitionClass).values()) {
            toReturn.addAll(partitionIds);
          }
        }
      } else {
        throw new IllegalArgumentException("Unrecognized partition class " + partitionClass);
      }
      return toReturn;
    }
  }
}

