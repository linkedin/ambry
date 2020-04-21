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

import com.github.ambry.network.Port;
import com.github.ambry.utils.Utils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class with clustermap related utility methods for use by other classes.
 */
public class ClusterMapUtils {
  // datacenterId == UNKNOWN_DATACENTER_ID indicate datacenterId is not available at the time when this blobId is formed.
  public static final byte UNKNOWN_DATACENTER_ID = -1;
  public static final String PARTITION_OVERRIDE_STR = "PartitionOverride";
  public static final String REPLICA_ADDITION_STR = "ReplicaAddition";
  public static final String PROPERTYSTORE_STR = "PROPERTYSTORE";
  // Following two ZNode paths are the path to ZNode that stores some admin configs. Partition override config is used
  // to administratively override partition from frontend's point of view. Replica addition config is used to specify
  // detailed new replica infos (capacity, mount path, etc) which will be added to target server.
  // Note that, root path in Helix is "/ClusterName/PROPERTYSTORE", so the full path is (use partition override as example)
  // "/ClusterName/PROPERTYSTORE/AdminConfigs/PartitionOverride"
  public static final String PARTITION_OVERRIDE_ZNODE_PATH = "/AdminConfigs/" + PARTITION_OVERRIDE_STR;
  public static final String REPLICA_ADDITION_ZNODE_PATH = "/AdminConfigs/" + REPLICA_ADDITION_STR;
  static final String DISK_CAPACITY_STR = "capacityInBytes";
  static final String DISK_STATE = "diskState";
  static final String PARTITION_STATE = "state";
  static final String PARTITION_CLASS_STR = "partitionClass";
  static final String REPLICAS_STR = "Replicas";
  static final String REPLICAS_DELIM_STR = ",";
  static final String REPLICAS_STR_SEPARATOR = ":";
  static final String REPLICAS_CAPACITY_STR = "replicaCapacityInBytes";
  static final String REPLICA_TYPE_STR = "replicaType";
  static final String SSL_PORT_STR = "sslPort";
  static final String HTTP2_PORT_STR = "http2Port";
  static final String RACKID_STR = "rackId";
  static final String SEALED_STR = "SEALED";
  static final String STOPPED_REPLICAS_STR = "STOPPED";
  static final String AVAILABLE_STR = "AVAILABLE";
  static final String READ_ONLY_STR = "RO";
  static final String READ_WRITE_STR = "RW";
  static final String ZKCONNECT_STR = "zkConnectStr";
  static final String ZKCONNECT_STR_DELIMITER = ",";
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
  private static final Logger logger = LoggerFactory.getLogger(ClusterMapUtils.class);

  /**
   * Stores all zk related info for a DC.
   */
  public static class DcZkInfo {
    private final String dcName;
    private final byte dcId;
    private final List<String> zkConnectStrs;
    private final ReplicaType replicaType;

    /**
     * Construct a DcInfo object with the given parameters.
     * @param dcName the associated datacenter name.
     * @param dcId the associated datacenter ID.
     * @param zkConnectStrs the associated ZK connect strings for this datacenter. (Usually there should be only one ZK
     *                      endpoint but in special case we allow multiple ZK endpoints in same dc)
     * @param replicaType the type of replicas (cloud or disk backed) present in this datacenter.
     */
    DcZkInfo(String dcName, byte dcId, List<String> zkConnectStrs, ReplicaType replicaType) {
      this.dcName = dcName;
      this.dcId = dcId;
      this.zkConnectStrs = zkConnectStrs;
      this.replicaType = replicaType;
    }

    public String getDcName() {
      return dcName;
    }

    public byte getDcId() {
      return dcId;
    }

    public List<String> getZkConnectStrs() {
      return zkConnectStrs;
    }

    public ReplicaType getReplicaType() {
      return replicaType;
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
      String name = entry.getString(DATACENTER_STR);
      byte id = (byte) entry.getInt(DATACENTER_ID_STR);
      ReplicaType replicaType = entry.optEnum(ReplicaType.class, REPLICA_TYPE_STR, ReplicaType.DISK_BACKED);
      ArrayList<String> zkConnectStrs =
          (replicaType == ReplicaType.DISK_BACKED) ? Utils.splitString(entry.getString(ZKCONNECT_STR),
              ZKCONNECT_STR_DELIMITER) : Utils.splitString(entry.optString(ZKCONNECT_STR), ZKCONNECT_STR_DELIMITER);
      DcZkInfo dcZkInfo = new DcZkInfo(name, id, zkConnectStrs, replicaType);
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
    return schemaVersionStr == null ? 0 : Integer.parseInt(schemaVersionStr);
  }

  /**
   * Get the list of sealed replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no sealed replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of sealed replicas.
   */
  static List<String> getSealedReplicas(InstanceConfig instanceConfig) {
    List<String> sealedReplicas = instanceConfig.getRecord().getListField(ClusterMapUtils.SEALED_STR);
    return sealedReplicas == null ? new ArrayList<>() : sealedReplicas;
  }

  /**
   * Get the list of stopped replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no stopped replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of stopped replicas.
   */
  static List<String> getStoppedReplicas(InstanceConfig instanceConfig) {
    List<String> stoppedReplicas = instanceConfig.getRecord().getListField(ClusterMapUtils.STOPPED_REPLICAS_STR);
    return stoppedReplicas == null ? new ArrayList<>() : stoppedReplicas;
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
  public static String getDcName(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getSimpleField(DATACENTER_STR);
  }

  /**
   * Get the ssl port associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the ssl port associated with the given instance.
   */
  public static Integer getSslPortStr(InstanceConfig instanceConfig) {
    String sslPortStr = instanceConfig.getRecord().getSimpleField(SSL_PORT_STR);
    return sslPortStr == null ? null : Integer.valueOf(sslPortStr);
  }

  /**
   * Get the http2 port associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the http2 port associated with the given instance.
   */
  public static Integer getHttp2PortStr(InstanceConfig instanceConfig) {
    String http2PortStr = instanceConfig.getRecord().getSimpleField(HTTP2_PORT_STR);
    return http2PortStr == null ? null : Integer.valueOf(http2PortStr);
  }

  /**
   * Get the xid associated with this instance. The xid is like a timestamp or a change number, so if it is absent,
   * a value representing the earliest point in time is returned.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the xid associated with the given instance.
   */
  static long getXid(InstanceConfig instanceConfig) {
    String xid = instanceConfig.getRecord().getSimpleField(XID_STR);
    return xid == null ? DEFAULT_XID : Long.parseLong(xid);
  }

  /**
   * Converts a hostname into a canonical hostname.
   *
   * @param unqualifiedHostname hostname to be fully qualified
   * @return canonical hostname that can be compared with DataNode.getHostname()
   */
  public static String getFullyQualifiedDomainName(String unqualifiedHostname) {
    if (unqualifiedHostname == null) {
      throw new IllegalArgumentException("Hostname cannot be null.");
    } else if (unqualifiedHostname.length() == 0) {
      throw new IllegalArgumentException("Hostname cannot be zero length.");
    }

    try {
      return InetAddress.getByName(unqualifiedHostname).getCanonicalHostName().toLowerCase();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(
          "Host (" + unqualifiedHostname + ") is unknown so cannot determine fully qualified domain name.");
    }
  }

  /**
   * Validate hostName.
   * @param clusterMapResolveHostnames indicates if a reverse DNS lookup is enabled or not.
   * @param hostName hostname to be validated.
   * @throws IllegalArgumentException if hostname is not valid.
   */
  public static void validateHostName(Boolean clusterMapResolveHostnames, String hostName) {
    if (clusterMapResolveHostnames) {
      String fqdn = getFullyQualifiedDomainName(hostName);
      if (!fqdn.equals(hostName)) {
        throw new IllegalArgumentException(
            "Hostname(" + hostName + ") does not match its fully qualified domain name: " + fqdn);
      }
    }
  }

  /**
   * Validate plainTextPort, sslPort and http2Port.
   * @param plainTextPort PlainText {@link Port}.
   * @param sslPort SSL {@link Port}.
   * @param http2Port HTTP2 SSL {@link Port}.
   * @param sslRequired if ssl encrypted port needed.
   * @throws IllegalArgumentException if ports are not valid.
   */
  public static void validatePorts(Port plainTextPort, Port sslPort, Port http2Port, boolean sslRequired) {
    if (sslRequired && sslPort == null && http2Port == null) {
      throw new IllegalArgumentException("No SSL port to a data node to which SSL is enabled.");
    }

    if (plainTextPort.getPort() < MIN_PORT || plainTextPort.getPort() > MAX_PORT) {
      throw new IllegalArgumentException(
          "PlainText Port " + plainTextPort.getPort() + " not in valid range [" + MIN_PORT + " - " + MAX_PORT + "]");
    }
    if (!sslRequired) {
      return;
    }
    // check ports duplication
    Set<Integer> ports = new HashSet<Integer>();
    ports.add(plainTextPort.getPort());

    if (sslPort != null) {
      if (sslPort.getPort() < MIN_PORT || sslPort.getPort() > MAX_PORT) {
        throw new IllegalArgumentException(
            "SSL Port " + sslPort.getPort() + " not in valid range [" + MIN_PORT + " - " + MAX_PORT + "]");
      }
      if (!ports.add(sslPort.getPort())) {
        throw new IllegalArgumentException("Port number duplication found. " + ports);
      }
    }

    if (http2Port != null) {
      if (http2Port.getPort() < MIN_PORT || http2Port.getPort() > MAX_PORT) {
        throw new IllegalArgumentException(
            "HTTP2 Port " + http2Port.getPort() + " not in valid range [" + MIN_PORT + " - " + MAX_PORT + "]");
      }
      if (!ports.add(http2Port.getPort())) {
        throw new IllegalArgumentException("Port number duplication found. " + ports);
      }
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
        logger.debug("Replica [{}] on {} {} is down", replica.getPartitionId().toPathString(),
            replica.getDataNodeId().getHostname(), replica.getMountPath());
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
  static class PartitionSelectionHelper implements ClusterMapChangeListener {
    private final int minimumLocalReplicaCount;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ClusterManagerCallback<?, ?, ?, ?> clusterManagerCallback;
    private Collection<? extends PartitionId> allPartitions;
    private Map<String, SortedMap<Integer, List<PartitionId>>> partitionIdsByClassAndLocalReplicaCount;
    private Map<PartitionId, List<ReplicaId>> partitionIdToLocalReplicas;
    private String localDatacenterName;

    /**
     * @param clusterManagerCallback the {@link ClusterManagerCallback} to query current cluster info
     * @param localDatacenterName the name of the local datacenter. Can be null if datacenter specific replica counts
     * @param minimumLocalReplicaCount the minimum number of replicas in local datacenter. This is used when selecting
     */
    PartitionSelectionHelper(ClusterManagerCallback<?, ?, ?, ?> clusterManagerCallback, String localDatacenterName,
        int minimumLocalReplicaCount) {
      this.localDatacenterName = localDatacenterName;
      this.minimumLocalReplicaCount = minimumLocalReplicaCount;
      this.clusterManagerCallback = clusterManagerCallback;
      updatePartitions(clusterManagerCallback.getPartitions(), localDatacenterName);
    }

    /**
     * Updates the partitions tracked by this helper.
     * @param allPartitions the list of all {@link PartitionId}s
     * @param localDatacenterName the name of the local datacenter. Can be null if datacenter specific replica counts
     *                            are not required.
     */
    void updatePartitions(Collection<? extends PartitionId> allPartitions, String localDatacenterName) {
      // since this method is called during initialization only, we don't really need read-write lock here.
      this.allPartitions = allPartitions;
      partitionIdsByClassAndLocalReplicaCount = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      partitionIdToLocalReplicas = new HashMap<>();
      populatePartitionAndLocalReplicaMaps(allPartitions, partitionIdsByClassAndLocalReplicaCount,
          partitionIdToLocalReplicas, localDatacenterName);
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
        } else {
          logger.debug("{} is in READ_ONLY state, skipping it", partition.toString());
        }
      }
      return healthyWritablePartitions.isEmpty() ? writablePartitions : healthyWritablePartitions;
    }

    /**
     * Returns a writable partition selected at random, that belongs to the specified partition class and that is in the
     * state {@link PartitionState#READ_WRITE} AND has enough replicas up. In case none of the partitions have enough
     * replicas up, any writable partition is returned. Enough replicas is considered to be all local replicas if such
     * information is available. In case localDatacenterName is not available, all of the partition's replicas should be up.
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @param partitionsToExclude partitions that should be excluded from the result. Can be {@code null} or empty.
     * @return A writable partition or {@code null} if no writable partition with given criteria found
     */
    PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
      PartitionId anyWritablePartition = null;
      rwLock.readLock().lock();
      try {
        List<PartitionId> partitionsInClass = getPartitionsInClass(partitionClass, true);
        int workingSize = partitionsInClass.size();
        while (workingSize > 0) {
          int randomIndex = ThreadLocalRandom.current().nextInt(workingSize);
          PartitionId selected = partitionsInClass.get(randomIndex);
          if (partitionsToExclude == null || partitionsToExclude.size() == 0 || !partitionsToExclude.contains(
              selected)) {
            if (selected.getPartitionState() == PartitionState.READ_WRITE) {
              anyWritablePartition = selected;
              if (areEnoughReplicasForPartitionUp(selected)) {
                return selected;
              }
            }
          }
          if (randomIndex != workingSize - 1) {
            partitionsInClass.set(randomIndex, partitionsInClass.get(workingSize - 1));
          }
          workingSize--;
        }
        //if we are here then that means we couldn't find any partition with all local replicas up
        return anyWritablePartition;
      } finally {
        rwLock.readLock().unlock();
      }
    }

    /**
     * Check whether the parition has has enough replicas up for write operations to try. Enough replicas is considered
     * to be all local replicas if such information is available. In case localDatacenterName is not available, all of
     * the partition's replicas should be up.
     * @param partitionId the {@link PartitionId} to check.
     * @return true if enough replicas are up; false otherwise.
     */
    private boolean areEnoughReplicasForPartitionUp(PartitionId partitionId) {
      if (localDatacenterName != null && !localDatacenterName.isEmpty()) {
        return areAllLocalReplicasForPartitionUp(partitionId);
      } else {
        return areAllReplicasForPartitionUp(partitionId);
      }
    }

    /**
     * Check whether all local replicas of the given {@link PartitionId} are up.
     * @param partitionId the {@link PartitionId} to check.
     * @return true if all local replicas are up; false otherwise.
     */
    private boolean areAllLocalReplicasForPartitionUp(PartitionId partitionId) {
      for (ReplicaId replica : partitionIdToLocalReplicas.get(partitionId)) {
        if (replica.isDown()) {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns the partitions belonging to the {@code partitionClass}. Returns all partitions if {@code partitionClass}
     * is {@code null}.
     * @param partitionClass the class of the partitions desired.
     * @param minimumReplicaCountRequired if {@code true}, returns only the partitions with the number of replicas in
     *                                    local datacenter that is larger than or equal to minimum required count.
     * @return the partitions belonging to the {@code partitionClass}. Returns all partitions if {@code partitionClass}
     * is {@code null}.
     */
    private List<PartitionId> getPartitionsInClass(String partitionClass, boolean minimumReplicaCountRequired) {
      List<PartitionId> toReturn = new ArrayList<>();
      rwLock.readLock().lock();
      try {
        if (partitionClass == null) {
          toReturn.addAll(allPartitions);
        } else if (partitionIdsByClassAndLocalReplicaCount.containsKey(partitionClass)) {
          SortedMap<Integer, List<PartitionId>> partitionsByReplicaCount =
              partitionIdsByClassAndLocalReplicaCount.get(partitionClass);
          if (minimumReplicaCountRequired) {
            // get partitions with replica count >= min replica count specified in ClusterMapConfig
            for (List<PartitionId> partitionIds : partitionsByReplicaCount.tailMap(minimumLocalReplicaCount).values()) {
              toReturn.addAll(partitionIds);
            }
          } else {
            for (List<PartitionId> partitionIds : partitionIdsByClassAndLocalReplicaCount.get(partitionClass)
                .values()) {
              toReturn.addAll(partitionIds);
            }
          }
        } else {
          throw new IllegalArgumentException("Unrecognized partition class " + partitionClass);
        }
      } finally {
        rwLock.readLock().unlock();
      }
      return toReturn;
    }

    @Override
    public void onReplicaAddedOrRemoved(List<ReplicaId> addedReplicas, List<ReplicaId> removedReplicas) {
      // For now, the simplest and safest way is to re-populate "partitionIdsByClassAndLocalReplicaCount" and
      // "partitionIdToLocalReplicas" maps. Since cluster expansion and replica movement should be infrequent, we should
      // be fine with the overhead of re-populating these two maps.
      // No matter whether this method is called by local dc's or remote dcs' cluster change handler, we need to populate
      // "allPartitions" again because there may be some new partitions with special class added to remote dc only.
      generatePartitionMapsAndSwitch(addedReplicas, removedReplicas);
    }

    /**
     * Generate new partition-selection related maps and switch current maps to new ones. This method should be synchronized
     * because it can be invoked by cluster change handlers in different dcs concurrently. We need to ensure one dc has
     * completely updated in-mem data structures and then move on to the next.
     * @param addedReplicas a list of replicas are newly added in cluster
     * @param removedReplicas a list of replicas have been removed from cluster.
     */
    private synchronized void generatePartitionMapsAndSwitch(List<ReplicaId> addedReplicas,
        List<ReplicaId> removedReplicas) {
      String dcName = addedReplicas.isEmpty() ? removedReplicas.get(0).getDataNodeId().getDatacenterName()
          : addedReplicas.get(0).getDataNodeId().getDatacenterName();
      logger.info("Re-populating partition-selection related maps because replicas are added or removed in {}", dcName);
      // condition here, remember this method can be invoked by multiple threads (synchronize this method?)
      Collection<? extends PartitionId> partitionsInCluster = clusterManagerCallback.getPartitions();
      Map<String, SortedMap<Integer, List<PartitionId>>> partitionSortedByReplicaCount =
          new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      Map<PartitionId, List<ReplicaId>> partitionAndLocalReplicas = new HashMap<>();
      populatePartitionAndLocalReplicaMaps(partitionsInCluster, partitionSortedByReplicaCount,
          partitionAndLocalReplicas, localDatacenterName);
      rwLock.writeLock().lock();
      // switch references to newly generated maps
      try {
        allPartitions = partitionsInCluster;
        partitionIdsByClassAndLocalReplicaCount = partitionSortedByReplicaCount;
        partitionIdToLocalReplicas = partitionAndLocalReplicas;
      } finally {
        rwLock.writeLock().unlock();
      }
      logger.info("Partition-selection related maps updated after replica changes in {}", dcName);
    }

    /**
     * Populate partition-selection related maps that track partition and its replicas in local dc.
     * @param allPartitions all the partitions in cluster.
     * @param partitionIdsByClassAndLocalReplicaCount a map that tracks partitions sorted by local replica count.
     * @param partitionIdToLocalReplicas a map that tracks partition to its local replicas.
     * @param localDatacenterName the name of local dc. Can be null if dc specific replica counts are not required.
     */
    private void populatePartitionAndLocalReplicaMaps(Collection<? extends PartitionId> allPartitions,
        Map<String, SortedMap<Integer, List<PartitionId>>> partitionIdsByClassAndLocalReplicaCount,
        Map<PartitionId, List<ReplicaId>> partitionIdToLocalReplicas, String localDatacenterName) {
      for (PartitionId partition : allPartitions) {
        String partitionClass = partition.getPartitionClass();
        int localReplicaCount = 0;
        for (ReplicaId replicaId : partition.getReplicaIds()) {
          if (localDatacenterName != null && !localDatacenterName.isEmpty() && replicaId.getDataNodeId()
              .getDatacenterName()
              .equals(localDatacenterName)) {
            partitionIdToLocalReplicas.computeIfAbsent(partition, key -> new LinkedList<>()).add(replicaId);
            localReplicaCount++;
          }
        }
        SortedMap<Integer, List<PartitionId>> replicaCountToPartitionIds =
            partitionIdsByClassAndLocalReplicaCount.computeIfAbsent(partitionClass, key -> new TreeMap<>());
        replicaCountToPartitionIds.computeIfAbsent(localReplicaCount, key -> new ArrayList<>()).add(partition);
      }
    }
  }
}
