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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.frontend.ReservedMetadataIdMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class with clustermap related utility methods for use by other classes.
 */
public class ClusterMapUtils {
  public static final String PARTITION_OVERRIDE_STR = "PartitionOverride";
  public static final String REPLICA_ADDITION_STR = "ReplicaAddition";
  public static final String PARTITION_DISABLED_STR = "PartitionDisabled";
  public static final String FULL_AUTO_MIGRATION_STR = "FullAutoMigration";
  public static final String PROPERTYSTORE_STR = "PROPERTYSTORE";
  // Following two ZNode paths are the path to ZNode that stores some admin configs. Partition override config is used
  // to administratively override partition from frontend's point of view. Replica addition config is used to specify
  // detailed new replica infos (capacity, mount path, etc) which will be added to target server.
  // Note that, root path in Helix is "/ClusterName/PROPERTYSTORE", so the full path is (use partition override as example)
  // "/ClusterName/PROPERTYSTORE/AdminConfigs/PartitionOverride"
  public static final String ADMIN_CONFIG_ZNODE_PATH = "/AdminConfigs/";
  public static final String PARTITION_OVERRIDE_ZNODE_PATH = ADMIN_CONFIG_ZNODE_PATH + PARTITION_OVERRIDE_STR;
  public static final String REPLICA_ADDITION_ZNODE_PATH = ADMIN_CONFIG_ZNODE_PATH + REPLICA_ADDITION_STR;
  public static final String PARTITION_DISABLED_ZNODE_PATH = ADMIN_CONFIG_ZNODE_PATH + PARTITION_DISABLED_STR + "/";
  public static final String FULL_AUTO_MIGRATION_ZNODE_PATH = ADMIN_CONFIG_ZNODE_PATH + FULL_AUTO_MIGRATION_STR;
  static final String DISK_KEY = "DISK";
  static final String DISK_CAPACITY_STR = "capacityInBytes";
  static final String DISK_STATE = "diskState";
  static final String PARTITION_STATE = "state";
  static final String PARTITION_CLASS_STR = "partitionClass";
  static final String REPLICAS_STR = "Replicas";
  static final String REPLICAS_DELIM_STR = ",";
  static final String REPLICAS_STR_SEPARATOR = ":";
  static final String REPLICAS_CAPACITY_STR = "replicaCapacityInBytes";
  static final String REPLICA_TYPE_STR = "replicaType";
  public static final String SSL_PORT_STR = "sslPort";
  public static final String HTTP2_PORT_STR = "http2Port";
  static final String RACKID_STR = "rackId";
  static final String SEALED_STR = "SEALED";
  static final String PARTIALLY_SEALED_STR = "PARTIALLY_SEALED";
  static final String STOPPED_REPLICAS_STR = "STOPPED";
  static final String DISABLED_REPLICAS_STR = "DISABLED";
  static final String RESOURCES_STR = "RESOURCES";
  static final String AVAILABLE_STR = "AVAILABLE";
  static final String UNAVAILABLE_STR = "UNAVAILABLE";
  static final String READ_ONLY_STR = "RO";
  static final String READ_WRITE_STR = "RW";
  static final String PARTIAL_READ_WRITE_STR = "PRW";
  static final String ZKCONNECT_STR = "zkConnectStr";
  static final String ZKCONNECT_STR_DELIMITER = ",";
  static final String ZKINFO_STR = "zkInfo";
  static final String DATACENTER_STR = "datacenter";
  static final String DATACENTER_ID_STR = "id";
  static final String SCHEMA_VERSION_STR = "schemaVersion";
  static final String XID_STR = "xid";
  static final String DISK_CAPACITY_DELIM_STR = ",";
  static final long DEFAULT_XID = Long.MIN_VALUE;
  static final int MIN_PORT = 1025;
  static final int MAX_PORT = 65535;
  public static long MIN_REPLICA_CAPACITY_IN_BYTES = 1024 * 1024 * 1024L;
  static final long MAX_REPLICA_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024 * 1024;
  static final long MIN_DISK_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024;
  static final int CURRENT_SCHEMA_VERSION = 0;
  static final String WRITABLE_LOG_STR = "writable";
  static final String FULLY_WRITABLE_LOG_STR = "fully writable";
  private static final String INSTANCE_NAME_DELIMITER = "_";
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
    return port == null ? host : host + INSTANCE_NAME_DELIMITER + port;
  }

  /**
   * Construct and return the instance name for given data node id
   * @param dataNodeId the {@link DataNodeId} object.
   * @return the constructed instance name.
   */
  public static String getInstanceName(DataNodeId dataNodeId) {
    return getInstanceName(dataNodeId.getHostname(), dataNodeId.getPort());
  }

  public static String getPortFromInstanceName(String instanceName) {
    return instanceName.contains(INSTANCE_NAME_DELIMITER) ? instanceName.split(INSTANCE_NAME_DELIMITER)[1] : null;
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
   * Returns the replica's {@link PartitionState} based on the specified {@link ReplicaSealStatus}.
   * @param replicaSealStatus {@link ReplicaSealStatus} of the replica.
   * @return PartitionState object.
   */
  public static PartitionState convertReplicaSealStatusToPartitionState(ReplicaSealStatus replicaSealStatus) {
    PartitionState partitionState = null;
    switch (replicaSealStatus) {
      case SEALED:
        partitionState = PartitionState.READ_ONLY;
        break;
      case PARTIALLY_SEALED:
        partitionState = PartitionState.PARTIAL_READ_WRITE;
        break;
      case NOT_SEALED:
        partitionState = PartitionState.READ_WRITE;
        break;
    }
    return partitionState;
  }

  /**
   * Returns the replica's {@link ReplicaSealStatus} based on the specified {@link PartitionState}.
   * @param partitionState {@link PartitionState} object.
   * @return ReplicaSealStatus object.
   */
  public static ReplicaSealStatus convertPartitionStateToReplicaSealSatus(PartitionState partitionState) {
    ReplicaSealStatus replicaSealStatus = null;
    switch (partitionState) {
      case READ_ONLY:
        replicaSealStatus = ReplicaSealStatus.SEALED;
        break;
      case PARTIAL_READ_WRITE:
        replicaSealStatus = ReplicaSealStatus.PARTIALLY_SEALED;
        break;
      case READ_WRITE:
        replicaSealStatus = ReplicaSealStatus.NOT_SEALED;
        break;
    }
    return replicaSealStatus;
  }

  /**
   * Choose a random {@link PartitionId} for putting the metadata chunk and return a {@link BlobId} that belongs to the
   * chosen partition id.
   * @param partitionClass the partition class to choose partitions from.
   * @param partitionIdsToExclude the list of {@link PartitionId}s that should be excluded from consideration.
   * @param reservedMetadataIdMetrics the {@link ReservedMetadataIdMetrics} object.
   * @param clusterMap the {@link ClusterMap} object.
   * @param accountId the account id to be encoded in the blob id.
   * @param containerId the container id to be encoded in the blob id.
   * @param isEncrypted {@code true} is the blob is encrypted. {@code false} otherwise.
   * @param routerConfig the {@link RouterConfig} object.
   * @return the chosen {@link BlobId}.
   */
  public static BlobId reserveMetadataBlobId(String partitionClass, List<PartitionId> partitionIdsToExclude,
      ReservedMetadataIdMetrics reservedMetadataIdMetrics, ClusterMap clusterMap, short accountId, short containerId,
      boolean isEncrypted, RouterConfig routerConfig) {
    PartitionId selected = clusterMap.getRandomFullyWritablePartition(partitionClass, partitionIdsToExclude);
    if (selected == null) {
      reservedMetadataIdMetrics.numFailedPartitionReserveAttempts.inc();
      return null;
    }
    if (!partitionClass.equals(selected.getPartitionClass())) {
      logger.debug(
          "While reserving metadata chunk id, no partitions for partitionClass='{}' found, partitionClass='{}' used"
              + " instead for metadata chunk.", partitionClass, selected.getPartitionClass());
      reservedMetadataIdMetrics.numUnexpectedReservedPartitionClassCount.inc();
    }
    return new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        clusterMap.getLocalDatacenterId(), accountId, containerId, selected, isEncrypted,
        BlobId.BlobDataType.METADATA);
  }

  /**
   * Return the partition class for the specified {@link Account} and {@link Container}.
   * If the partition class for the Account and Container is not specified or cannot be determined, then return the
   * specified defaultPartitionClass.
   * @param account the {@link Account} object.
   * @param container the {@link Container} object.
   * @param defaultPartitionClass the default partition class.
   * @return the partition class as required by the parameters.
   */
  public static String getPartitionClass(Account account, Container container, String defaultPartitionClass) {
    String partitionClass = defaultPartitionClass;
    if (account != null) {
      if (container != null && !Utils.isNullOrEmpty(container.getReplicationPolicy())) {
        partitionClass = container.getReplicationPolicy();
      }
    }
    return partitionClass;
  }


  /**
   * Get the schema version associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the schema version of the information stored. If the field is absent in the InstanceConfig, the version
   *         is assumed to be 0, and 0 is returned.
   */
  static int getSchemaVersion(InstanceConfig instanceConfig) {
    return getSchemaVersion(instanceConfig.getRecord());
  }

  /**
   * Get the schema version associated with the given instance (if any).
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the schema version of the information stored. If the field is absent in the InstanceConfig, the version
   *         is assumed to be 0, and 0 is returned.
   */
  static int getSchemaVersion(ZNRecord znRecord) {
    String schemaVersionStr = znRecord.getSimpleField(SCHEMA_VERSION_STR);
    return schemaVersionStr == null ? 0 : Integer.parseInt(schemaVersionStr);
  }

  /**
   * Get the list of sealed replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no sealed replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of sealed replicas.
   */
  static List<String> getSealedReplicas(InstanceConfig instanceConfig) {
    return getSealedReplicas(instanceConfig.getRecord());
  }

  /**
   * Get the list of partially sealed replicas on a given instance.
   * This is guaranteed to return a non-null list. It would return an empty list if there are no sealed replicas or if
   * the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of partially sealed replicas.
   */
  static List<String> getPartiallySealedReplicas(InstanceConfig instanceConfig) {
    return getPartiallySealedReplicas(instanceConfig.getRecord());
  }

  /**
   * Get the list of sealed replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no sealed replicas or if the field itself is absent for this instance.
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the list of sealed replicas.
   */
  static List<String> getSealedReplicas(ZNRecord znRecord) {
    List<String> sealedReplicas = znRecord.getListField(ClusterMapUtils.SEALED_STR);
    return sealedReplicas == null ? new ArrayList<>() : sealedReplicas;
  }

  /**
   * Get the list of partially sealed replicas on a given instance. This is guaranteed to return a non-null list. It
   * would return an empty list if there are no partially sealed replicas or if the field itself is absent for this
   * instance.
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the list of partially sealed replicas.
   */
  static List<String> getPartiallySealedReplicas(ZNRecord znRecord) {
    List<String> partiallySealedReplicas = znRecord.getListField(ClusterMapUtils.PARTIALLY_SEALED_STR);
    return partiallySealedReplicas == null ? new ArrayList<>() : partiallySealedReplicas;
  }

  /**
   * Get the list of stopped replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no stopped replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of stopped replicas.
   */
  static List<String> getStoppedReplicas(InstanceConfig instanceConfig) {
    return getStoppedReplicas(instanceConfig.getRecord());
  }

  /**
   * Get the list of stopped replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no stopped replicas or if the field itself is absent for this instance.
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the list of stopped replicas.
   */
  static List<String> getStoppedReplicas(ZNRecord znRecord) {
    List<String> stoppedReplicas = znRecord.getListField(ClusterMapUtils.STOPPED_REPLICAS_STR);
    return stoppedReplicas == null ? new ArrayList<>() : stoppedReplicas;
  }

  /**
   * Get the list of disabled replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no disabled replicas or if the field itself is absent for this instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of disabled replicas.
   */
  public static List<String> getDisabledReplicas(InstanceConfig instanceConfig) {
    return getDisabledReplicas(instanceConfig.getRecord());
  }

  /**
   * Get the list of disabled replicas on a given instance. This is guaranteed to return a non-null list. It would return
   * an empty list if there are no disabled replicas or if the field itself is absent for this instance.
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the list of disabled replicas.
   */
  public static List<String> getDisabledReplicas(ZNRecord znRecord) {
    List<String> disabledReplicas = znRecord.getListField(ClusterMapUtils.DISABLED_REPLICAS_STR);
    return disabledReplicas == null ? new ArrayList<>() : disabledReplicas;
  }

  /**
   * Get the rack id associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the rack id associated with the given instance.
   */
  static String getRackId(InstanceConfig instanceConfig) {
    return getRackId(instanceConfig.getRecord());
  }

  /**
   * Get the rack id associated with the given instance (if any).
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the rack id associated with the given instance.
   */
  static String getRackId(ZNRecord znRecord) {
    return znRecord.getSimpleField(RACKID_STR);
  }

  /**
   * Get the datacenter name associated with the given instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the datacenter name associated with the given instance.
   */
  public static String getDcName(InstanceConfig instanceConfig) {
    return getDcName(instanceConfig.getRecord());
  }

  /**
   * Get the datacenter name associated with the given instance.
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the datacenter name associated with the given instance.
   */
  public static String getDcName(ZNRecord znRecord) {
    return znRecord.getSimpleField(DATACENTER_STR);
  }

  /**
   * Get the ssl port associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the ssl port associated with the given instance.
   */
  public static Integer getSslPortStr(InstanceConfig instanceConfig) {
    return getSslPortStr(instanceConfig.getRecord());
  }

  /**
   * Get the ssl port associated with the given instance (if any).
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the ssl port associated with the given instance.
   */
  static Integer getSslPortStr(ZNRecord znRecord) {
    String sslPortStr = znRecord.getSimpleField(SSL_PORT_STR);
    return sslPortStr == null ? null : Integer.valueOf(sslPortStr);
  }

  /**
   * Get the http2 port associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the http2 port associated with the given instance.
   */
  public static Integer getHttp2PortStr(InstanceConfig instanceConfig) {
    return getHttp2PortStr(instanceConfig.getRecord());
  }

  /**
   * Get the http2 port associated with the given instance (if any).
   * @param znRecord the {@link ZNRecord} associated with the interested instance.
   * @return the http2 port associated with the given instance.
   */
  static Integer getHttp2PortStr(ZNRecord znRecord) {
    String http2PortStr = znRecord.getSimpleField(HTTP2_PORT_STR);
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
   * Get resource name associated with given partition.
   * @param helixAdmin the {@link HelixAdmin} to access resources in cluster
   * @param clusterName the name of cluster in which the partition resides
   * @param partitionName name of partition
   * @return resource name associated with given partition. {@code null} if not found.
   */
  static String getResourceNameOfPartition(HelixAdmin helixAdmin, String clusterName, String partitionName) {
    String result = null;
    for (String resourceName : helixAdmin.getResourcesInCluster(clusterName)) {
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
      if (idealState.getPartitionSet().contains(partitionName)) {
        result = resourceName;
        break;
      }
    }
    return result;
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
  public static byte[] serializeShortAndLong(Short shortValue, Long longValue) {
    ByteBuffer buffer = ByteBuffer.allocate(Short.SIZE / Byte.SIZE + Long.SIZE / Byte.SIZE);
    buffer.putShort(shortValue);
    buffer.putLong(longValue);
    return buffer.array();
  }

  /**
   * Convert {@link PartitionState} object to str.
   * @param partitionState {@link PartitionState} to convert.
   * @return String representation for {@link PartitionState}.
   */
  public static String partitionStateToStr(PartitionState partitionState) {
    String partitionStateStr = null;
    switch (partitionState) {
      case READ_WRITE:
        partitionStateStr = READ_WRITE_STR;
        break;
      case PARTIAL_READ_WRITE:
        partitionStateStr = PARTIAL_READ_WRITE_STR;
        break;
      case READ_ONLY:
        partitionStateStr = READ_ONLY_STR;
        break;
      default:
        throw new IllegalArgumentException(String.format("Invalid partition state %s", partitionState.name()));
    }
    return partitionStateStr;
  }

  /**
   * Convert partition state string to {@link ReplicaSealStatus} object.
   * @param partitionStateStr the partition state string to convert.
   * @return ReplicaSealStatus object.
   */
  public static ReplicaSealStatus partitionStateStrToReplicaSealStatus(String partitionStateStr) {
    if (partitionStateStr.equals(READ_WRITE_STR)) {
      return ReplicaSealStatus.NOT_SEALED;
    } else if (partitionStateStr.equals(PARTIAL_READ_WRITE_STR)) {
      return ReplicaSealStatus.PARTIALLY_SEALED;
    } else if (partitionStateStr.equals(READ_ONLY_STR)) {
      return ReplicaSealStatus.SEALED;
    }
    throw new IllegalArgumentException(String.format("Invalid partition state str %s", partitionStateStr));
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
   * @param maxDiskCapacityInBytes max allowed capacity of a disk.
   */
  static void validateDiskCapacity(long diskCapacityInBytes, long maxDiskCapacityInBytes) {
    if (diskCapacityInBytes < MIN_DISK_CAPACITY_IN_BYTES) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + diskCapacityInBytes + " is less than " + MIN_DISK_CAPACITY_IN_BYTES);
    } else if (diskCapacityInBytes > maxDiskCapacityInBytes) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + diskCapacityInBytes + " is more than " + maxDiskCapacityInBytes);
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
    private final ClusterManagerQueryHelper<?, ?, ?, ?> clusterManagerQueryHelper;
    private final String localDatacenterName;
    private final String defaultPartitionClass;
    private Collection<? extends PartitionId> allPartitions;
    private Map<String, SortedMap<Integer, List<PartitionId>>> partitionIdsByClassAndLocalReplicaCount;
    private Map<PartitionId, List<ReplicaId>> partitionIdToLocalReplicas;
    private HelixClusterManagerMetrics clusterManagerMetrics;

    /**
     * @param clusterManagerQueryHelper the {@link ClusterManagerQueryHelper} to query current cluster info
     * @param localDatacenterName the name of the local datacenter. Can be null if datacenter specific replica counts
     * @param minimumLocalReplicaCount the minimum number of replicas in local datacenter. This is used when selecting
     * @param defaultPartitionClass the default partition class to use if a partition class is not found
     */
    PartitionSelectionHelper(ClusterManagerQueryHelper<?, ?, ?, ?> clusterManagerQueryHelper, String localDatacenterName,
        int minimumLocalReplicaCount, String defaultPartitionClass, HelixClusterManagerMetrics clusterManagerMetrics) {
      this.localDatacenterName = localDatacenterName;
      this.minimumLocalReplicaCount = minimumLocalReplicaCount;
      this.clusterManagerQueryHelper = clusterManagerQueryHelper;
      this.defaultPartitionClass = defaultPartitionClass;
      this.clusterManagerMetrics = clusterManagerMetrics;

      Collection<? extends PartitionId> partitions = clusterManagerQueryHelper.getPartitions();
      Collection<PartitionId> filteredPartitions = getFilteredPartitions(partitions);
      updatePartitions(filteredPartitions, localDatacenterName);
      logger.debug("Number of partitions in data center {} {}", localDatacenterName, allPartitions.size());
      for (Map.Entry<String, SortedMap<Integer, List<PartitionId>>> entry : partitionIdsByClassAndLocalReplicaCount.entrySet()) {
        logger.debug("Partition class {}, partitions {}", entry.getKey(), entry.getValue().values());
      }
      for (Map.Entry<PartitionId, List<ReplicaId>> entry : partitionIdToLocalReplicas.entrySet()) {
        logger.debug("Partition {}, local Replicas {}", entry.getKey().toPathString(), entry.getValue());
      }
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
     * Gets the partitions after filtering based on replication factor comparison to routerputsuccesstarget
     * @param partitions
     * @return filtered partitions that can be written to
     */
    private Collection<PartitionId> getFilteredPartitions(Collection<? extends PartitionId> partitions) {
      Collection<PartitionId> filteredPartitions = new ArrayList<>();
      if (clusterManagerQueryHelper.isPartitionFilteringEnabled()) {
        for (PartitionId partition : partitions) {
          if (clusterManagerQueryHelper.isValidPartition(partition.toString())) {
            filteredPartitions.add(partition);
          }
        }
      } else {
        filteredPartitions.addAll(partitions);
      }
      return filteredPartitions;
    }

    /**
     * Returns all the partitions that are not in state {@link PartitionState#READ_ONLY} AND have the highest number of
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
      return getPartitionsWithState(partitionClass, (partitionState -> partitionState != PartitionState.READ_ONLY),
          WRITABLE_LOG_STR);
    }

    /**
     * Returns all the partitions that are in the state {@link PartitionState#READ_WRITE} AND have the highest number of
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
    List<PartitionId> getFullyWritablePartitions(String partitionClass) {
      return getPartitionsWithState(partitionClass, (partitionState -> partitionState == PartitionState.READ_WRITE),
          FULLY_WRITABLE_LOG_STR);
    }

    /**
     * Returns all the partitions that meet the specified stateCriteria AND have the highest number of
     * replicas in the local datacenter for the given {@code partitionClass}.
     * <p/>
     * Also attempts to return only partitions with healthy replicas but if no such partitions are found, returns all
     * the eligible partitions irrespective of replica health.
     * <p/>
     * If {@code partitionClass} is {@code null}, gets partitions from all partition classes.
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @param stateCriteria {@link Predicate} that specifies the selection criteria for the partition based on {@link PartitionState}.
     * @param criteriaStr String representing a name for stateCriteria for logging.
     * @return all the writable partitions in {@code partitionClass} with the highest replica count in the local
     * datacenter (all writable partitions if it is {@code null}).
     */
    List<PartitionId> getPartitionsWithState(String partitionClass, Predicate<PartitionState> stateCriteria,
        String criteriaStr) {
      List<PartitionId> suitablePartitions = new ArrayList<>();
      List<PartitionId> healthySuitablePartitions = new ArrayList<>();
      for (PartitionId partition : getPartitionsInClass(partitionClass, true)) {
        if (stateCriteria.test(partition.getPartitionState())) {
          suitablePartitions.add(partition);
          if (areAllReplicasForPartitionUp((partition))) {
            healthySuitablePartitions.add(partition);
          }
        } else {
          logger.debug("{} doesn't meet {} criteria, skipping it", partition, criteriaStr);
        }
      }
      return healthySuitablePartitions.isEmpty() ? suitablePartitions : healthySuitablePartitions;
    }

    /**
     * Returns a writable partition selected at random, that belongs to the specified partition class and that is not in
     * the state {@link PartitionState#READ_ONLY} AND has enough replicas up. In case none of the partitions have enough
     * replicas up, any writable partition is returned. Enough replicas is considered to be all local replicas if such
     * information is available. In case localDatacenterName is not available, all of the partition's replicas should be up.
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @param partitionsToExclude partitions that should be excluded from the result. Can be {@code null} or empty.
     * @return A writable partition or {@code null} if no writable partition with given criteria found
     */
    PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
      return getRandomPartitionWithState(partitionClass, partitionsToExclude,
          (partitionState -> partitionState != PartitionState.READ_ONLY), WRITABLE_LOG_STR);
    }

    /**
     * Returns a fully writable partition selected at random, that belongs to the specified partition class and that is in
     * the state {@link PartitionState#READ_WRITE} AND has enough replicas up. In case none of the partitions have enough
     * replicas up, any fully writable partition is returned. Enough replicas is considered to be all local replicas if such
     * information is available. In case localDatacenterName is not available, all of the partition's replicas should be up.
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @param partitionsToExclude partitions that should be excluded from the result. Can be {@code null} or empty.
     * @return A fully writable partition or {@code null} if no fully writable partition with given criteria found
     */
    PartitionId getRandomFullyWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
      return getRandomPartitionWithState(partitionClass, partitionsToExclude,
          (partitionState -> partitionState == PartitionState.READ_WRITE), FULLY_WRITABLE_LOG_STR);
    }

    /**
     * Returns a partition selected at random, that belongs to the specified partition class and meets the specified
     * stateCriteria AND has enough replicas up. In case none of the partitions have enough
     * replicas up, any partition that meets the state criteria is returned. Enough replicas is considered to be all
     * local replicas if such information is available. In case localDatacenterName is not available, all of the
     * partition's replicas should be up.
     * @param partitionClass the class of the partitions desired. Can be {@code null}.
     * @param partitionsToExclude partitions that should be excluded from the result. Can be {@code null} or empty.
     * @param stateCriteria {@link Predicate} that specifies the selection criteria for the partition based on {@link PartitionState}.
     * @param criteriaStr String representing a name for stateCriteria for logging.
     * @return A partition that meets the specified stateCriteria or {@code null} if no partition with given stateCriteria found
     */
    PartitionId getRandomPartitionWithState(String partitionClass, List<PartitionId> partitionsToExclude,
        Predicate<PartitionState> stateCriteria, String criteriaStr) {
      PartitionId anySuitablePartition = null;
      long startTime = SystemTime.getInstance().milliseconds();
      List<PartitionId> partitionsInClass = new ArrayList<>();
      rwLock.readLock().lock();
      try {
        partitionsInClass = getPartitionsInClass(partitionClass, true);
        int workingSize = partitionsInClass.size();
        while (workingSize > 0) {
          int randomIndex = ThreadLocalRandom.current().nextInt(workingSize);
          PartitionId selected = partitionsInClass.get(randomIndex);
          if (partitionsToExclude == null || partitionsToExclude.size() == 0 || !partitionsToExclude.contains(
              selected)) {
            if (stateCriteria.test(selected.getPartitionState())) {
              anySuitablePartition = selected;
              if (hasEnoughEligibleWritableReplicas(selected)) {
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
        return anySuitablePartition;
      } finally {
        rwLock.readLock().unlock();
        if (anySuitablePartition != null) {
          logger.debug(
              "Partition class {}, number of partitions {}, selected partition {} for {} criteria, search time in Ms {}",
              partitionClass != null ? partitionClass : "", partitionsInClass.size(), anySuitablePartition,
              SystemTime.getInstance().milliseconds() - startTime);
        } else {
          logger.debug(
              "Partition class {}, number of partitions {}, no partition selected for {} criteria, search time in Ms {}",
              partitionClass != null ? partitionClass : "", partitionsInClass.size(),
              SystemTime.getInstance().milliseconds() - startTime);
        }
      }
    }

    /**
     * Check whether the partition has enough eligible replicas for write operations to try. Here, "eligible" means
     * replica is up and in required states for PUT request (i.e LEADER or STANDBY). Enough replicas is considered to be
     * all local replicas if such information is available. In case localDatacenterName is not available, all of the
     * partition's replicas should be up.
     * @param partitionId the {@link PartitionId} to check.
     * @return true if enough replicas are eligible; false otherwise.
     */
    private boolean hasEnoughEligibleWritableReplicas(PartitionId partitionId) {
      if (localDatacenterName != null && !localDatacenterName.isEmpty()) {
        return areAllLocalReplicasForPartitionUp(partitionId) && areAllReplicaStatesEligibleForPut(partitionId,
            localDatacenterName);
      } else {
        return areAllReplicasForPartitionUp(partitionId) && areAllReplicaStatesEligibleForPut(partitionId, null);
      }
    }

    /**
     * Check if there are at least requiredEligibleReplicaCount of replicas eligible for Put operation for the specified
     * partition. If the specified checkLocalDcOnly is set to {@code true} then the check happens only for local datacenter.
     * Otherwise, the check happens for all the datacenters.
     * @param partitionId {@link PartitionId} whose replicas are checked for eligibility.
     * @param requiredEligibleReplicaCount required number of replicas which should be eligible for put.
     * @param checkLocalDcOnly if set to {@code true} then only local datacenter is checked. Otherwise, all the datacenters are checked.
     * @return {@code true} if there are enough replicas. {@code false} otherwise.
     */
    boolean hasEnoughEligibleReplicasAvailableForPut(PartitionId partitionId, int requiredEligibleReplicaCount,
        boolean checkLocalDcOnly) {
      Set<ReplicaId> eligibleReplicas = new HashSet<>();
      EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER)
          .forEach(state -> eligibleReplicas.addAll(
              partitionId.getReplicaIdsByState(state, checkLocalDcOnly ? localDatacenterName : null)));
      int eligibleReplicaCount = 0;
      for (ReplicaId replica : eligibleReplicas) {
        if (!replica.isDown()) {
          eligibleReplicaCount++;
        }
      }
      return eligibleReplicaCount >= requiredEligibleReplicaCount;
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

      // Generate a metric to track how often we choose a replica that is not globally available for writes.
      if (!isPartitionEligibleForParanoidDurability(partitionId)) {
        if (clusterManagerMetrics != null)
          clusterManagerMetrics.paranoidDurabilityIneligibleReplicaCount.inc();
      }
      return true;
    }

    /**
     * Checks whether there is at least one replica in every remote data center that is eligible for puts.
     * @param partitionId the {@link PartitionId} to check.
     * @return true if the given partition has at least one replica in every remote data center that is both up and
     * in the right Helix state.
     */
    private boolean isPartitionEligibleForParanoidDurability(PartitionId partitionId) {
      // Create a map (validRemoteReplicasPerDc) where the keys are names of remote data centers and the values are the number of replicas in that
      // data center that are both up and in the right Helix state (LEADER or STANDBY).
      Map<ReplicaState, ? extends List<? extends ReplicaId>> replicasByState = partitionId.getReplicaIdsByStates(EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER), null);
      List<? extends ReplicaId> replicas = replicasByState.values().stream().flatMap(List::stream).collect(Collectors.toList());

      Map<String, Integer> validRemoteReplicasPerDc = replicas.stream()
          .filter(replica -> !replica.getDataNodeId().getDatacenterName().equals(this.localDatacenterName))
          .filter(replica -> !replica.isDown())
          .collect(Collectors.groupingBy(replica -> replica.getDataNodeId().getDatacenterName(),
              Collectors.reducing(0, replica -> 1, Integer::sum)));

      return !validRemoteReplicasPerDc.isEmpty() && validRemoteReplicasPerDc.values().stream().allMatch(count -> count > 0);
    }

    /**
     * Check if all replicas from given partition are in eligible states for PUT request. (That is, replica state should
     * be either LEADER or STANDBY.)
     * @param partitionId the {@link PartitionId} to check.
     * @param dcName the datacenter which replicas come from. If null, replicas from all datacenters should be checked.
     * @return true if all replicas are eligible for put.
     */
    private boolean areAllReplicaStatesEligibleForPut(PartitionId partitionId, String dcName) {
      Set<ReplicaId> eligibleReplicas = new HashSet<>();
      EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER)
          .forEach(state -> eligibleReplicas.addAll(partitionId.getReplicaIdsByState(state, dcName)));
      List<? extends ReplicaId> replicas;
      if (dcName != null) {
        replicas = partitionIdToLocalReplicas.get(partitionId);
      } else {
        replicas = partitionId.getReplicaIds();
      }
      logger.debug("Partition for Put {}, all replicas {}, eligible replicas {}", partitionId.toPathString(), replicas,
          eligibleReplicas);
      return replicas.size() == eligibleReplicas.size();
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
        } else {
          SortedMap<Integer, List<PartitionId>> partitionsByReplicaCount =
              partitionIdsByClassAndLocalReplicaCount.get(partitionClass);
          if (partitionsByReplicaCount == null) {
            partitionsByReplicaCount = partitionIdsByClassAndLocalReplicaCount.get(defaultPartitionClass);
          }
          if (partitionsByReplicaCount == null) {
            throw new IllegalArgumentException(
                "No partitions for partition class = '" + partitionClass + "' or default partition class = '" + defaultPartitionClass + "' found");
          }
          if (minimumReplicaCountRequired) {
            // get partitions with replica count >= min replica count specified in ClusterMapConfig
            for (List<PartitionId> partitionIds : partitionsByReplicaCount.tailMap(minimumLocalReplicaCount).values()) {
              toReturn.addAll(partitionIds);
            }
          } else {
            for (List<PartitionId> partitionIds : partitionsByReplicaCount.values()) {
              toReturn.addAll(partitionIds);
            }
          }
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
      Collection<? extends PartitionId> partitions = clusterManagerQueryHelper.getPartitions();
      Collection<PartitionId> partitionsInCluster = getFilteredPartitions(partitions);

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
