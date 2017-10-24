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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  static final String DISK_CAPACITY_STR = "capacityInBytes";
  static final String DISK_STATE = "diskState";
  static final String REPLICAS_STR = "Replicas";
  static final String REPLICAS_DELIM_STR = ",";
  static final String REPLICAS_STR_SEPARATOR = ":";
  static final String SSLPORT_STR = "sslPort";
  static final String RACKID_STR = "rackId";
  static final String SEALED_STR = "SEALED";
  static final String AVAILABLE_STR = "AVAILABLE";
  static final String UNAVAILABLE_STR = "UNAVAILABLE";
  static final String ZKCONNECTSTR_STR = "zkConnectStr";
  static final String ZKINFO_STR = "zkInfo";
  static final String DATACENTER_STR = "datacenter";
  static final String DATACENTER_ID_STR = "id";
  static final int UNKNOWN_RACK_ID = -1;
  static final int MIN_PORT = 1025;
  static final int MAX_PORT = 65535;
  static final long MIN_REPLICA_CAPACITY_IN_BYTES = 1024 * 1024 * 1024L;
  static final long MAX_REPLICA_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024 * 1024;
  static final long MIN_DISK_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024;
  static final long MAX_DISK_CAPACITY_IN_BYTES = 10L * 1024 * 1024 * 1024 * 1024;

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
      byte id = Byte.parseByte(entry.getString(DATACENTER_ID_STR));
      DcZkInfo dcZkInfo = new DcZkInfo(entry.getString(DATACENTER_STR), id, entry.getString(ZKCONNECTSTR_STR));
      dataCenterToZkAddress.put(dcZkInfo.dcName, dcZkInfo);
    }
    return dataCenterToZkAddress;
  }

  /**
   * Get the list of sealed replicas on a given instance.
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the list of sealed replicas.
   */
  static List<String> getSealedReplicas(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getListField(ClusterMapUtils.SEALED_STR);
  }

  /**
   * Get the rack id associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the rack id associated with the given instance.
   */
  static Long getRackId(InstanceConfig instanceConfig) {
    return Long.valueOf(instanceConfig.getRecord().getSimpleField(RACKID_STR));
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
}

