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
  static final String DISK_CAPACITY_STR = "capacityInBytes";
  static final String DISK_STATE = "diskState";
  static final String REPLICAS_STR = "Replicas";
  static final String REPLICAS_DELIM_STR = ",";
  static final String REPLICAS_STR_SEPARATOR = ":";
  static final String SSLPORT_STR = "sslPort";
  static final String RACKID_STR = "rackId";
  static final String SEALED_STR = "SEALED";
  static final String ONLINE_STR = "ONLINE";
  static final String OFFLINE_STR = "OFFLINE";
  static final String ZKCONNECTSTR_STR = "zkConnectStr";
  static final String ZKINFO_STR = "zkInfo";
  static final String DATACENTER_STR = "datacenter";
  static final int MissingRackId = -1;
  static final int MinPort = 1025;
  static final int MaxPort = 65535;
  static final long Min_Replica_Capacity_In_Bytes = 1024 * 1024 * 1024L;
  static final long Max_Replica_Capacity_In_Bytes = 10995116277760L; // 10 TB

  /**
   * Construct and return the instance name given the host and port.
   * @param host the hostname of the instance.
   * @param port the port of the instance.
   * @return the constructed instance name.
   */
  public static String getInstanceName(String host, int port) {
    return port == -1 ? host : host + "_" + port;
  }

  /**
   * Parses zk layout JSON string and populates and returns a map of datacenter name to Zk connect strings.
   * @param zkLayoutJsonString the string containing the Zookeeper layout.
   * @throws JSONException if there is an error parsing the JSON.
   */
  static Map<String, String> parseZkJsonAndPopulateZkInfo(String zkLayoutJsonString) throws JSONException {
    Map<String, String> dataCenterToZkAddress = new HashMap<>();
    JSONObject root = new JSONObject(zkLayoutJsonString);
    JSONArray all = (root).getJSONArray(ZKINFO_STR);
    for (int i = 0; i < all.length(); i++) {
      JSONObject entry = all.getJSONObject(i);
      dataCenterToZkAddress.put(entry.getString(DATACENTER_STR), entry.getString(ZKCONNECTSTR_STR));
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
  static String getRackIdStr(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getSimpleField(RACKID_STR);
  }

  /**
   * Get the ssl port string associated with the given instance (if any).
   * @param instanceConfig the {@link InstanceConfig} associated with the interested instance.
   * @return the ssl port associated with the given instance.
   */
  static String getSslPortStr(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getSimpleField(SSLPORT_STR);
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
   * Validate the replica capacity.
   * @param replicaCapacityInBytes the replica capacity to validate.
   */
  static void validateReplicaCapacityInBytes(long replicaCapacityInBytes) {
    if (replicaCapacityInBytes < Min_Replica_Capacity_In_Bytes) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + replicaCapacityInBytes + " is less than " + Min_Replica_Capacity_In_Bytes);
    } else if (replicaCapacityInBytes > Max_Replica_Capacity_In_Bytes) {
      throw new IllegalStateException(
          "Invalid disk capacity: " + replicaCapacityInBytes + " is more than " + Max_Replica_Capacity_In_Bytes);
    }
  }
}

