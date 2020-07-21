/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Singleton;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An implementation of {@link DataNodeConfigSource} that converts between {@link InstanceConfig}s received by an
 * {@link InstanceConfigChangeListener} and {@link DataNodeConfig}s.
 */
public class InstanceConfigToDataNodeConfigAdapter implements DataNodeConfigSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceConfigToDataNodeConfigAdapter.class);
  private final HelixManager helixManager;
  private final Converter converter;
  private final String clusterName;
  private final Singleton<HelixAdmin> helixAdmin;

  /**
   * @param helixManager the {@link HelixManager} to use as the source of truth for {@link InstanceConfig}s.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   */
  public InstanceConfigToDataNodeConfigAdapter(HelixManager helixManager, ClusterMapConfig clusterMapConfig) {
    this.helixManager = helixManager;
    this.converter = new Converter(clusterMapConfig);
    clusterName = clusterMapConfig.clusterMapClusterName;
    helixAdmin = new Singleton<>(helixManager::getClusterManagmentTool);
  }

  @Override
  public void addDataNodeConfigChangeListener(DataNodeConfigChangeListener listener) throws Exception {
    helixManager.addInstanceConfigChangeListener((InstanceConfigChangeListener) (instanceConfigs, context) -> {
      Iterable<DataNodeConfig> dataNodeConfigs =
          () -> instanceConfigs.stream().map(converter::convert).filter(Objects::nonNull).iterator();
      listener.onDataNodeConfigChange(dataNodeConfigs);
    });
  }

  @Override
  public boolean set(DataNodeConfig config) {
    InstanceConfig instanceConfig = converter.convert(config);
    return helixAdmin.get().setInstanceConfig(clusterName, instanceConfig.getInstanceName(), instanceConfig);
  }

  @Override
  public DataNodeConfig get(String instanceName) {
    InstanceConfig instanceConfig = helixAdmin.get().getInstanceConfig(clusterName, instanceName);
    return instanceConfig != null ? converter.convert(instanceConfig) : null;
  }

  static class Converter {
    private final ClusterMapConfig clusterMapConfig;

    Converter(ClusterMapConfig clusterMapConfig) {
      this.clusterMapConfig = clusterMapConfig;
    }

    /**
     * @param instanceConfig the {@link InstanceConfig} to convert to a {@link DataNodeConfig} object.
     * @return the {@link DataNodeConfig}, or {@code null} if the {@link InstanceConfig} provided has an unsupported
     *         schema version.
     */
    DataNodeConfig convert(InstanceConfig instanceConfig) {
      int schemaVersion = getSchemaVersion(instanceConfig);
      if (schemaVersion != 0) {
        LOGGER.warn("Unknown InstanceConfig schema version {} in {}. Ignoring.", schemaVersion, instanceConfig);
        return null;
      }
      DataNodeConfig dataNodeConfig = new DataNodeConfig(instanceConfig.getInstanceName(), instanceConfig.getHostName(),
          Integer.parseInt(instanceConfig.getPort()), getDcName(instanceConfig), getSslPortStr(instanceConfig),
          getHttp2PortStr(instanceConfig), getRackId(instanceConfig), getXid(instanceConfig));
      dataNodeConfig.getSealedReplicas().addAll(getSealedReplicas(instanceConfig));
      dataNodeConfig.getStoppedReplicas().addAll(getStoppedReplicas(instanceConfig));
      dataNodeConfig.getDisabledReplicas().addAll(getDisabledReplicas(instanceConfig));
      instanceConfig.getRecord().getMapFields().forEach((mountPath, diskProps) -> {
        if (diskProps.get(DISK_STATE) == null) {
          // Check if this map field actually holds disk properties, since we can't tell from just the field key (the
          // mount path with no special prefix). There may be extra fields when Helix controller adds partitions in ERROR
          // state to InstanceConfig.
          LOGGER.info("{} field does not contain disk info on {}. Storing it in extraMapFields.", mountPath,
              instanceConfig.getInstanceName());
          dataNodeConfig.getExtraMapFields().put(mountPath, diskProps);
        } else {
          DataNodeConfig.DiskConfig disk = new DataNodeConfig.DiskConfig(
              diskProps.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE,
              Long.parseLong(diskProps.get(DISK_CAPACITY_STR)));
          String replicasStr = diskProps.get(REPLICAS_STR);
          if (!Utils.isNullOrEmpty(replicasStr)) {
            for (String replicaStr : replicasStr.split(REPLICAS_DELIM_STR)) {
              String[] replicaStrParts = replicaStr.split(REPLICAS_STR_SEPARATOR);
              // partition name and replica name are the same.
              String partitionName = replicaStrParts[0];
              long replicaCapacity = Long.parseLong(replicaStrParts[1]);
              String partitionClass =
                  replicaStrParts.length > 2 ? replicaStrParts[2] : clusterMapConfig.clusterMapDefaultPartitionClass;
              disk.getReplicaConfigs()
                  .put(partitionName, new DataNodeConfig.ReplicaConfig(replicaCapacity, partitionClass));
            }
          }
          dataNodeConfig.getDiskConfigs().put(mountPath, disk);
        }
      });
      return dataNodeConfig;
    }

    /**
     * @param dataNodeConfig the {@link DataNodeConfig} to convert into an {@link InstanceConfig}.
     * @return the {@link InstanceConfig}.
     */
    InstanceConfig convert(DataNodeConfig dataNodeConfig) {
      InstanceConfig instanceConfig = new InstanceConfig(dataNodeConfig.getInstanceName());
      instanceConfig.setHostName(dataNodeConfig.getHostName());
      instanceConfig.setPort(Integer.toString(dataNodeConfig.getPort()));
      if (dataNodeConfig.getSslPort() != null) {
        instanceConfig.getRecord().setIntField(SSL_PORT_STR, dataNodeConfig.getSslPort());
      }
      if (dataNodeConfig.getHttp2Port() != null) {
        instanceConfig.getRecord().setIntField(HTTP2_PORT_STR, dataNodeConfig.getHttp2Port());
      }
      instanceConfig.getRecord().setSimpleField(DATACENTER_STR, dataNodeConfig.getDatacenterName());
      instanceConfig.getRecord().setSimpleField(RACKID_STR, dataNodeConfig.getRackId());
      long xid = dataNodeConfig.getXid();
      if (xid != DEFAULT_XID) {
        // Set the XID only if it is not the default, in order to avoid unnecessary updates.
        instanceConfig.getRecord().setLongField(XID_STR, xid);
      }
      instanceConfig.getRecord().setIntField(SCHEMA_VERSION_STR, CURRENT_SCHEMA_VERSION);
      instanceConfig.getRecord().setListField(SEALED_STR, new ArrayList<>(dataNodeConfig.getSealedReplicas()));
      instanceConfig.getRecord()
          .setListField(STOPPED_REPLICAS_STR, new ArrayList<>(dataNodeConfig.getStoppedReplicas()));
      instanceConfig.getRecord()
          .setListField(DISABLED_REPLICAS_STR, new ArrayList<>(dataNodeConfig.getDisabledReplicas()));
      dataNodeConfig.getDiskConfigs().forEach((mountPath, diskConfig) -> {
        Map<String, String> diskProps = new HashMap<>();
        diskProps.put(DISK_STATE, diskConfig.getState() == HardwareState.AVAILABLE ? AVAILABLE_STR : UNAVAILABLE_STR);
        diskProps.put(DISK_CAPACITY_STR, Long.toString(diskConfig.getDiskCapacityInBytes()));
        StringBuilder replicasStrBuilder = new StringBuilder();
        diskConfig.getReplicaConfigs()
            .forEach((partitionName, replicaConfig) -> replicasStrBuilder.append(partitionName)
                .append(REPLICAS_STR_SEPARATOR)
                .append(replicaConfig.getReplicaCapacityInBytes())
                .append(REPLICAS_STR_SEPARATOR)
                .append(replicaConfig.getPartitionClass())
                .append(REPLICAS_DELIM_STR));
        diskProps.put(REPLICAS_STR, replicasStrBuilder.toString());
        instanceConfig.getRecord().setMapField(mountPath, diskProps);
      });
      dataNodeConfig.getExtraMapFields().forEach((k, v) -> instanceConfig.getRecord().setMapField(k, v));
      return instanceConfig;
    }
  }
}
