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
import java.util.Objects;
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
  private final ClusterMapConfig clusterMapConfig;

  /**
   * @param helixManager the {@link HelixManager} to use as the source of truth for {@link InstanceConfig}s.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   */
  public InstanceConfigToDataNodeConfigAdapter(HelixManager helixManager, ClusterMapConfig clusterMapConfig) {
    this.helixManager = helixManager;
    this.clusterMapConfig = clusterMapConfig;
  }

  @Override
  public void addServerConfigChangeListener(DataNodeConfigChangeListener listener) throws Exception {
    helixManager.addInstanceConfigChangeListener((InstanceConfigChangeListener) (instanceConfigs, context) -> {
      Iterable<DataNodeConfig> dataNodeConfigs =
          () -> instanceConfigs.stream().map(this::convert).filter(Objects::nonNull).iterator();
      listener.onDataNodeConfigChange(dataNodeConfigs);
    });
  }

  /**
   * @param instanceConfig the {@link InstanceConfig} to convert to a {@link DataNodeConfig} object.
   * @return the {@link DataNodeConfig}, or {@code null} if the {@link InstanceConfig} provided has an unsupported schema
   *         version.
   */
  private DataNodeConfig convert(InstanceConfig instanceConfig) {
    return convert(instanceConfig, clusterMapConfig);
  }

  /**
   * Exposed for testing.
   * @param instanceConfig the {@link InstanceConfig} to convert to a {@link DataNodeConfig} object.
   * @param clusterMapConfig the {@link ClusterMapConfig} containing any default values that may be needed.
   * @return the {@link DataNodeConfig}, or {@code null} if the {@link InstanceConfig} provided has an unsupported schema
   *         version.
   */
  static DataNodeConfig convert(InstanceConfig instanceConfig, ClusterMapConfig clusterMapConfig) {
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
    // TODO uncomment this line once 1534 is merged
    // dataNodeConfig.getDisabledReplicas().addAll(getDisabledReplicas(instanceConfig));
    instanceConfig.getRecord().getMapFields().forEach((mountPath, diskProps) -> {
      if (diskProps.get(DISK_STATE) == null) {
        // Check if this map field actually holds disk properties, since we can't tell from just the field key (the
        // mount path with no special prefix). There may be extra fields when Helix controller adds partitions in ERROR
        // state to InstanceConfig.
        LOGGER.warn("{} field does not contain disk info on {}. Skip it and continue on next one.", mountPath,
            instanceConfig.getInstanceName());
      } else {
        DataNodeConfig.DiskConfig disk = new DataNodeConfig.DiskConfig(
            diskProps.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE,
            Long.parseLong(diskProps.get(DISK_CAPACITY_STR)));
        String replicasStr = diskProps.get(REPLICAS_STR);
        if (!replicasStr.isEmpty()) {
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
}
