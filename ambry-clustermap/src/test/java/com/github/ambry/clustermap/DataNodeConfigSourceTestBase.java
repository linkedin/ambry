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

import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Base class containing utilities for tests of {@link DataNodeConfigSource} implementations.
 */
public class DataNodeConfigSourceTestBase {
  static final String DC_NAME = "DC";
  private int counter = 0;

  /**
   * Check that {@link DataNodeConfigChangeListener#onDataNodeConfigChange} was called with the specified configs
   * within a short period of time.
   * @param listener the {@link DataNodeConfigChangeListener} to check.
   * @param expectedConfigs the set of configs expected.
   */
  void checkListenerCall(DataNodeConfigChangeListener listener, Set<DataNodeConfig> expectedConfigs) {
    verify(listener, timeout(500)).onDataNodeConfigChange(argThat(configs -> {
      Set<DataNodeConfig> configsReceived = new HashSet<>();
      configs.forEach(configsReceived::add);
      return configsReceived.equals(expectedConfigs);
    }));
  }

  /**
   * @param numDisks the number of disks to include in the config
   * @param numReplicasPerDisk the number of replicas to include on each disk in the config.
   * @return a generated {@link DataNodeConfig} to use in tests.
   */
  DataNodeConfig createConfig(int numDisks, int numReplicasPerDisk) {
    String hostname = "host" + counter++;
    int port = 2222;
    String instanceName = ClusterMapUtils.getInstanceName(hostname, port);
    DataNodeConfig dataNodeConfig =
        new DataNodeConfig(instanceName, hostname, port, DC_NAME, port + 1, port + 2, "rack",
            ClusterMapUtils.DEFAULT_XID);
    int nextPartitionId = 0;
    for (int i = 0; i < numDisks; i++) {
      DataNodeConfig.DiskConfig diskConfig = new DataNodeConfig.DiskConfig(HardwareState.AVAILABLE, 2000);
      for (int j = 0; j < numReplicasPerDisk; j++) {
        int partitionId = nextPartitionId++;
        String partitionName = Integer.toString(partitionId);
        switch (partitionId % 4) {
          case 0:
            break;
          case 1:
            dataNodeConfig.getSealedReplicas().add(partitionName);
            break;
          case 2:
            dataNodeConfig.getStoppedReplicas().add(partitionName);
            break;
          case 3:
            dataNodeConfig.getDisabledReplicas().add(partitionName);
            break;
        }
        diskConfig.getReplicaConfigs().put(partitionName, new DataNodeConfig.ReplicaConfig(10, "class"));
      }
      dataNodeConfig.getDiskConfigs().put("/" + i, diskConfig);
    }
    return dataNodeConfig;
  }
}
