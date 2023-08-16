/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.cloud;

import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaTokenPersistor;
import com.github.ambry.replication.ReplicationMetrics;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements methods to persist {@link com.github.ambry.replication.RemoteReplicaInfo.ReplicaTokenInfo} to Azure
 */
public class AzureStoreTokenWriter extends ReplicaTokenPersistor {
  private static final Logger logger = LoggerFactory.getLogger(AzureStoreTokenWriter.class);

  /**
   * Constructor
   * @param partitionGroupedByMountPath
   * @param properties
   * @param replicationMetrics
   * @param clusterMap
   * @param findTokenHelper
   */
  public AzureStoreTokenWriter(Map<String, Set<PartitionInfo>> partitionGroupedByMountPath,
      VerifiableProperties properties, ReplicationMetrics replicationMetrics, ClusterMap clusterMap,
      FindTokenHelper findTokenHelper) {
    super(partitionGroupedByMountPath, replicationMetrics, clusterMap, findTokenHelper);
    CloudConfig cloudConfig = new CloudConfig(properties);
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(properties);
  }

  @Override
  public void persist(String mountPath, List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfoList) throws IOException {
    // TODO
  }

  @Override
  public List<RemoteReplicaInfo.ReplicaTokenInfo> retrieve(String mountPath) {
    // TODO
    return Collections.emptyList();
  }
}
