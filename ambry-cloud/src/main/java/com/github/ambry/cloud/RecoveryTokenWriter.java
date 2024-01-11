/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaTokenPersistor;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.replication.ReplicationMetrics;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RecoveryTokenWriter extends ReplicaTokenPersistor  {

  public RecoveryTokenWriter(Map<String, Set<PartitionInfo>> partitionGroupedByMountPath,
      ReplicationMetrics replicationMetrics, ClusterMap clusterMap, FindTokenHelper findTokenHelper) {
    super(partitionGroupedByMountPath, replicationMetrics, clusterMap, findTokenHelper);
  }

  @Override
  protected void persist(String mountPath, List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfoList) throws IOException {

  }

  @Override
  public List<RemoteReplicaInfo.ReplicaTokenInfo> retrieve(String mountPath) throws ReplicationException {
    return null;
  }
}
