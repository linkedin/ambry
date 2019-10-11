/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;
import java.io.IOException;
import java.util.Collection;
import java.util.List;


public interface ReplicationAPI {

  void start() throws ReplicationException;

  boolean controlReplicationForPartitions(Collection<PartitionId> ids, List<String> origins, boolean enable);

  void updateTotalBytesReadByRemoteReplica(PartitionId partitionId, String hostName, String replicaPath,
      long totalBytesRead);

  long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath);

  void shutdown() throws ReplicationException;
}
