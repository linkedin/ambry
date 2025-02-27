/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import java.util.List;


public class FileCopyUtils {

  /**
   * Get the peer replica in the given datacenter for file copy. We should only copy from LEADER replicas.
   * @param partitionId the {@link PartitionId} of the replica.
   * @param datacenterName the name of the datacenter.
   * @return the peer replica in the given datacenter for file copy.
   */
  static public ReplicaId getPeerForFileCopy(PartitionId partitionId, String datacenterName) {
    List<? extends ReplicaId> replicaIds = partitionId.getReplicaIdsByState(ReplicaState.LEADER, datacenterName);
    if (replicaIds.isEmpty()) {
      return null;
    }
    return replicaIds.get(0);
  }
}
