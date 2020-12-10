/**
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
 */
package com.github.ambry.cloud;

import com.github.ambry.clustermap.PartitionId;
import java.util.Collection;
import java.util.List;


/**
 * Interface that provides methods for compacting deprecated container from assigned partitions.
 */
public interface CloudContainerCompactor {
  /**
   * Compact deprecated containers from partitions assigned to this vcr node.
   * @param assignedPartitions {@link Collection} of {@link PartitionId}s assigned to this node.
   */
  void compactAssignedDeprecatedContainers(Collection<? extends PartitionId> assignedPartitions);

  /**
   * Shutdown the compactor.
   */
  void shutdown();
}
