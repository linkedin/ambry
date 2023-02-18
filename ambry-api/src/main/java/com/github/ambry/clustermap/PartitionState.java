/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

/**
 * The valid states for a {@link PartitionId}. The state of a partition is resolved via the {@link ReplicaSealStatus}es
 * of the partition's replicas. See ClusterMapUtils
 *
 * The state transition of a partition's state will always follow the following order:
 * READ_WRITE <-> PARTIAL_READ_WRITE <-> READ_ONLY
 */
public enum PartitionState {
  /** The partition is available for all reads and writes */
  READ_WRITE,
  /** The partition is available for all reads but limited writes */
  PARTIAL_READ_WRITE,
  /** The partition is available for reads only. */
  READ_ONLY
}
