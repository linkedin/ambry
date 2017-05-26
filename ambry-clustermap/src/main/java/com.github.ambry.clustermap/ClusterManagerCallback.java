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

import java.util.List;


/**
 * A callback that needs to be implemented by dynamic implementations of the cluster manager which can be
 * used by dynamic cluster manager components such as {@link AmbryDataNode}, {@link AmbryDisk},
 * {@link AmbryPartition}, and {@link AmbryReplica}
 */
interface ClusterManagerCallback {
  /**
   * Get all replica ids associated with the given {@link AmbryPartition}
   * @param partition the {@link AmbryPartition} for which to get the list of replicas.
   * @return the list of {@link AmbryReplica}s associated with the given partition.
   */
  List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition);

  /**
   * Get the counter for the sealed state change for partitions.
   * @return the counter for the sealed state change for partitions.
   */
  long getSealedStateChangeCounter();
}
