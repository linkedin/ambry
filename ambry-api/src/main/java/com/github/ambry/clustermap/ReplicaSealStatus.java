/*
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
package com.github.ambry.clustermap;

/**
 * Valid sealed status of a {@link ReplicaId}.
 * Sealed status of a replica is determined by the percentage of replica's size compared to its capacity.
 * See {@link com.github.ambry.config.StoreConfig} for the thresholds.
 *
 * The state transition of a replica's seal status due to size will always follow the following order:
 * NOT_SEALED <-> PARTIALLY_SEALED <-> SEALED
 */
public enum ReplicaSealStatus {
  NOT_SEALED,
  PARTIALLY_SEALED,
  SEALED;

  /**
   * Merge the specified {@link ReplicaSealStatus}es into a single {@link ReplicaSealStatus} by choosing the most restrictive
   * seal status for the replica in the order
   * {@link ReplicaSealStatus#SEALED} > {@link ReplicaSealStatus#PARTIALLY_SEALED} > {@link ReplicaSealStatus#NOT_SEALED}.
   * @param sealStatus1 {@link ReplicaSealStatus} object to be merged.
   * @param sealStatus2 {@link ReplicaSealStatus} object to be merged.
   * @return The merged {@link ReplicaSealStatus} object.
   */
  public static ReplicaSealStatus mergeReplicaSealStatus(ReplicaSealStatus sealStatus1, ReplicaSealStatus sealStatus2) {
    if (sealStatus1 == ReplicaSealStatus.SEALED || sealStatus2 == ReplicaSealStatus.SEALED) {
      return ReplicaSealStatus.SEALED;
    } else if (sealStatus1 == ReplicaSealStatus.PARTIALLY_SEALED || sealStatus2 == ReplicaSealStatus.PARTIALLY_SEALED) {
      return ReplicaSealStatus.PARTIALLY_SEALED;
    } else {
      return ReplicaSealStatus.NOT_SEALED;
    }
  }
}
