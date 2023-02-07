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
  SEALED
}
