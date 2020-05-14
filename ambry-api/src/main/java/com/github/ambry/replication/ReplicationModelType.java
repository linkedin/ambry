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

package com.github.ambry.replication;

/**
 * The model used for cross-colo replication
 * Under ALL_TO_ALL mode, all replicas of local data center replicate from all peer replicas in remote data centers
 * Under LEADER_BASED mode, only the leader replica of local data center (as elected by helix) will replicate with other leader replicas in remote data centers
 */
public enum ReplicationModelType {
  ALL_TO_ALL, LEADER_BASED
}
