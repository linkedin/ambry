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
package com.github.ambry.replication.continuous;

/**
 * These are states we are tracking for a continuous replication cycle
 *
 * UNKNOWN - We do not know the state
 * OFFLINE - Replica is determined to be offline
 * STANDBY - Replica is waiting for its data to come from intra colo replication, we should not pull data, valid in remote colo
 * STANDBY_TIMED_OUT - Replica was in STANDBY, but data has not arrived for some time, so we need to pull data,
 *                     ,valid in remote colo,
 * ACTIVE - We can pull data for this replica , valid for remote colo leader-leader pair and intra-colo replication.
 */
public enum ReplicaState {
  UNKNOWN, OFFLINE, STANDBY, STANDBY_TIMED_OUT, ACTIVE
}
