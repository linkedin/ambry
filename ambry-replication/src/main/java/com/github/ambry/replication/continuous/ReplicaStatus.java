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
 * These are statuses we are tracking for a continuous replication cycle
 *
 * UNKNOWN - We do not know the status
 * OFFLINE - Replica is determined to be offline
 * STANDBY - Replica is waiting for its data to come from intra colo replication, does not make any replication calls until data arrives from intra colo thread,
 *                       or it has timed out and moved to STANDBY_TIMED_OUT_ON_NO_PROGRESS status, valid in remote colo
 * STANDBY_TIMED_OUT_ON_NO_PROGRESS - Replica was in STANDBY, but data has not arrived for some time, so we need to pull data,
 *                     valid in remote colo,
 * ACTIVE - We can pull data for this replica , valid for remote colo leader-leader pair and intra-colo replication.
 */
public enum ReplicaStatus {
  UNKNOWN, OFFLINE, STANDBY, STANDBY_TIMED_OUT_ON_NO_PROGRESS, ACTIVE
}
