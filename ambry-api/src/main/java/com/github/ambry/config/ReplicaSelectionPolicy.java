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
package com.github.ambry.config;

/**
 * Specifies how the backup-node must select server replicas to back up.
 * FIXED: The backup-node will select the same server replica each time for a partition
 * ROUND_ROBIN: The backup-node will cycle through all replicas of a partition
 */
public enum ReplicaSelectionPolicy {
  FIXED,
  ROUND_ROBIN
}
