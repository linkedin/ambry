/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.replica.prioritization;

/**
 * Enum representing the priority tiers for replica prioritization.
 * The order of the enum values indicates their priority level, with the first value being the highest priority.
 */
public enum PriorityTier {
  BELOW_MIN_REPLICA_WITH_DISRUPTION,   // Highest priority
  BELOW_MIN_REPLICA_NO_DISRUPTION,     // Second priority
  MIN_REPLICA_WITH_DISRUPTION,   // Third priority
  MIN_REPLICA_NO_DISRUPTION,     // Fourth priority
  NORMAL                        // Lowest priority (filler)
}
