package com.github.ambry.replica.prioritization;

public enum PriorityTier {
  BELOW_MIN_REPLICA_WITH_DISRUPTION,   // Highest priority
  BELOW_MIN_REPLICA_NO_DISRUPTION,     // Second priority
  MIN_REPLICA_WITH_DISRUPTION,   // Third priority
  MIN_REPLICA_NO_DISRUPTION,     // Fourth priority
  NORMAL                        // Lowest priority (filler)
}
