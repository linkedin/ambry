package com.github.ambry.replica.prioritization;

public enum PartitionPriorityTier {
  // Priority tier definitions
    CRITICAL(90, 100),    // Critical priority - imminent disruption
    HIGH(70, 89),         // High priority - upcoming disruption
    MEDIUM(40, 69),       // Medium priority
    LOW(0, 39);           // Low priority

    private final int minPriority;
    private final int maxPriority;

  PartitionPriorityTier(int minPriority, int maxPriority) {
      this.minPriority = minPriority;
      this.maxPriority = maxPriority;
    }

    public static PartitionPriorityTier fromPriority(int priority) {
      for (PartitionPriorityTier tier : values()) {
        if (priority >= tier.minPriority && priority <= tier.maxPriority) {
          return tier;
        }
      }
      return LOW; // Default
    }
}
