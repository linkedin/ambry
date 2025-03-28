package com.github.ambry.replica.prioritization;

import com.google.protobuf.Timestamp;

/**
 * The Operation interface defines the contract for operations in the system.
 * Implementing classes should provide the start time of the operation.
 */
public interface Operation extends Comparable<Operation> {
  /**
   * Gets the start time of the operation.
   *
   * @return the start time of the operation in milliseconds.
   */
  Timestamp getStartTime();
}
