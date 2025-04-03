package com.github.ambry.replica.prioritization;


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
  long getStartTime();
}
