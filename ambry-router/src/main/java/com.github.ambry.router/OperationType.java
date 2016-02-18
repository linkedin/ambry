package com.github.ambry.router;

/**
 * Types for operations. Different types of operations may have different default parameter
 * values specified in {@code AmbryOperationPolicy}.
 */
public enum OperationType {
  PUT,
  GET,
  DELETE
}
