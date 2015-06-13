package com.github.ambry.admin;

/**
 * Enumerates the different custom operations that the admin supports
 */
public enum AdminOperationType {
  Echo,
  GetReplicasForBlobId,
  Unknown;

  /**
   * Converts an operation from its string representation to enum representation.
   * @param operationType
   * @return
   */
  public static AdminOperationType convert(String operationType) {
    if (operationType != null) {
      for (AdminOperationType adminOperationType : AdminOperationType.values()) {
        if (operationType.equalsIgnoreCase(adminOperationType.toString())) {
          return adminOperationType;
        }
      }
    }
    return Unknown;
  }
}
