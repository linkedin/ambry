package com.github.ambry.admin;

/**
 * TODO: write description
 */
public enum AdminOperationType {
  Echo,
  GetReplicasForBlobId,
  Unknown;

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
