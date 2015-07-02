package com.github.ambry.admin;

/**
 * Enumerates the different custom operations that the admin supports.
 */
enum AdminOperationType {
  echo,
  getReplicasForBlobId,
  unknown;

  /**
   * Converts the operation specified by the input string into an {@link AdminOperationType}.
   * @param operationTypeStr - the operation requested as a string.
   * @return - the operation requested as a valid {@link AdminOperationType} if operation is known, otherwise returns
   * {@link AdminOperationType#unknown}.
   */
  public static AdminOperationType getAdminOperationType(String operationTypeStr) {
    try {
      return AdminOperationType.valueOf(operationTypeStr);
    } catch (IllegalArgumentException e) {
      return AdminOperationType.unknown;
    }
  }
}
