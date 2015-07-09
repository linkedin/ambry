package com.github.ambry.admin;

/**
 * Enumerates the different custom operations that the admin supports.
 */
enum AdminOperationType {
  echo,
  getReplicasForBlobId,
  unknown;
}
