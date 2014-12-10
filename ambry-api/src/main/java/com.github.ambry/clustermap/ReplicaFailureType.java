package com.github.ambry.clustermap;

/**
 * The error codes conveyed to the cluster map when there is a replica related error.
 */
public enum ReplicaFailureType {
  Node_Timeout,
  Disk_Error,
  Partition_ReadOnly
}
