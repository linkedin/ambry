package com.github.ambry.clustermap;

/**
 * The response codes conveyed to the cluster map when there is a replica related event.
 */
public enum ReplicaEventType {
  Node_Response,
  Node_Timeout,
  Disk_Error,
  Disk_Ok,
  Partition_ReadOnly
}
