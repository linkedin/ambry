/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

/**
 * The response codes conveyed to the cluster map when there is a replica related event.
 */
public enum ReplicaEventType {
  /**
   * Node is up and responds in time.
   */
  Node_Response,

  /**
   * Node fails to respond before timeout and connection is lost.
   */
  Node_Timeout,

  /**
   * Disk is in bad state due to failures/errors.
   */
  Disk_Error,

  /**
   * Disk responds in time and is proper functioning.
   */
  Disk_Ok,

  /**
   * The partition which replica belongs to is in ReadOnly state.
   */
  Partition_ReadOnly,

  /**
   * The replica is unavailable because it is either stopped or unreachable.
   */
  Replica_Unavailable,

  /**
   * The replica responds in time and is available for requests.
   */
  Replica_Available
}
