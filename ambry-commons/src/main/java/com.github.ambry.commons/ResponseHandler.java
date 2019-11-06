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
package com.github.ambry.commons;

import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.server.ServerErrorCode;
import java.io.IOException;
import java.net.SocketException;
import java.util.List;


/**
 * ResponseHandler can be used by components whenever an operation encounters an error or an exception, to delegate
 * the responsibility of conveying appropriate replica related errors to the cluster map.
 * It can also be used to convey the information that a replica related operation was successful.
 * The cluster map uses this information to set soft states and dynamically handle failures.
 */

public class ResponseHandler {
  private ClusterMap clusterMap;

  /**
   * Construct a ResponseHandler instance.
   * @param clusterMap the {@link ClusterMap} associated with the cluster.
   */
  public ResponseHandler(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  /**
   * Act on an event in the form of a {@link ServerErrorCode} on the given {@link ReplicaId}
   * @param replicaId the {@link ReplicaId} to which the request that received the error was made.
   * @param errorCode the {@link ServerErrorCode} received for the request.
   */
  private void onServerEvent(ReplicaId replicaId, ServerErrorCode errorCode) {
    switch (errorCode) {
      case IO_Error:
      case Disk_Unavailable:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
        break;
      case Partition_ReadOnly:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Partition_ReadOnly);
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Replica_Available);
        break;
      case Temporarily_Disabled:
      case Replica_Unavailable:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Replica_Unavailable);
        break;
      default:
        // other server error codes
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Disk_Ok);
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Replica_Available);
        break;
    }
    // Regardless of what the error code is (or there is no error), it is a node response event.
    clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Node_Response);
  }

  /**
   * Act on an event in the form of a {@link NetworkClientErrorCode} on the given {@link ReplicaId}
   * @param replicaId the {@link ReplicaId} to which the request that received the error was made.
   * @param errorCode the {@link NetworkClientErrorCode} received for the request.
   */
  private void onNetworkEvent(ReplicaId replicaId, NetworkClientErrorCode errorCode) {
    switch (errorCode) {
      case NetworkError:
        clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Node_Timeout);
        break;
      default:
        break;
    }
  }

  /**
   * Perform the action when a request to the given {@link ReplicaId} is met with an exception.
   * @param replicaId the {@link ReplicaId} to which the request that received the exception was made.
   * @param e the {@link Exception} received.
   */
  private void onException(ReplicaId replicaId, Exception e) {
    if (e instanceof SocketException || e instanceof IOException || e instanceof ConnectionPoolTimeoutException) {
      clusterMap.onReplicaEvent(replicaId, ReplicaEventType.Node_Timeout);
    }
  }

  /**
   * Action to take when a request to the given {@link ReplicaId} results in an event. The event could come in the
   * form of an {@link Exception}, {@link NetworkClientErrorCode}, or a {@link ServerErrorCode} (possibly indicating
   * that there was no error).
   * @param replicaId the {@link ReplicaId} to which the request was sent.
   * @param event the type of the event. The event could be an {@link Exception}, {@link NetworkClientErrorCode} or a
   * {@link ServerErrorCode}.
   */
  public void onEvent(ReplicaId replicaId, Object event) {
    if(replicaId instanceof CloudReplica)
      return;
    if (event instanceof ServerErrorCode) {
      onServerEvent(replicaId, (ServerErrorCode) event);
    } else if (event instanceof Exception) {
      onException(replicaId, (Exception) event);
    } else if (event instanceof NetworkClientErrorCode) {
      onNetworkEvent(replicaId, (NetworkClientErrorCode) event);
    }
  }

  /**
   * Take action when connection to certain {@link DataNodeId} timed out. The clustermap is supposed to mark node resource
   * down and avoid subsequent requests routed to this node within down window.
   * @param dataNodeId the {@link DataNodeId} associated with timeout connection.
   */
  public void onConnectionTimeout(DataNodeId dataNodeId) {
    if (dataNodeId != null) {
      List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
      if (!replicaIds.isEmpty()) {
        // Pick any replica from this node to mark resource down
        clusterMap.onReplicaEvent(replicaIds.get(0), ReplicaEventType.Node_Timeout);
      }
    }
  }
}
