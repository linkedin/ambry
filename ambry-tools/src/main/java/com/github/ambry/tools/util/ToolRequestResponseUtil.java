/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.util;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Pair;
import java.io.DataInputStream;
import java.io.InputStream;


public class ToolRequestResponseUtil {

  /**
   * Decodes the stream from given responseInfo to get {@link GetResponse}
   *
   * @param responseInfo {@link ResponseInfo} responseInfo
   * @param clusterMap {@link ClusterMap} clusterMap
   * @return serverErrorCode, InputStream, GetResponse
   * @throws Exception exception
   */
  public static Pair<Pair<ServerErrorCode, InputStream>, GetResponse> decodeGetResponse(ResponseInfo responseInfo,
      ClusterMap clusterMap) throws Exception {
    InputStream serverResponseStream = new NettyByteBufDataInputStream(responseInfo.content());
    GetResponse getResponse = GetResponse.readFrom(new DataInputStream(serverResponseStream), clusterMap);
    ServerErrorCode partitionErrorCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
    ServerErrorCode errorCode =
        partitionErrorCode == ServerErrorCode.NoError ? getResponse.getError() : partitionErrorCode;
    InputStream stream = errorCode == ServerErrorCode.NoError ? getResponse.getInputStream() : null;
    return new Pair<>(new Pair<>(errorCode, stream), getResponse);
  }

  /**
   * Get replica of given {@link PartitionId} from given {@link DataNodeId}. If partitionId is null, it returns any
   * replica on the certain node.
   * @param dataNodeId the {@link DataNodeId} on which replica resides.
   * @param partitionId the {@link PartitionId} which replica belongs to.
   * @return {@link ReplicaId} from given node.
   */
  public static ReplicaId getReplicaFromNode(DataNodeId dataNodeId, PartitionId partitionId, ClusterMap clusterMap) {
    ReplicaId replicaToReturn = null;
    if (partitionId != null) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        if (replicaId.getDataNodeId().getHostname().equals(dataNodeId.getHostname())) {
          replicaToReturn = replicaId;
          break;
        }
      }
    } else {
      // pick any replica on this node
      replicaToReturn = clusterMap.getReplicaIds(dataNodeId).get(0);
    }
    return replicaToReturn;
  }
}
