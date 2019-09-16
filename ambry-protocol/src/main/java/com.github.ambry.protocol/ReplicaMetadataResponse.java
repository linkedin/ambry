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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;


/**
 * The response for a ReplicaMetadataRequest. This returns the new entries found
 * and the new token that could be used for future searches
 */
public class ReplicaMetadataResponse extends Response {

  private List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList;
  private int replicaMetadataResponseInfoListSizeInBytes;

  private static int Replica_Metadata_Response_Info_List_Size_In_Bytes = 4;

  public static final short REPLICA_METADATA_RESPONSE_VERSION_V_1 = 1;
  public static final short REPLICA_METADATA_RESPONSE_VERSION_V_2 = 2;
  public static final short REPLICA_METADATA_RESPONSE_VERSION_V_3 = 3;
  public static final short REPLICA_METADATA_RESPONSE_VERSION_V_4 = 4;
  public static final short REPLICA_METADATA_RESPONSE_VERSION_V_5 = 5;
  public static final short REPLICA_METADATA_RESPONSE_VERSION_V_6 = 6;

  public ReplicaMetadataResponse(int correlationId, String clientId, ServerErrorCode error,
      List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList, short version) {
    super(RequestOrResponseType.ReplicaMetadataResponse, version, correlationId, clientId, error);
    validateVersion(version);
    this.replicaMetadataResponseInfoList = replicaMetadataResponseInfoList;
    this.replicaMetadataResponseInfoListSizeInBytes = 0;
    for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : replicaMetadataResponseInfoList) {
      this.replicaMetadataResponseInfoListSizeInBytes += replicaMetadataResponseInfo.sizeInBytes();
    }
  }

  public ReplicaMetadataResponse(int correlationId, String clientId, ServerErrorCode error, short version) {
    super(RequestOrResponseType.ReplicaMetadataResponse, version, correlationId, clientId, error);
    validateVersion(version);
    replicaMetadataResponseInfoList = null;
    replicaMetadataResponseInfoListSizeInBytes = 0;
  }

  public List<ReplicaMetadataResponseInfo> getReplicaMetadataResponseInfoList() {
    return replicaMetadataResponseInfoList;
  }

  public static ReplicaMetadataResponse readFrom(DataInputStream stream, FindTokenHelper helper, ClusterMap clusterMap)
      throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.ReplicaMetadataResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    int replicaMetadataResponseInfoListCount = stream.readInt();
    ArrayList<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList =
        new ArrayList<ReplicaMetadataResponseInfo>(replicaMetadataResponseInfoListCount);
    for (int i = 0; i < replicaMetadataResponseInfoListCount; i++) {
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
          ReplicaMetadataResponseInfo.readFrom(stream, helper, clusterMap, versionId);
      replicaMetadataResponseInfoList.add(replicaMetadataResponseInfo);
    }
    if (error != ServerErrorCode.No_Error) {
      return new ReplicaMetadataResponse(correlationId, clientId, error, versionId);
    } else {
      return new ReplicaMetadataResponse(correlationId, clientId, error, replicaMetadataResponseInfoList, versionId);
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      if (replicaMetadataResponseInfoList != null) {
        bufferToSend.putInt(replicaMetadataResponseInfoList.size());
        for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : replicaMetadataResponseInfoList) {
          replicaMetadataResponseInfo.writeTo(bufferToSend);
        }
      } else {
        bufferToSend.putInt(0);
      }
      bufferToSend.flip();
    }
    return bufferToSend.remaining() > 0 ? channel.write(bufferToSend) : 0;
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend != null && bufferToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + Replica_Metadata_Response_Info_List_Size_In_Bytes
        + replicaMetadataResponseInfoListSizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReplicaMetadataResponse[");
    sb.append("ServerErrorCode=").append(getError());
    if (replicaMetadataResponseInfoList != null) {
      sb.append(" ReplicaMetadataResponseInfo ");
      for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : replicaMetadataResponseInfoList) {
        sb.append(replicaMetadataResponseInfo.toString());
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * validate that the version is valid.
   * @param version to validate
   */
  static void validateVersion(short version) {
    if (version < REPLICA_METADATA_RESPONSE_VERSION_V_1 || version > REPLICA_METADATA_RESPONSE_VERSION_V_6) {
      throw new IllegalArgumentException("Invalid replica metadata response version: " + version);
    }
  }

  /**
   * Get the compatible response version for the given request version.
   * @param requestVersion for which to get the compatible response version.
   * @return compatible responseVersion
   */
  public static short getCompatibleResponseVersion(short requestVersion) {
    switch (requestVersion) {
      case ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1:
        return REPLICA_METADATA_RESPONSE_VERSION_V_5;
      case ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2:
        return REPLICA_METADATA_RESPONSE_VERSION_V_6;
      default:
        throw new IllegalArgumentException("Invalid replica metadata request version: " + requestVersion
            + " No compatible replica metadata response version found");
    }
  }
}
