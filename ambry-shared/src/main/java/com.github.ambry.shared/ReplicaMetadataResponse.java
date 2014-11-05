package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.FindToken;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import com.github.ambry.store.MessageInfo;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The response for a ReplicaMetadataRequest. This returns the new entries found
 * and the new token that could be used for future searches
 */
public class ReplicaMetadataResponse extends Response {

  private List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList;
  private int replicaMetadataResponseInfoListSizeInBytes;

  private static int Replica_Metadata_Response_Info_List_Size_In_Bytes = 4;

  public ReplicaMetadataResponse(int correlationId, String clientId, ServerErrorCode error,
      List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList) {
    super(RequestOrResponseType.ReplicaMetadataResponse, Request_Response_Version, correlationId, clientId, error);
    this.replicaMetadataResponseInfoList = replicaMetadataResponseInfoList;
    this.replicaMetadataResponseInfoListSizeInBytes = 0;
    for (ReplicaMetadataResponseInfo replicaMetadataResponseInfo : replicaMetadataResponseInfoList) {
      this.replicaMetadataResponseInfoListSizeInBytes += replicaMetadataResponseInfo.sizeInBytes();
    }
  }

  public ReplicaMetadataResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.ReplicaMetadataResponse, Request_Response_Version, correlationId, clientId, error);
    replicaMetadataResponseInfoList = null;
    replicaMetadataResponseInfoListSizeInBytes = 0;
  }

  public List<ReplicaMetadataResponseInfo> getReplicaMetadataResponseInfoList() {
    return replicaMetadataResponseInfoList;
  }

  public static ReplicaMetadataResponse readFrom(DataInputStream stream, FindTokenFactory factory,
      ClusterMap clusterMap)
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
          ReplicaMetadataResponseInfo.readFrom(stream, factory, clusterMap);
      replicaMetadataResponseInfoList.add(replicaMetadataResponseInfo);
    }
    if (error != ServerErrorCode.No_Error) {
      return new ReplicaMetadataResponse(correlationId, clientId, error);
    } else {
      // ignore version for now
      return new ReplicaMetadataResponse(correlationId, clientId, error,
          replicaMetadataResponseInfoList);
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
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
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
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
}
