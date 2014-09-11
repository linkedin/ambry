package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;


/**
 * GetRequest to fetch data
 */
public class GetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private List<PartitionRequestInfo> partitionRequestInfoList;
  private int sizeSent;
  private int totalPartitionRequestInfoListSize;

  private static final int MessageFormat_Size_InBytes = 2;
  private static final int Partition_Request_Info_List_Size = 4;

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList) {
    super(RequestOrResponseType.GetRequest, Request_Response_Version, correlationId, clientId);

    this.flags = flags;
    if (partitionRequestInfoList == null) {
      throw new IllegalArgumentException("No partition info specified in GetRequest");
    }
    this.partitionRequestInfoList = partitionRequestInfoList;
    for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
      totalPartitionRequestInfoListSize += partitionRequestInfo.sizeInBytes();
    }
    this.sizeSent = 0;
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public List<PartitionRequestInfo> getPartitionInfoList() {
    return partitionRequestInfoList;
  }

  public static GetRequest readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    RequestOrResponseType type = RequestOrResponseType.GetRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    int totalNumberOfPartitionInfo = stream.readInt();
    ArrayList<PartitionRequestInfo> partitionRequestInfoList =
        new ArrayList<PartitionRequestInfo>(totalNumberOfPartitionInfo);
    for (int i = 0; i < totalNumberOfPartitionInfo; i++) {
      PartitionRequestInfo partitionRequestInfo = PartitionRequestInfo.readFrom(stream, clusterMap);
      partitionRequestInfoList.add(partitionRequestInfo);
    }
    // ignore version for now
    return new GetRequest(correlationId, clientId, messageType, partitionRequestInfoList);
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
      bufferToSend.putInt(partitionRequestInfoList.size());
      for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
        partitionRequestInfo.writeTo(bufferToSend);
      }
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      int written = channel.write(bufferToSend);
      sizeSent += written;
    }
  }

  @Override
  public boolean isSendComplete() {
    return sizeSent == sizeInBytes();
  }

  @Override
  public long sizeInBytes() {
    // header + message format size + partition request info size + total partition request info list size
    return super.sizeInBytes() + MessageFormat_Size_InBytes +
        Partition_Request_Info_List_Size + totalPartitionRequestInfoListSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GetRequest[");
    sb.append(", ").append("MessageFormatFlags=").append(flags);
    for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
      sb.append(", ").append(partitionRequestInfo.toString());
    }
    sb.append("]");
    return sb.toString();
  }
}
