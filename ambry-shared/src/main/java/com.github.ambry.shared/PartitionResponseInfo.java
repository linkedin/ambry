package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.MessageInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Contains the partition and the message info list for a partition. This is used by
 * get response to specify the message info list in a partition
 */
public class PartitionResponseInfo {

  private final PartitionId partitionId;
  private final int messageInfoListSize;
  private final MessageInfoListSerde messageInfoListSerDe;

  public PartitionResponseInfo(PartitionId partitionId, List<MessageInfo> messageInfoList) {
    this.messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.partitionId = partitionId;
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoListSerDe.getMessageInfoList();
  }

  public static PartitionResponseInfo readFrom(DataInputStream stream, ClusterMap map)
      throws IOException {
    PartitionId partitionId = map.getPartitionIdFromStream(stream);
    List<MessageInfo> messageInfoList = MessageInfoListSerde.deserializeMessageInfoList(stream, map);
    return new PartitionResponseInfo(partitionId, messageInfoList);
  }

  public void writeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(partitionId.getBytes());
    messageInfoListSerDe.serializeMessageInfoList(byteBuffer);
  }

  public long sizeInBytes() {
    return partitionId.getBytes().length + messageInfoListSize;
  }
}
