package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.store.MessageInfo;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A serde for serializing and deserializing list of message info
 */
public class MessageInfoListSerde {

  private final List<MessageInfo> messageInfoList;

  public MessageInfoListSerde(List<MessageInfo> messageInfoList) {
    this.messageInfoList = messageInfoList;
  }

  public int getMessageInfoListSize() {
    int listcountSize = 4;
    if (messageInfoList == null) {
      return listcountSize;
    }
    int size = 0;
    int longfieldSize = 8;
    size += listcountSize;
    if (messageInfoList != null) {
      for (MessageInfo messageInfo : messageInfoList) {
        size += messageInfo.getStoreKey().sizeInBytes();
        size += longfieldSize;
        size += longfieldSize;
        size += 1;
      }
    }
    return size;
  }

  public void serializeMessageInfoList(ByteBuffer outputBuffer) {
    outputBuffer.putInt(messageInfoList == null ? 0 : messageInfoList.size());
    if (messageInfoList != null) {
      for (MessageInfo messageInfo : messageInfoList) {
        outputBuffer.put(messageInfo.getStoreKey().toBytes());
        outputBuffer.putLong(messageInfo.getSize());
        outputBuffer.putLong(messageInfo.getExpirationTimeInMs());
        outputBuffer.put(messageInfo.isDeleted() ? (byte) 1 : 0);
      }
    }
  }

  public static List<MessageInfo> deserializeMessageInfoList(DataInputStream stream, ClusterMap map)
      throws IOException {
    final Logger logger = LoggerFactory.getLogger(MessageInfoListSerde.class);
    int messageInfoListCount = stream.readInt();
    logger.info("MessageInfoListSerde messageInfoListCount " + messageInfoListCount);
    ArrayList<MessageInfo> messageListInfo = new ArrayList<MessageInfo>(messageInfoListCount);
    for (int i = 0; i < messageInfoListCount; i++) {
      BlobId id = new BlobId(stream, map);
      logger.info("MessageInfoListSerde blobId " + id);
      long size = stream.readLong();
      logger.info("MessageInfoListSerde size " + size);
      long ttl = stream.readLong();
      logger.info("MessageInfoListSerde ttl " + ttl);
      byte b = stream.readByte();
      boolean isDeleted = false;
      if (b == 1) {
        isDeleted = true;
      }
      logger.info("MessageInfoListSerde isDeleted " + isDeleted);
      messageListInfo.add(new MessageInfo(id, size, isDeleted, ttl));
    }
    return messageListInfo;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoList;
  }
}
