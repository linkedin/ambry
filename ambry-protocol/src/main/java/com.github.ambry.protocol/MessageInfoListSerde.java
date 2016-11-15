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
import com.github.ambry.commons.BlobId;
import com.github.ambry.store.MessageInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


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
    int messageInfoListCount = stream.readInt();
    ArrayList<MessageInfo> messageListInfo = new ArrayList<MessageInfo>(messageInfoListCount);
    for (int i = 0; i < messageInfoListCount; i++) {
      BlobId id = new BlobId(stream, map);
      long size = stream.readLong();
      long ttl = stream.readLong();
      byte b = stream.readByte();
      boolean isDeleted = false;
      if (b == 1) {
        isDeleted = true;
      }
      messageListInfo.add(new MessageInfo(id, size, isDeleted, ttl));
    }
    return messageListInfo;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoList;
  }
}
