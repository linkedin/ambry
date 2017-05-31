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
class MessageInfoListSerde {

  private final List<MessageInfo> messageInfoList;
  static final short MessageInfoListVersion_V1 = 1;
  static final short MessageInfoListVersion_V2 = 2;
  static final short MessageInfoListVersion_V3 = 3;

  private final short version;

  private static final byte CRC_PRESENT = (byte) 1;
  private static final byte DELETED = (byte) 1;

  static final int SHORT_FIELD_SIZE_IN_BYTES = 2;
  static final int INT_FIELD_SIZE_IN_BYTES = 4;
  static final int LONG_FIELD_SIZE_IN_BYTES = 8;

  MessageInfoListSerde(List<MessageInfo> messageInfoList, short version) {
    this.messageInfoList = messageInfoList;
    this.version = version;
  }

  int getMessageInfoListSize() {
    if (messageInfoList == null) {
      return INT_FIELD_SIZE_IN_BYTES;
    }
    int size = 0;
    size += INT_FIELD_SIZE_IN_BYTES;
    for (MessageInfo messageInfo : messageInfoList) {
      size += messageInfo.getStoreKey().sizeInBytes();
      // message size
      size += LONG_FIELD_SIZE_IN_BYTES;
      // expiration time
      size += LONG_FIELD_SIZE_IN_BYTES;
      // whether deleted
      size += 1;
      if (version == MessageInfoListVersion_V2 || version == MessageInfoListVersion_V3) {
        // whether crc is present
        size += 1;
        if (messageInfo.getCrc() != null) {
          // crc
          size += LONG_FIELD_SIZE_IN_BYTES;
        }
      }
      if (version == MessageInfoListVersion_V3) {
        // accountId
        size += SHORT_FIELD_SIZE_IN_BYTES;
        // containerId
        size += SHORT_FIELD_SIZE_IN_BYTES;
        // operationTime
        size += LONG_FIELD_SIZE_IN_BYTES;
      }
    }
    return size;
  }

  void serializeMessageInfoList(ByteBuffer outputBuffer) {
    outputBuffer.putInt(messageInfoList == null ? 0 : messageInfoList.size());
    if (messageInfoList != null) {
      for (MessageInfo messageInfo : messageInfoList) {
        outputBuffer.put(messageInfo.getStoreKey().toBytes());
        outputBuffer.putLong(messageInfo.getSize());
        outputBuffer.putLong(messageInfo.getExpirationTimeInMs());
        outputBuffer.put(messageInfo.isDeleted() ? DELETED : (byte) ~DELETED);
        if (version == MessageInfoListVersion_V2 || version == MessageInfoListVersion_V3) {
          Long crc = messageInfo.getCrc();
          if (crc != null) {
            outputBuffer.put(CRC_PRESENT);
            outputBuffer.putLong(crc);
          } else {
            outputBuffer.put((byte) ~CRC_PRESENT);
          }
        }
        if (version == MessageInfoListVersion_V3) {
          outputBuffer.putShort(messageInfo.getAccountId());
          outputBuffer.putShort(messageInfo.getContainerId());
          outputBuffer.putLong(messageInfo.getOperationTimeMs());
        }
      }
    }
  }

  static List<MessageInfo> deserializeMessageInfoList(DataInputStream stream, ClusterMap map,
      short versionToDeserializeIn) throws IOException {
    int messageInfoListCount = stream.readInt();
    ArrayList<MessageInfo> messageListInfo = new ArrayList<MessageInfo>(messageInfoListCount);
    for (int i = 0; i < messageInfoListCount; i++) {
      BlobId id = new BlobId(stream, map);
      long size = stream.readLong();
      long ttl = stream.readLong();
      boolean isDeleted = stream.readByte() == DELETED;
      Long crc = null;
      if (versionToDeserializeIn == MessageInfoListVersion_V2 || versionToDeserializeIn == MessageInfoListVersion_V3) {
        crc = stream.readByte() == CRC_PRESENT ? stream.readLong() : null;
      }
      if (versionToDeserializeIn == MessageInfoListVersion_V1 || versionToDeserializeIn == MessageInfoListVersion_V2) {
        messageListInfo.add(new MessageInfo.MessageInfoBuilder(id, size).setDeleted(isDeleted)
            .setExpirationTimeMs(ttl)
            .setCRC(crc)
            .build());
      } else if (versionToDeserializeIn == MessageInfoListVersion_V3) {
        short accountId = stream.readShort();
        short containerId = stream.readShort();
        long operationTime = stream.readLong();
        messageListInfo.add(new MessageInfo.MessageInfoBuilder(id, size).setDeleted(isDeleted)
            .setExpirationTimeMs(ttl)
            .setCRC(crc)
            .setAccountId(accountId)
            .setContainerId(containerId)
            .setOperationTimeMs(operationTime)
            .build());
      }
    }
    return messageListInfo;
  }

  List<MessageInfo> getMessageInfoList() {
    return messageInfoList;
  }
}
