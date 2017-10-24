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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;


/**
 * A serde for serializing and deserializing {@link MessageInfo} and {@link MessageMetadata} lists.
 */
class MessageInfoAndMetadataListSerde {
  private final List<MessageInfo> messageInfoList;
  private final List<MessageMetadata> messageMetadataList;
  static final short VERSION_1 = 1;
  static final short VERSION_2 = 2;
  static final short VERSION_3 = 3;
  static final short VERSION_4 = 4;

  private final short version;

  private static final byte FIELD_PRESENT = (byte) 1;
  private static final byte DELETED = (byte) 1;

  MessageInfoAndMetadataListSerde(List<MessageInfo> messageInfoList, List<MessageMetadata> messageMetadataList,
      short version) {
    if (messageInfoList != null && messageMetadataList != null
        && messageInfoList.size() != messageMetadataList.size()) {
      throw new IllegalArgumentException(
          "Mismatch in the number of messages in message Info list: " + messageInfoList.size()
              + " and message metadata list: " + messageMetadataList.size());
    }
    this.messageInfoList = messageInfoList;
    this.messageMetadataList = messageMetadataList;
    this.version = version;
  }

  MessageInfoAndMetadataListSerde(List<MessageInfo> messageInfoList, short version) {
    this(messageInfoList,
        messageInfoList == null ? null : new ArrayList<>(Collections.nCopies(messageInfoList.size(), null)), version);
  }

  /**
   * @return the size in bytes that serialization of this MessageInfoAndMetadataList will result in.
   */
  int getMessageInfoAndMetadataListSize() {
    if (messageInfoList == null) {
      return Integer.BYTES;
    }
    int size = Integer.BYTES;
    ListIterator<MessageInfo> infoListIterator = messageInfoList.listIterator();
    ListIterator<MessageMetadata> metadataListIterator = messageMetadataList.listIterator();
    while (infoListIterator.hasNext()) {
      MessageInfo messageInfo = infoListIterator.next();
      MessageMetadata messageMetadata = metadataListIterator.next();
      size += messageInfo.getStoreKey().sizeInBytes();
      // message size
      size += Long.BYTES;
      // expiration time
      size += Long.BYTES;
      // whether deleted
      size += 1;
      if (version < VERSION_1 || version > VERSION_4) {
        throw new IllegalArgumentException("Unknown version in MessageInfoList " + version);
      }
      if (version > VERSION_1) {
        // whether crc is present
        size += 1;
        if (messageInfo.getCrc() != null) {
          // crc
          size += Long.BYTES;
        }
      }
      if (version > VERSION_2) {
        // accountId
        size += Short.BYTES;
        // containerId
        size += Short.BYTES;
        // operationTime
        size += Long.BYTES;
      }
      if (version > VERSION_3) {
        // whether message metadata is present.
        size += Byte.BYTES;
        if (messageMetadata != null) {
          size += messageMetadata.sizeInBytes();
        }
      }
    }
    return size;
  }

  /**
   * Serialize this object into the given buffer.
   * @param outputBuffer the ByteBuffer to serialize into.
   */
  void serializeMessageInfoAndMetadataList(ByteBuffer outputBuffer) {
    outputBuffer.putInt(messageInfoList == null ? 0 : messageInfoList.size());
    if (messageInfoList != null) {
      ListIterator<MessageInfo> infoListIterator = messageInfoList.listIterator();
      ListIterator<MessageMetadata> metadataListIterator = messageMetadataList.listIterator();
      while (infoListIterator.hasNext()) {
        MessageInfo messageInfo = infoListIterator.next();
        MessageMetadata messageMetadata = metadataListIterator.next();
        outputBuffer.put(messageInfo.getStoreKey().toBytes());
        outputBuffer.putLong(messageInfo.getSize());
        outputBuffer.putLong(messageInfo.getExpirationTimeInMs());
        outputBuffer.put(messageInfo.isDeleted() ? DELETED : (byte) ~DELETED);
        if (version < VERSION_1 || version > VERSION_4) {
          throw new IllegalArgumentException("Unknown version in MessageInfoList " + version);
        }
        if (version > VERSION_1) {
          Long crc = messageInfo.getCrc();
          if (crc != null) {
            outputBuffer.put(FIELD_PRESENT);
            outputBuffer.putLong(crc);
          } else {
            outputBuffer.put((byte) ~FIELD_PRESENT);
          }
        }
        if (version > VERSION_2) {
          outputBuffer.putShort(messageInfo.getAccountId());
          outputBuffer.putShort(messageInfo.getContainerId());
          outputBuffer.putLong(messageInfo.getOperationTimeMs());
        }
        if (version > VERSION_3) {
          if (messageMetadata != null) {
            outputBuffer.put(FIELD_PRESENT);
            messageMetadata.serializeMessageMetadata(outputBuffer);
          } else {
            outputBuffer.put((byte) ~FIELD_PRESENT);
          }
        }
      }
    }
  }

  /**
   * Deserialize the given stream and return the MessageInfo and Metadata lists.
   * @param stream the stream to deserialize from.
   * @param map the clustermap to use.
   * @param versionToDeserializeIn the SerDe version to use to deserialize.
   * @return a pair of {@link MessageInfo} list and {@link MessageMetadata} list.
   * @throws IOException if an I/O error occurs while reading from the stream.
   */
  static Pair<List<MessageInfo>, List<MessageMetadata>> deserializeMessageInfoAndMetadataList(DataInputStream stream,
      ClusterMap map, short versionToDeserializeIn) throws IOException {
    int messageCount = stream.readInt();
    ArrayList<MessageInfo> messageInfoList = new ArrayList<>(messageCount);
    ArrayList<MessageMetadata> messageMetadataList = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      BlobId id = new BlobId(stream, map);
      long size = stream.readLong();
      long ttl = stream.readLong();
      boolean isDeleted = stream.readByte() == DELETED;
      Long crc = null;
      short accountId = Account.UNKNOWN_ACCOUNT_ID;
      short containerId = Container.UNKNOWN_CONTAINER_ID;
      long operationTime = Utils.Infinite_Time;
      if (versionToDeserializeIn < VERSION_1 || versionToDeserializeIn > VERSION_4) {
        throw new IllegalArgumentException("Unknown version to deserialize MessageInfoList " + versionToDeserializeIn);
      }
      if (versionToDeserializeIn > VERSION_1) {
        crc = stream.readByte() == FIELD_PRESENT ? stream.readLong() : null;
      }
      if (versionToDeserializeIn > VERSION_2) {
        accountId = stream.readShort();
        containerId = stream.readShort();
        operationTime = stream.readLong();
      }

      messageInfoList.add(new MessageInfo(id, size, isDeleted, ttl, crc, accountId, containerId, operationTime));

      if (versionToDeserializeIn > VERSION_3) {
        MessageMetadata messageMetadata =
            stream.readByte() == FIELD_PRESENT ? MessageMetadata.deserializeMessageMetadata(stream) : null;
        messageMetadataList.add(messageMetadata);
      } else {
        messageMetadataList.add(null);
      }
    }
    return new Pair<>(messageInfoList, messageMetadataList);
  }

  List<MessageInfo> getMessageInfoList() {
    return messageInfoList;
  }

  List<MessageMetadata> getMessageMetadataList() {
    return messageMetadataList;
  }
}
