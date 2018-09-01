/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * Transformer implementation that replaces BlobIds in messages with
 * the converted value from the StoreKeyConverter
 */
public class BlobIdTransformer implements Transformer {

  private final StoreKeyConverter storeKeyConverter;
  private final StoreKeyFactory storeKeyFactory;

  /**
   * StoreKeyConverter should already have converted the expected list of IDs.
   * @param storeKeyConverter
   * @param storeKeyFactory
   */
  public BlobIdTransformer(StoreKeyFactory storeKeyFactory, StoreKeyConverter storeKeyConverter) {
    this.storeKeyFactory = Objects.requireNonNull(storeKeyFactory, "storeKeyFactory must not be null");
    this.storeKeyConverter = Objects.requireNonNull(storeKeyConverter, "storeKeyConverter must not be null");
  }

  @Override
  public TransformationOutput transform(Message message) {
    Message transformedMsg = null;
    try {
      Objects.requireNonNull(message, "message must not be null");
      Objects.requireNonNull(message.getMessageInfo(), "message's messageInfo must not be null");
      Objects.requireNonNull(message.getStream(), "message's inputStream must not be null");
      StoreKey oldStoreKey = message.getMessageInfo().getStoreKey();
      StoreKey newStoreKey = storeKeyConverter.getConverted(oldStoreKey);
      if (newStoreKey != null) {
        transformedMsg = newMessage(message.getStream(), newStoreKey, message.getMessageInfo());
      }
    } catch (Exception e) {
      return new TransformationOutput(e);
    }
    return new TransformationOutput(transformedMsg);
  }

  @Override
  public void warmup(List<MessageInfo> messageInfos) throws Exception {
    List<StoreKey> storeKeys = new ArrayList<>();
    for (MessageInfo messageInfo : messageInfos) {
      if (!messageInfo.isExpired() && !messageInfo.isDeleted()) {
        storeKeys.add(messageInfo.getStoreKey());
      }
    }
    storeKeyConverter.convert(storeKeys);
  }

  /**
   * Extracts the message header from the input stream
   * @param stream input stream from which the message header will be read
   * @return message header from stream, will be of either Message_Header_Version_V1 or Message_Header_Version_V2
   * @throws IOException
   * @throws MessageFormatException
   */
  private MessageFormatRecord.MessageHeader_Format getMessageHeader(InputStream stream)
      throws IOException, MessageFormatException {
    DataInputStream inputStream = new DataInputStream(stream);
    short headerVersion = inputStream.readShort();
    ByteBuffer headerBuf;
    MessageFormatRecord.MessageHeader_Format header;
    switch (headerVersion) {
      case Message_Header_Version_V1:
        headerBuf = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
        headerBuf.putShort(headerVersion);
        inputStream.read(headerBuf.array(), Version_Field_Size_In_Bytes,
            MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize() - Version_Field_Size_In_Bytes);
        headerBuf.rewind();
        header = new MessageFormatRecord.MessageHeader_Format_V1(headerBuf);
        break;
      case Message_Header_Version_V2:
        headerBuf = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V2.getHeaderSize());
        headerBuf.putShort(headerVersion);
        inputStream.read(headerBuf.array(), Version_Field_Size_In_Bytes,
            MessageFormatRecord.MessageHeader_Format_V2.getHeaderSize() - Version_Field_Size_In_Bytes);
        headerBuf.rewind();
        header = new MessageFormatRecord.MessageHeader_Format_V2(headerBuf);
        break;
      default:
        throw new MessageFormatException("Message header version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
    header.verifyHeader();
    return header;
  }

  /**
   * Creates a Message from the old Message
   * input stream, replacing the old store key and account/container IDs
   * with a new store key and account/container IDs
   * @param inputStream the input stream of the Message
   * @param newKey the new StoreKey
   * @param oldMessageInfo the {@link MessageInfo} of the message being transformed
   * @return new Message message
   * @throws IOException
   * @throws MessageFormatException
   */
  private Message newMessage(InputStream inputStream, StoreKey newKey, MessageInfo oldMessageInfo)
      throws IOException, MessageFormatException {
    MessageHeader_Format headerFormat = getMessageHeader(inputStream);
    storeKeyFactory.getStoreKey(new DataInputStream(inputStream));
    BlobId newBlobId = (BlobId) newKey;

    if (headerFormat.isPutRecord()) {
      ByteBuffer blobEncryptionKey = null;
      if (headerFormat.hasEncryptionKeyRecord()) {
        blobEncryptionKey = deserializeBlobEncryptionKey(inputStream);
      }
      BlobProperties oldProperties = deserializeBlobProperties(inputStream);
      BlobProperties newProperties =
          new BlobProperties(oldProperties.getBlobSize(), oldProperties.getServiceId(), oldProperties.getOwnerId(),
              oldProperties.getContentType(), oldProperties.isPrivate(), oldProperties.getTimeToLiveInSeconds(),
              oldProperties.getCreationTimeInMs(), newBlobId.getAccountId(), newBlobId.getContainerId(),
              oldProperties.isEncrypted());
      ByteBuffer userMetaData = deserializeUserMetadata(inputStream);
      BlobData blobData = deserializeBlob(inputStream);

      PutMessageFormatInputStream putMessageFormatInputStream =
          new PutMessageFormatInputStream(newKey, blobEncryptionKey, newProperties, userMetaData, blobData.getStream(),
              blobData.getSize(), blobData.getBlobType());
      // Reuse the original CRC if present in the oldMessageInfo. This is important to ensure that messages that are
      // received via replication are sent to the store with proper CRCs (which the store needs to detect duplicate
      // messages). As an additional guard, here the original CRC is only reused if the key's ID in string form is the
      // same after conversion.
      Long originalCrc = oldMessageInfo.getStoreKey().getID().equals(newKey.getID()) ? oldMessageInfo.getCrc() : null;
      MessageInfo info =
          new MessageInfo(newKey, putMessageFormatInputStream.getSize(), false, oldMessageInfo.isTtlUpdated(),
              oldMessageInfo.getExpirationTimeInMs(), originalCrc, newProperties.getAccountId(),
              newProperties.getContainerId(), oldMessageInfo.getOperationTimeMs());
      return new Message(info, putMessageFormatInputStream);
    } else {
      throw new IllegalArgumentException("Only 'put' records are valid");
    }
  }
}
