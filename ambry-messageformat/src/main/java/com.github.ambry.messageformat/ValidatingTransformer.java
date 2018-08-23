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
package com.github.ambry.messageformat;

import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * An implementation of the {@link Transformer} interface that simply deserializes the message to validate it.
 */
public class ValidatingTransformer implements Transformer {
  private final StoreKeyFactory storeKeyFactory;

  public ValidatingTransformer(StoreKeyFactory storeKeyFactory, StoreKeyConverter storeKeyConverter) {
    this.storeKeyFactory = storeKeyFactory;
  }

  @Override
  public TransformationOutput transform(Message message) {
    ByteBuffer encryptionKey;
    BlobProperties props;
    ByteBuffer metadata;
    BlobData blobData;
    MessageInfo msgInfo = message.getMessageInfo();
    InputStream msgStream = message.getStream();
    TransformationOutput transformationOutput = null;
    try {
      // Read header
      ByteBuffer headerVersion = ByteBuffer.allocate(Version_Field_Size_In_Bytes);
      msgStream.read(headerVersion.array());
      short version = headerVersion.getShort();
      if (!isValidHeaderVersion(version)) {
        throw new MessageFormatException("Header version not supported " + version,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      int headerSize = getHeaderSizeForVersion(version);
      ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
      headerBuffer.put(headerVersion.array());
      msgStream.read(headerBuffer.array(), Version_Field_Size_In_Bytes, headerSize - Version_Field_Size_In_Bytes);
      headerBuffer.rewind();
      MessageHeader_Format header = getMessageHeader(version, headerBuffer);
      StoreKey keyInStream = storeKeyFactory.getStoreKey(new DataInputStream(msgStream));
      if (header.isPutRecord()) {
        encryptionKey = header.hasEncryptionKeyRecord() ? deserializeBlobEncryptionKey(msgStream) : null;
        props = deserializeBlobProperties(msgStream);
        metadata = deserializeUserMetadata(msgStream);
        blobData = deserializeBlob(msgStream);
      } else {
        throw new IllegalStateException("Message cannot be a deleted record ");
      }
      if (msgInfo.getStoreKey().equals(keyInStream)) {
        PutMessageFormatInputStream transformedStream =
            new PutMessageFormatInputStream(keyInStream, encryptionKey, props, metadata, blobData.getStream(),
                blobData.getSize(), blobData.getBlobType());
        MessageInfo transformedMsgInfo =
            new MessageInfo(keyInStream, transformedStream.getSize(), msgInfo.isDeleted(), msgInfo.isTtlUpdated(),
                msgInfo.getExpirationTimeInMs(), msgInfo.getCrc(), msgInfo.getAccountId(), msgInfo.getContainerId(),
                msgInfo.getOperationTimeMs());
        transformationOutput = new TransformationOutput(new Message(transformedMsgInfo, transformedStream));
      } else {
        throw new IllegalStateException(
            "StoreKey in stream: " + keyInStream + " failed to match store key from Index: " + msgInfo.getStoreKey());
      }
    } catch (Exception e) {
      transformationOutput = new TransformationOutput(e);
    }
    return transformationOutput;
  }

  @Override
  public void warmup(List<MessageInfo> messageInfos) throws Exception {
    //no-op
  }
}

