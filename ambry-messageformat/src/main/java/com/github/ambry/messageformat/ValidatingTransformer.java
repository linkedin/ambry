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
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * An implementation of the {@link Transformer} interface that simply deserializes the message to validate it.
 */
public class ValidatingTransformer implements Transformer {
  private final StoreKeyFactory storeKeyFactory;
  private final static Logger logger = LoggerFactory.getLogger(ValidatingTransformer.class);

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
      header.verifyHeader();
      StoreKey keyInStream = storeKeyFactory.getStoreKey(new DataInputStream(msgStream));
      if (header.isPutRecord()) {
        if (header.hasLifeVersion() && header.getLifeVersion() != msgInfo.getLifeVersion()) {
          logger.trace("LifeVersion in stream: " + header.getLifeVersion() + " failed to match lifeVersion from Index: "
              + msgInfo.getLifeVersion() + " for key " + keyInStream);
        }
        encryptionKey = header.hasEncryptionKeyRecord() ? deserializeBlobEncryptionKey(msgStream) : null;
        props = deserializeBlobProperties(msgStream);
        metadata = deserializeUserMetadata(msgStream);
        blobData = deserializeBlob(msgStream);
      } else {
        throw new IllegalStateException("Message cannot be anything rather than put record ");
      }
      if (msgInfo.getStoreKey().equals(keyInStream)) {
        // BlobIDTransformer only exists on ambry-server and replication between servers is relying on blocking channel
        // which is still using java ByteBuffer. So, no need to consider releasing stuff.
        // @todo, when netty Bytebuf is adopted for blocking channel on ambry-server, remember to release this ByteBuf.
        PutMessageFormatInputStream transformedStream =
            new PutMessageFormatInputStream(keyInStream, encryptionKey, props, metadata,
                new ByteBufInputStream(blobData.content(), true), blobData.getSize(), blobData.getBlobType(),
                msgInfo.getLifeVersion());
        MessageInfo transformedMsgInfo =
            new MessageInfo(keyInStream, transformedStream.getSize(), false, msgInfo.isTtlUpdated(),
                false, msgInfo.getExpirationTimeInMs(), msgInfo.getCrc(), msgInfo.getAccountId(),
                msgInfo.getContainerId(), msgInfo.getOperationTimeMs(), msgInfo.getLifeVersion());
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

