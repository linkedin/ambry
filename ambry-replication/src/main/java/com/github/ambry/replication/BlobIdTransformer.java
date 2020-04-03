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
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.CompositeBlobInfo;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * Transformer implementation that replaces BlobIds in messages with
 * the converted value from the StoreKeyConverter
 */
public class BlobIdTransformer implements Transformer {

  private final StoreKeyConverter storeKeyConverter;
  private final StoreKeyFactory storeKeyFactory;
  private final static Logger logger = LoggerFactory.getLogger(BlobIdTransformer.class);

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
    storeKeyConverter.dropCache();
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
      case Message_Header_Version_V3:
        headerBuf = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V3.getHeaderSize());
        headerBuf.putShort(headerVersion);
        inputStream.read(headerBuf.array(), Version_Field_Size_In_Bytes,
            MessageFormatRecord.MessageHeader_Format_V3.getHeaderSize() - Version_Field_Size_In_Bytes);
        headerBuf.rewind();
        header = new MessageFormatRecord.MessageHeader_Format_V3(headerBuf);
        break;
      default:
        throw new MessageFormatException("Message header version [" + headerVersion + "] not supported",
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
   * @throws Exception
   */
  private Message newMessage(InputStream inputStream, StoreKey newKey, MessageInfo oldMessageInfo) throws Exception {
    MessageHeader_Format headerFormat = getMessageHeader(inputStream);
    storeKeyFactory.getStoreKey(new DataInputStream(inputStream));
    BlobId newBlobId = (BlobId) newKey;

    if (headerFormat.isPutRecord()) {
      if (headerFormat.hasLifeVersion() && headerFormat.getLifeVersion() != oldMessageInfo.getLifeVersion()) {
        // The original Put buffer might have lifeVersion as 0, but the message info might have a higher lifeVersion.
        logger.trace(
            "LifeVersion in stream: " + headerFormat.getLifeVersion() + " failed to match lifeVersion from Index: "
                + oldMessageInfo.getLifeVersion() + " for key " + oldMessageInfo.getStoreKey());
      }
      ByteBuffer blobEncryptionKey = null;
      if (headerFormat.hasEncryptionKeyRecord()) {
        blobEncryptionKey = deserializeBlobEncryptionKey(inputStream);
      }
      BlobProperties oldProperties = deserializeBlobProperties(inputStream);
      ByteBuffer userMetaData = deserializeUserMetadata(inputStream);
      BlobData blobData = deserializeBlob(inputStream);
      ByteBuf blobDataBytes = blobData.content();

      long blobPropertiesSize = oldProperties.getBlobSize();

      //If the blob is a metadata blob its data chunk id list
      //will be rewritten with transformed IDs
      if (blobData.getBlobType().equals(BlobType.MetadataBlob)) {
        ByteBuffer serializedMetadataContent = blobDataBytes.nioBuffer();
        CompositeBlobInfo compositeBlobInfo =
            MetadataContentSerDe.deserializeMetadataContentRecord(serializedMetadataContent, storeKeyFactory);
        Map<StoreKey, StoreKey> convertedKeys = storeKeyConverter.convert(compositeBlobInfo.getKeys());
        List<StoreKey> newKeys = new ArrayList<>();
        boolean isOldMetadataKeyDifferentFromNew = !oldMessageInfo.getStoreKey().getID().equals(newKey.getID());
        short metadataAccountId = newBlobId.getAccountId();
        short metadataContainerId = newBlobId.getContainerId();
        for (StoreKey oldDataChunkKey : compositeBlobInfo.getKeys()) {
          StoreKey newDataChunkKey = convertedKeys.get(oldDataChunkKey);
          if (newDataChunkKey == null) {
            throw new IllegalStateException("Found metadata chunk with a deprecated data chunk. " + " Old MetadataID: "
                + oldMessageInfo.getStoreKey().getID() + " New MetadataID: " + newKey.getID() + " Old Datachunk ID: "
                + oldDataChunkKey.getID());
          }
          if (isOldMetadataKeyDifferentFromNew && newDataChunkKey.getID().equals(oldDataChunkKey.getID())) {
            throw new IllegalStateException(
                "Found changed metadata chunk with an unchanged data chunk" + " Old MetadataID: "
                    + oldMessageInfo.getStoreKey().getID() + " New MetadataID: " + newKey.getID()
                    + " Old Datachunk ID: " + oldDataChunkKey.getID());
          }
          if (!isOldMetadataKeyDifferentFromNew && !newDataChunkKey.getID().equals(oldDataChunkKey.getID())) {
            throw new IllegalStateException(
                "Found unchanged metadata chunk with a changed data chunk" + " Old MetadataID: "
                    + oldMessageInfo.getStoreKey().getID() + " New MetadataID: " + newKey.getID()
                    + " Old Datachunk ID: " + oldDataChunkKey.getID() + " New Datachunk ID: "
                    + newDataChunkKey.getID());
          }
          BlobId newDataChunkBlobId = (BlobId) newDataChunkKey;
          if (newDataChunkBlobId.getAccountId() != metadataAccountId
              || newDataChunkBlobId.getContainerId() != metadataContainerId) {
            throw new IllegalStateException(
                "Found changed metadata chunk with a datachunk with a different account/container" + " Old MetadataID: "
                    + oldMessageInfo.getStoreKey().getID() + " New MetadataID: " + newKey.getID()
                    + " Old Datachunk ID: " + oldDataChunkKey.getID() + " New Datachunk ID: "
                    + newDataChunkBlobId.getID() + " Metadata AccountId: " + metadataAccountId
                    + " Metadata ContainerId: " + metadataContainerId + " Datachunk AccountId: "
                    + newDataChunkBlobId.getAccountId() + " Datachunk ContainerId: "
                    + newDataChunkBlobId.getContainerId());
          }
          newKeys.add(newDataChunkKey);
        }
        ByteBuffer metadataContent;
        if (compositeBlobInfo.getMetadataContentVersion() == Metadata_Content_Version_V2) {
          metadataContent = MetadataContentSerDe.serializeMetadataContentV2(compositeBlobInfo.getChunkSize(),
              compositeBlobInfo.getTotalSize(), newKeys);
        } else if (compositeBlobInfo.getMetadataContentVersion() == Metadata_Content_Version_V3) {
          List<Pair<StoreKey, Long>> keyAndSizeList = new ArrayList<>();
          List<CompositeBlobInfo.ChunkMetadata> chunkMetadataList = compositeBlobInfo.getChunkMetadataList();
          for (int i = 0; i < newKeys.size(); i++) {
            keyAndSizeList.add(new Pair<>(newKeys.get(i), chunkMetadataList.get(i).getSize()));
          }
          metadataContent =
              MetadataContentSerDe.serializeMetadataContentV3(compositeBlobInfo.getTotalSize(), keyAndSizeList);
        } else {
          throw new IllegalStateException("Unexpected metadata content version from composite blob: "
              + compositeBlobInfo.getMetadataContentVersion());
        }
        blobPropertiesSize = compositeBlobInfo.getTotalSize();
        metadataContent.flip();
        blobDataBytes.release();
        blobDataBytes = Unpooled.wrappedBuffer(metadataContent);
        blobData = new BlobData(blobData.getBlobType(), metadataContent.remaining(), blobDataBytes);
      }

      BlobProperties newProperties =
          new BlobProperties(blobPropertiesSize, oldProperties.getServiceId(), oldProperties.getOwnerId(),
              oldProperties.getContentType(), oldProperties.isPrivate(), oldProperties.getTimeToLiveInSeconds(),
              oldProperties.getCreationTimeInMs(), newBlobId.getAccountId(), newBlobId.getContainerId(),
              oldProperties.isEncrypted(), null);

      // BlobIDTransformer only exists on ambry-server and replication between servers is relying on blocking channel
      // which is still using java ByteBuffer. So, no need to consider releasing stuff.
      // @todo, when netty Bytebuf is adopted for blocking channel on ambry-server, remember to release this ByteBuf.
      PutMessageFormatInputStream putMessageFormatInputStream =
          new PutMessageFormatInputStream(newKey, blobEncryptionKey, newProperties, userMetaData,
              new ByteBufInputStream(blobDataBytes, true), blobData.getSize(), blobData.getBlobType(),
              oldMessageInfo.getLifeVersion());
      // Reuse the original CRC if present in the oldMessageInfo. This is important to ensure that messages that are
      // received via replication are sent to the store with proper CRCs (which the store needs to detect duplicate
      // messages). As an additional guard, here the original CRC is only reused if the key's ID in string form is the
      // same after conversion.
      Long originalCrc = oldMessageInfo.getStoreKey().getID().equals(newKey.getID()) ? oldMessageInfo.getCrc() : null;
      MessageInfo info =
          new MessageInfo(newKey, putMessageFormatInputStream.getSize(), false, oldMessageInfo.isTtlUpdated(), false,
              oldMessageInfo.getExpirationTimeInMs(), originalCrc, newProperties.getAccountId(),
              newProperties.getContainerId(), oldMessageInfo.getOperationTimeMs(), oldMessageInfo.getLifeVersion());
      return new Message(info, putMessageFormatInputStream);
    } else {
      throw new IllegalArgumentException("Only 'put' records are valid");
    }
  }
}
