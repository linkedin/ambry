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
package com.github.ambry.messageformat;

import com.github.ambry.network.Send;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * A send object for the message format to send data from the underlying store
 * to the network channel
 */

public class MessageFormatSend implements Send {

  private MessageReadSet readSet;
  private MessageFormatFlags flag;
  private ArrayList<SendInfo> sendInfoList;
  private ArrayList<MessageMetadata> messageMetadataList;
  private long totalSizeToWrite;
  private long sizeWritten;
  private int currentWriteIndex;
  private long sizeWrittenFromCurrentIndex;
  private StoreKeyFactory storeKeyFactory;
  private Logger logger = LoggerFactory.getLogger(getClass());

  private class SendInfo {
    private long relativeOffset;
    private long sizeToSend;

    public SendInfo(long relativeOffset, long sizeToSend) {
      this.relativeOffset = relativeOffset;
      this.sizeToSend = sizeToSend;
    }

    public long relativeOffset() {
      return relativeOffset;
    }

    public long sizetoSend() {
      return sizeToSend;
    }
  }

  public MessageFormatSend(MessageReadSet readSet, MessageFormatFlags flag, MessageFormatMetrics metrics,
      StoreKeyFactory storeKeyFactory) throws IOException, MessageFormatException {
    this.readSet = readSet;
    this.flag = flag;
    this.storeKeyFactory = storeKeyFactory;
    totalSizeToWrite = 0;
    long startTime = SystemTime.getInstance().milliseconds();
    calculateOffsets();
    metrics.calculateOffsetMessageFormatSendTime.update(SystemTime.getInstance().milliseconds() - startTime);
    sizeWritten = 0;
    currentWriteIndex = 0;
    sizeWrittenFromCurrentIndex = 0;
  }

  /**
   * Calculates the offsets from the MessageReadSet that needs to be sent over the network
   * based on the type of data requested as indicated by the flags
   */
  private void calculateOffsets() throws IOException, MessageFormatException {
    try {
      // get size
      int messageCount = readSet.count();
      // for each message, determine the offset and size that needs to be sent based on the flag
      sendInfoList = new ArrayList<>(messageCount);
      messageMetadataList = new ArrayList<>(messageCount);
      logger.trace("Calculate offsets of messages for one partition, MessageFormatFlag : {} number of messages : {}",
          flag, messageCount);
      for (int i = 0; i < messageCount; i++) {
        if (flag == MessageFormatFlags.All) {
          // just copy over the total size and use relative offset to be 0
          // We do not have to check any version in this case as we dont
          // have to read any data to deserialize anything.
          sendInfoList.add(i, new SendInfo(0, readSet.sizeInBytes(i)));
          messageMetadataList.add(i, null);
          totalSizeToWrite += readSet.sizeInBytes(i);
        } else {
          // read header version
          long startTime = SystemTime.getInstance().milliseconds();
          ByteBuffer headerVersion = ByteBuffer.allocate(Version_Field_Size_In_Bytes);
          readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(headerVersion)), 0,
              Version_Field_Size_In_Bytes);
          logger.trace("Calculate offsets, read header version time: {}",
              SystemTime.getInstance().milliseconds() - startTime);

          headerVersion.flip();
          short version = headerVersion.getShort();
          if (!isValidHeaderVersion(version)) {
            throw new MessageFormatException(
                "Version not known while reading message - version " + version + ", StoreKey " + readSet.getKeyAt(i),
                MessageFormatErrorCodes.Unknown_Format_Version);
          }
          ByteBuffer header = ByteBuffer.allocate(getHeaderSizeForVersion(version));
          // read the header
          startTime = SystemTime.getInstance().milliseconds();
          headerVersion.clear();
          header.putShort(headerVersion.getShort());
          readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(header)), Version_Field_Size_In_Bytes,
              header.capacity() - Version_Field_Size_In_Bytes);
          logger.trace("Calculate offsets, read header time: {}", SystemTime.getInstance().milliseconds() - startTime);

          startTime = SystemTime.getInstance().milliseconds();
          header.flip();
          MessageHeader_Format headerFormat = getMessageHeader(version, header);
          headerFormat.verifyHeader();
          int storeKeyRelativeOffset = header.capacity();

          StoreKey storeKey = storeKeyFactory.getStoreKey(
              new DataInputStream(new MessageReadSetIndexInputStream(readSet, i, storeKeyRelativeOffset)));
          if (storeKey.compareTo(readSet.getKeyAt(i)) != 0) {
            throw new MessageFormatException(
                "Id mismatch between metadata and store - metadataId " + readSet.getKeyAt(i) + " storeId " + storeKey,
                MessageFormatErrorCodes.Store_Key_Id_MisMatch);
          }
          logger.trace("Calculate offsets, verify header time: {}",
              SystemTime.getInstance().milliseconds() - startTime);

          startTime = SystemTime.getInstance().milliseconds();
          if (flag == MessageFormatFlags.BlobProperties) {
            sendInfoList.add(i, new SendInfo(headerFormat.getBlobPropertiesRecordRelativeOffset(),
                headerFormat.getBlobPropertiesRecordSize()));
            messageMetadataList.add(null);
            totalSizeToWrite += headerFormat.getBlobPropertiesRecordSize();
            logger.trace("Calculate offsets, get total size of blob properties time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace("Sending blob properties for message relativeOffset : {} size : {}",
                sendInfoList.get(i).relativeOffset(), sendInfoList.get(i).sizetoSend());
          } else if (flag == MessageFormatFlags.BlobUserMetadata) {
            messageMetadataList.add(headerFormat.hasEncryptionKeyRecord() ? new MessageMetadata(
                extractEncryptionKey(i, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                    headerFormat.getBlobEncryptionKeyRecordSize())) : null);
            sendInfoList.add(i, new SendInfo(headerFormat.getUserMetadataRecordRelativeOffset(),
                headerFormat.getUserMetadataRecordSize()));
            totalSizeToWrite += headerFormat.getUserMetadataRecordSize();
            logger.trace("Calculate offsets, get total size of user metadata time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace("Sending user metadata for message relativeOffset : {} size : {}",
                sendInfoList.get(i).relativeOffset(), sendInfoList.get(i).sizetoSend());
          } else if (flag == MessageFormatFlags.BlobInfo) {
            messageMetadataList.add(headerFormat.hasEncryptionKeyRecord() ? new MessageMetadata(
                extractEncryptionKey(i, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                    headerFormat.getBlobEncryptionKeyRecordSize())) : null);
            sendInfoList.add(i, new SendInfo(headerFormat.getBlobPropertiesRecordRelativeOffset(),
                headerFormat.getBlobPropertiesRecordSize() + headerFormat.getUserMetadataRecordSize()));
            totalSizeToWrite += headerFormat.getBlobPropertiesRecordSize() + headerFormat.getUserMetadataRecordSize();
            logger.trace("Calculate offsets, get total size of blob info time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace(
                "Sending blob info (blob properties + user metadata) for message relativeOffset : {} " + "size : {}",
                sendInfoList.get(i).relativeOffset(), sendInfoList.get(i).sizetoSend());
          } else if (flag == MessageFormatFlags.Blob) {
            messageMetadataList.add(headerFormat.hasEncryptionKeyRecord() ? new MessageMetadata(
                extractEncryptionKey(i, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                    headerFormat.getBlobEncryptionKeyRecordSize())) : null);
            sendInfoList.add(i,
                new SendInfo(headerFormat.getBlobRecordRelativeOffset(), headerFormat.getBlobRecordSize()));
            totalSizeToWrite += headerFormat.getBlobRecordSize();
            logger.trace("Calculate offsets, get total size of blob time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace("Sending data for message relativeOffset : {} size : {}", sendInfoList.get(i).relativeOffset(),
                sendInfoList.get(i).sizetoSend());
          } else {
            throw new MessageFormatException("Unknown flag in request " + flag, MessageFormatErrorCodes.IO_Error);
          }
        }
      }
    } catch (IOException e) {
      logger.trace("IOError when calculating offsets");
      throw new MessageFormatException("IOError when calculating offsets ", e, MessageFormatErrorCodes.IO_Error);
    }
  }

  /**
   * Extract the encryption key from the message at the given index from the readSet.
   * @param readSetIndex the index in the readSet for the message from which the encryption key has to be extracted.
   * @param encryptionKeyRelativeOffset the relative offset of the encryption key record in the message.
   * @param encryptionKeySize the size of the encryption key record in the message.
   * @return the extracted encryption key.
   * @throws IOException if an IO error is encountered while deserializing the message.
   * @throws MessageFormatException if a Message Format error is encountered while deserializing the message.
   */
  private ByteBuffer extractEncryptionKey(int readSetIndex, int encryptionKeyRelativeOffset, int encryptionKeySize)
      throws IOException, MessageFormatException {
    ByteBuffer serializedEncryptionKeyRecord = ByteBuffer.allocate(encryptionKeySize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(serializedEncryptionKeyRecord)),
        encryptionKeyRelativeOffset, encryptionKeySize);
    serializedEncryptionKeyRecord.flip();
    return deserializeBlobEncryptionKey(new ByteBufferInputStream(serializedEncryptionKeyRecord));
  }

  public List<MessageMetadata> getMessageMetadataList() {
    return messageMetadataList;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (!isSendComplete()) {
      written = readSet.writeTo(currentWriteIndex, channel,
          sendInfoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
          sendInfoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex);
      logger.trace("writeindex {} relativeOffset {} maxSize {} written {}", currentWriteIndex,
          sendInfoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
          sendInfoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex, written);
      sizeWritten += written;
      sizeWrittenFromCurrentIndex += written;
      logger.trace("size written in this loop : {} size written till now : {}", written, sizeWritten);
      if (sizeWrittenFromCurrentIndex == sendInfoList.get(currentWriteIndex).sizetoSend()) {
        currentWriteIndex++;
        sizeWrittenFromCurrentIndex = 0;
      }
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return totalSizeToWrite == sizeWritten;
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}

/**
 * A stream representation of the message read set. This helps to
 * read the read set as a sequential stream even though the underlying
 * representation can be random
 */
class MessageReadSetIndexInputStream extends InputStream {

  private final MessageReadSet messageReadSet;
  private final int indexToRead;
  private int currentOffset;

  public MessageReadSetIndexInputStream(MessageReadSet messageReadSet, int indexToRead, int startingOffset) {
    this.messageReadSet = messageReadSet;
    if (indexToRead >= messageReadSet.count()) {
      throw new IllegalArgumentException(
          "The index provided " + indexToRead + " " + "is outside the bounds of the number of messages in the read set "
              + messageReadSet.count());
    }
    this.indexToRead = indexToRead;
    this.currentOffset = startingOffset;
  }

  @Override
  public int read() throws IOException {
    if (currentOffset == messageReadSet.sizeInBytes(indexToRead)) {
      throw new IOException("Reached end of stream of message read set");
    }
    ByteBuffer buf = ByteBuffer.allocate(1);
    ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(buf);
    long bytesRead = messageReadSet.writeTo(indexToRead, Channels.newChannel(bufferStream), currentOffset, 1);
    if (bytesRead != 1) {
      throw new IllegalStateException("Number of bytes read for read from messageReadSet should be 1");
    }
    currentOffset++;
    buf.flip();
    return buf.get() & 0xFF;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (currentOffset == messageReadSet.sizeInBytes(indexToRead)) {
      throw new IOException("Reached end of stream of message read set");
    }
    ByteBuffer buf = ByteBuffer.wrap(b);
    ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(buf);
    long sizeToRead = Math.min(len - off, messageReadSet.sizeInBytes(indexToRead) - currentOffset);
    long bytesWritten =
        messageReadSet.writeTo(indexToRead, Channels.newChannel(bufferStream), currentOffset, sizeToRead);
    currentOffset += bytesWritten;
    return (int) bytesWritten;
  }
}
