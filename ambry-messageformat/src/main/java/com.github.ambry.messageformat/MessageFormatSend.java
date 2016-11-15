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
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A send object for the message format to send data from the underlying store
 * to the network channel
 */

public class MessageFormatSend implements Send {

  private MessageReadSet readSet;
  private MessageFormatFlags flag;
  private ArrayList<SendInfo> infoList;
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

  // calculates the offsets from the MessageReadSet that needs to be sent over the network
  // based on the type of data requested as indicated by the flags
  private void calculateOffsets() throws IOException, MessageFormatException {
    try {
      // get size
      int messageCount = readSet.count();
      // for each message, determine the offset and size that needs to be sent based on the flag
      infoList = new ArrayList<SendInfo>(messageCount);
      logger.trace("Calculate offsets of messages for one partition, MessageFormatFlag : {} number of messages : {}",
          flag, messageCount);
      for (int i = 0; i < messageCount; i++) {
        if (flag == MessageFormatFlags.All) {
          // just copy over the total size and use relative offset to be 0
          // We do not have to check any version in this case as we dont
          // have to read any data to deserialize anything.
          infoList.add(i, new SendInfo(0, readSet.sizeInBytes(i)));
          totalSizeToWrite += readSet.sizeInBytes(i);
        } else {
          // read header version
          long startTime = SystemTime.getInstance().milliseconds();
          ByteBuffer headerVersion = ByteBuffer.allocate(MessageFormatRecord.Version_Field_Size_In_Bytes);
          readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(headerVersion)), 0,
              MessageFormatRecord.Version_Field_Size_In_Bytes);
          logger.trace("Calculate offsets, read header version time: {}",
              SystemTime.getInstance().milliseconds() - startTime);

          headerVersion.flip();
          short version = headerVersion.getShort();
          switch (version) {
            case MessageFormatRecord.Message_Header_Version_V1:

              // read the header
              startTime = SystemTime.getInstance().milliseconds();
              ByteBuffer header = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
              headerVersion.clear();
              header.putShort(headerVersion.getShort());
              readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(header)),
                  MessageFormatRecord.Version_Field_Size_In_Bytes,
                  MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize()
                      - MessageFormatRecord.Version_Field_Size_In_Bytes);
              logger.trace("Calculate offsets, read header time: {}",
                  SystemTime.getInstance().milliseconds() - startTime);

              startTime = SystemTime.getInstance().milliseconds();
              header.flip();
              MessageFormatRecord.MessageHeader_Format_V1 headerFormat =
                  new MessageFormatRecord.MessageHeader_Format_V1(header);
              headerFormat.verifyHeader();
              StoreKey storeKey = storeKeyFactory.getStoreKey(
                  new DataInputStream(new MessageReadSetIndexInputStream(readSet, i, header.capacity())));
              if (storeKey.compareTo(readSet.getKeyAt(i)) != 0) {
                throw new MessageFormatException(
                    "Id mismatch between metadata and store - metadataId " + readSet.getKeyAt(i) + " storeId "
                        + storeKey, MessageFormatErrorCodes.Store_Key_Id_MisMatch);
              }
              logger.trace("Calculate offsets, verify header time: {}",
                  SystemTime.getInstance().milliseconds() - startTime);

              startTime = SystemTime.getInstance().milliseconds();
              if (flag == MessageFormatFlags.BlobProperties) {
                int blobPropertiesRecordSize = headerFormat.getUserMetadataRecordRelativeOffset()
                    - headerFormat.getBlobPropertiesRecordRelativeOffset();

                infoList.add(i,
                    new SendInfo(headerFormat.getBlobPropertiesRecordRelativeOffset(), blobPropertiesRecordSize));
                totalSizeToWrite += blobPropertiesRecordSize;
                logger.trace("Calculate offsets, get total size of blob properties time: {}",
                    SystemTime.getInstance().milliseconds() - startTime);
                logger.trace("Sending blob properties for message relativeOffset : {} size : {}",
                    infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
              } else if (flag == MessageFormatFlags.BlobUserMetadata) {
                int userMetadataRecordSize =
                    headerFormat.getBlobRecordRelativeOffset() - headerFormat.getUserMetadataRecordRelativeOffset();

                infoList.add(i,
                    new SendInfo(headerFormat.getUserMetadataRecordRelativeOffset(), userMetadataRecordSize));
                totalSizeToWrite += userMetadataRecordSize;
                logger.trace("Calculate offsets, get total size of user metadata time: {}",
                    SystemTime.getInstance().milliseconds() - startTime);
                logger.trace("Sending user metadata for message relativeOffset : {} size : {}",
                    infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
              } else if (flag == MessageFormatFlags.BlobInfo) {
                int blobPropertiesRecordPlusUserMetadataRecordSize =
                    headerFormat.getBlobRecordRelativeOffset() - headerFormat.getBlobPropertiesRecordRelativeOffset();

                infoList.add(i, new SendInfo(headerFormat.getBlobPropertiesRecordRelativeOffset(),
                    blobPropertiesRecordPlusUserMetadataRecordSize));
                totalSizeToWrite += blobPropertiesRecordPlusUserMetadataRecordSize;
                logger.trace("Calculate offsets, get total size of blob info time: {}",
                    SystemTime.getInstance().milliseconds() - startTime);
                logger.trace("Sending blob info (blob properties + user metadata) for message relativeOffset : {} "
                    + "size : {}", infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
              } else if (flag == MessageFormatFlags.Blob) {
                long blobRecordSize = headerFormat.getMessageSize() - (headerFormat.getBlobRecordRelativeOffset()
                    - headerFormat.getBlobPropertiesRecordRelativeOffset());
                infoList.add(i, new SendInfo(headerFormat.getBlobRecordRelativeOffset(), blobRecordSize));
                totalSizeToWrite += blobRecordSize;
                logger.trace("Calculate offsets, get total size of blob time: {}",
                    SystemTime.getInstance().milliseconds() - startTime);
                logger.trace("Sending data for message relativeOffset : {} size : {}", infoList.get(i).relativeOffset(),
                    infoList.get(i).sizetoSend());
              } else { //just return the header
                int messageHeaderSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize()
                    + MessageFormatRecord.Version_Field_Size_In_Bytes;
                infoList.add(i, new SendInfo(0, messageHeaderSize));
                totalSizeToWrite += messageHeaderSize;
                logger.trace("Calculate offsets, get total size of header time: {}",
                    SystemTime.getInstance().milliseconds() - startTime);
                logger.trace("Sending message header relativeOffset : {} size : {}", infoList.get(i).relativeOffset(),
                    infoList.get(i).sizetoSend());
              }
              break;
            default:
              String message =
                  "Version not known while reading message - version " + version + ", StoreKey " + readSet.getKeyAt(i);
              throw new MessageFormatException(message, MessageFormatErrorCodes.Unknown_Format_Version);
          }
        }
      }
    } catch (IOException e) {
      logger.trace("IOError when calculating offsets");
      throw new MessageFormatException("IOError when calculating offsets ", e, MessageFormatErrorCodes.IO_Error);
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (!isSendComplete()) {
      written = readSet.writeTo(currentWriteIndex, channel,
          infoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
          infoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex);
      logger.trace("writeindex {} relativeOffset {} maxSize {} written {}", currentWriteIndex,
          infoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
          infoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex, written);
      sizeWritten += written;
      sizeWrittenFromCurrentIndex += written;
      logger.trace("size written in this loop : {} size written till now : {}", written, sizeWritten);
      if (sizeWrittenFromCurrentIndex == infoList.get(currentWriteIndex).sizetoSend()) {
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
