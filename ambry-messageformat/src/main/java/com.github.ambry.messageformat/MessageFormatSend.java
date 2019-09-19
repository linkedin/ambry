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
import com.github.ambry.protocol.GetBlobStoreOption;
import com.github.ambry.router.ByteRange;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.SystemTime;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * A send object for the message format to send data from the underlying store
 * to the network channel
 */

public class MessageFormatSend implements Send {

  private MessageReadSet readSet;
  private List<MessageInfo> readSetInfo;
  private MessageFormatFlags flag;
  private ArrayList<SendInfo> sendInfoList;
  private ArrayList<MessageMetadata> messageMetadataList;
  private long totalSizeToWrite;
  private long sizeWritten;
  private int currentWriteIndex;
  private StoreKeyFactory storeKeyFactory;
  private Map<StoreKey, GetBlobStoreOption> getBlobStoreOptions;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final static int BUFFERED_INPUT_STREAM_BUFFER_SIZE = 256;

  private class SendInfo {
    private long relativeOffset;
    private long sizeToSend;
    private int readSetIdx;
    private ByteRange byteRange;
    private ByteBuffer finalBuffer = null;

    private long sizeWritten = 0;

    public SendInfo(long relativeOffset, long sizeToSend, int readSetIdx) {
      this.relativeOffset = relativeOffset;
      this.sizeToSend = sizeToSend;
      this.readSetIdx = readSetIdx;
    }

    public SendInfo(long relativeOffset, long sizeToSend, int readSetIdx, ByteRange byteRange) {
      this(relativeOffset, sizeToSend, readSetIdx);
      this.byteRange = byteRange;
      ByteBuffer prefetchedData = readSet.getPrefetchedData(readSetIdx);
      try {
        prefetchedData.position((int)relativeOffset);
        prefetchedData.limit((int)(relativeOffset + sizeToSend));
        BlobData blobData = MessageFormatRecord.deserializeBlob(new ByteBufferInputStream(prefetchedData.slice()));
        ByteBuffer buffer = blobData.getStream().getByteBuffer().duplicate();
        buffer.position((int) byteRange.getStartOffset());
        if (buffer.remaining() > (int)byteRange.getEndOffset()) {
          buffer.limit((int) byteRange.getEndOffset());
        }
        ByteBuffer dataBuffer = buffer.slice();
        finalBuffer = ByteBuffer.allocate(
            (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize((int) byteRange.getRangeSize()));

        MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(finalBuffer, byteRange.getRangeSize(), blobData.getBlobType());
        finalBuffer.put(dataBuffer.array());
        Crc32 crc = new Crc32();
        crc.update(finalBuffer.array(), 0, finalBuffer.position());
        finalBuffer.putLong(crc.getValue());
        finalBuffer.flip();

        this.relativeOffset = 0;
        this.sizeToSend = finalBuffer.remaining();
        logger.info("Has to send out " + this.sizeToSend + " byte");
      } catch (IOException e) {
        logger.info("IOException", e);
      } catch (MessageFormatException e) {
        logger.info("MessageFormatException", e);
      }
    }

    public boolean isSendComplete() {
      return sizeToSend == sizeWritten;
    }

    public long writeTo(WritableByteChannel channel) throws IOException {
      if (finalBuffer == null) {
        long written = readSet.writeTo(readSetIdx, channel, relativeOffset + sizeWritten, sizeToSend - sizeWritten);
        sizeWritten += written;
        logger.trace("writeindex {} relativeOffset {} maxSize {} written {}", readSetIdx, relativeOffset + sizeWritten,
            sizeToSend - sizeWritten, written);
        return written;
      } else {
        finalBuffer.position((int) sizeWritten);
        long written = channel.write(finalBuffer);
        sizeWritten += written;
        logger.info("SizeWritten = " + sizeWritten);
        return written;
      }
    }

    public long sizetoSend() {
      return sizeToSend;
    }

    public long relativeOffset() {
      return relativeOffset;
    }
  }

  public MessageFormatSend(StoreInfo info, MessageFormatFlags flag, MessageFormatMetrics metrics,
      StoreKeyFactory storeKeyFactory, boolean enableDataPrefetch,
      Map<StoreKey, GetBlobStoreOption> getBlobStoreOptions) throws IOException, MessageFormatException {
    this.readSet = info.getMessageReadSet();
    this.readSetInfo = info.getMessageReadSetInfo();
    this.flag = flag;
    this.storeKeyFactory = storeKeyFactory;
    this.getBlobStoreOptions = getBlobStoreOptions;
    totalSizeToWrite = 0;
    long startTime = SystemTime.getInstance().milliseconds();
    calculateOffsets(enableDataPrefetch);
    metrics.calculateOffsetMessageFormatSendTime.update(SystemTime.getInstance().milliseconds() - startTime);
    sizeWritten = 0;
    currentWriteIndex = 0;
  }

  private MessageHeader_Format getMessagesHeaderFormat(int idx) throws IOException, MessageFormatException {
    long startTime = SystemTime.getInstance().milliseconds();
    BufferedInputStream bufferedInputStream =
        new BufferedInputStream(new MessageReadSetIndexInputStream(readSet, idx, 0),
            BUFFERED_INPUT_STREAM_BUFFER_SIZE);
    // read and verify header version
    byte[] headerVersionBytes = new byte[Version_Field_Size_In_Bytes];
    bufferedInputStream.read(headerVersionBytes, 0, Version_Field_Size_In_Bytes);
    short version = ByteBuffer.wrap(headerVersionBytes).getShort();
    if (!isValidHeaderVersion(version)) {
      throw new MessageFormatException(
          "Version not known while reading message - version " + version + ", StoreKey " + readSet.getKeyAt(idx),
          MessageFormatErrorCodes.Unknown_Format_Version);
    }
    logger.trace("Calculate offsets, read and verify header version time: {}",
        SystemTime.getInstance().milliseconds() - startTime);

    // read and verify header
    startTime = SystemTime.getInstance().milliseconds();
    byte[] headerBytes = new byte[getHeaderSizeForVersion(version)];
    bufferedInputStream.read(headerBytes, Version_Field_Size_In_Bytes,
        headerBytes.length - Version_Field_Size_In_Bytes);

    ByteBuffer header = ByteBuffer.wrap(headerBytes);
    header.putShort(version);
    header.rewind();
    MessageHeader_Format headerFormat = getMessageHeader(version, header);
    headerFormat.verifyHeader();
    logger.trace("Calculate offsets, read and verify header time: {}",
        SystemTime.getInstance().milliseconds() - startTime);

    // read and verify storeKey
    startTime = SystemTime.getInstance().milliseconds();
    StoreKey storeKey = storeKeyFactory.getStoreKey(new DataInputStream(bufferedInputStream));
    if (storeKey.compareTo(readSet.getKeyAt(idx)) != 0) {
      throw new MessageFormatException(
          "Id mismatch between metadata and store - metadataId " + readSet.getKeyAt(idx) + " storeId " + storeKey,
          MessageFormatErrorCodes.Store_Key_Id_MisMatch);
    }
    logger.trace("Calculate offsets, read and verify storeKey time: {}",
        SystemTime.getInstance().milliseconds() - startTime);
    return headerFormat;
  }

  private boolean isDataBlob(int i, long relativeOffset, long sizeOfBlobRecord) {
    ByteBuffer prefetchedData = readSet.getPrefetchedData(i);
    try {
      prefetchedData.position((int) relativeOffset);
      prefetchedData.limit((int) (relativeOffset + sizeOfBlobRecord));
      BlobData blobData = MessageFormatRecord.deserializeBlob(new ByteBufferInputStream(prefetchedData.slice()));
      return blobData.getBlobType() == BlobType.DataBlob;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Calculates the offsets from the MessageReadSet that needs to be sent over the network
   * based on the type of data requested as indicated by the flags
   * @param enableDataPrefetch do data prefetch if this is true.
   */
  private void calculateOffsets(boolean enableDataPrefetch) throws IOException, MessageFormatException {
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
          GetBlobStoreOption getBlobStoreOption = getBlobStoreOptions.getOrDefault(readSet.getKeyAt(i), null);
          if (getBlobStoreOption == null || !enableDataPrefetch) {
            // just copy over the total size and use relative offset to be 0
            // We do not have to check any version in this case as we dont
            // have to read any data to deserialize anything.
            sendInfoList.add(new SendInfo(0, readSet.sizeInBytes(i), i));
            messageMetadataList.add(i, null);
            totalSizeToWrite += readSet.sizeInBytes(i);
          } else {
            // If we are here, then data prefetch is enabled.
            ByteRange range = getBlobStoreOption.getByteRange().toResolvedByteRange(readSet.sizeInBytes(i));
            MessageHeader_Format headerFormat = getMessagesHeaderFormat(i);
            // If the blob is encrypted, we have to send back the whole blob data for decryption.
            if (headerFormat.hasEncryptionKeyRecord()) {
              sendInfoList.add(new SendInfo(0, readSet.sizeInBytes(i), i));
              messageMetadataList.add(i, null);
              totalSizeToWrite += readSet.sizeInBytes(i);
            } else {
              readSet.doPrefetch(i, 0, readSet.sizeInBytes(i));
              if (isDataBlob(i, headerFormat.getBlobRecordRelativeOffset(), headerFormat.getBlobRecordSize())) {
                // Since we still have to send back the blob info and blob data, we now have to use two sendInfo
                SendInfo sendInfo = new SendInfo(0, headerFormat.getBlobRecordRelativeOffset(), i);
                sendInfoList.add(sendInfo);
                totalSizeToWrite += sendInfo.sizetoSend();
                messageMetadataList.add(i, null);

                readSetInfo.get(i).setByteRangeResponse();
                sendInfo = new SendInfo(headerFormat.getBlobRecordRelativeOffset(), headerFormat.getBlobRecordSize(), i,
                    range);
                totalSizeToWrite += sendInfo.sizetoSend();
                sendInfoList.add(sendInfo);
              } else {
                sendInfoList.add(new SendInfo(0, readSet.sizeInBytes(i), i));
                messageMetadataList.add(i, null);
                totalSizeToWrite += readSet.sizeInBytes(i);
              }
            }
          }
          if (enableDataPrefetch) {
            readSet.doPrefetch(i, 0, readSet.sizeInBytes(i));
          }
        } else {
          MessageHeader_Format headerFormat = getMessagesHeaderFormat(i);
          long startTime = SystemTime.getInstance().milliseconds();
          SendInfo sendInfo = null;
          if (flag == MessageFormatFlags.BlobProperties) {
            sendInfo = new SendInfo(headerFormat.getBlobPropertiesRecordRelativeOffset(),
                headerFormat.getBlobPropertiesRecordSize(), i);
            sendInfoList.add(sendInfo);
            messageMetadataList.add(null);
            if (enableDataPrefetch) {
              readSet.doPrefetch(i, headerFormat.getBlobPropertiesRecordRelativeOffset(),
                  headerFormat.getBlobPropertiesRecordSize());
            }
            totalSizeToWrite += headerFormat.getBlobPropertiesRecordSize();
            logger.trace("Calculate offsets, get total size of blob properties time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace("Sending blob properties for message relativeOffset : {} size : {}",
                sendInfo.relativeOffset(), sendInfo.sizetoSend());
          } else if (flag == MessageFormatFlags.BlobUserMetadata) {
            messageMetadataList.add(headerFormat.hasEncryptionKeyRecord() ? new MessageMetadata(
                extractEncryptionKey(i, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                    headerFormat.getBlobEncryptionKeyRecordSize())) : null);
            sendInfo = new SendInfo(headerFormat.getUserMetadataRecordRelativeOffset(),
                headerFormat.getUserMetadataRecordSize(), i);
            sendInfoList.add(sendInfo);
            if (enableDataPrefetch) {
              readSet.doPrefetch(i, headerFormat.getUserMetadataRecordRelativeOffset(),
                  headerFormat.getUserMetadataRecordSize());
            }
            totalSizeToWrite += headerFormat.getUserMetadataRecordSize();
            logger.trace("Calculate offsets, get total size of user metadata time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace("Sending user metadata for message relativeOffset : {} size : {}",
                sendInfo.relativeOffset(), sendInfo.sizetoSend());
          } else if (flag == MessageFormatFlags.BlobInfo) {
            messageMetadataList.add(headerFormat.hasEncryptionKeyRecord() ? new MessageMetadata(
                extractEncryptionKey(i, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                    headerFormat.getBlobEncryptionKeyRecordSize())) : null);
            sendInfo = new SendInfo(headerFormat.getBlobPropertiesRecordRelativeOffset(),
                headerFormat.getBlobPropertiesRecordSize() + headerFormat.getUserMetadataRecordSize(), i);
            sendInfoList.add(sendInfo);
            if (enableDataPrefetch) {
              readSet.doPrefetch(i, headerFormat.getBlobPropertiesRecordRelativeOffset(),
                  headerFormat.getBlobPropertiesRecordSize() + headerFormat.getUserMetadataRecordSize());
            }
            totalSizeToWrite += headerFormat.getBlobPropertiesRecordSize() + headerFormat.getUserMetadataRecordSize();
            logger.trace("Calculate offsets, get total size of blob info time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace(
                "Sending blob info (blob properties + user metadata) for message relativeOffset : {} " + "size : {}",
                sendInfo.relativeOffset(), sendInfo.sizetoSend());
          } else if (flag == MessageFormatFlags.Blob) {
            boolean hasEncriyptionKey = headerFormat.hasEncryptionKeyRecord();
            messageMetadataList.add(hasEncriyptionKey ? new MessageMetadata(
                extractEncryptionKey(i, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                    headerFormat.getBlobEncryptionKeyRecordSize())) : null);
            GetBlobStoreOption getBlobStoreOption = getBlobStoreOptions.getOrDefault(readSet.getKeyAt(i), null);
            // Send back the entire blob data when this blob is encrypted.
            if (enableDataPrefetch) {
              readSet.doPrefetch(i, headerFormat.getBlobRecordRelativeOffset(), headerFormat.getBlobRecordSize());
            }
            if (!hasEncriyptionKey && getBlobStoreOption != null && getBlobStoreOption.getByteRange() != null && enableDataPrefetch) {
              readSetInfo.get(i).setByteRangeResponse();
              ByteRange range = getBlobStoreOption.getByteRange().toResolvedByteRange(headerFormat.getBlobRecordSize());
              sendInfo = new SendInfo(headerFormat.getBlobRecordRelativeOffset(), headerFormat.getBlobRecordSize(), i, range);
            } else {
              sendInfo = new SendInfo(headerFormat.getBlobRecordRelativeOffset(), headerFormat.getBlobRecordSize(), i);
            }
            sendInfoList.add(sendInfo);
            totalSizeToWrite += sendInfo.sizetoSend();
            logger.trace("Calculate offsets, get total size of blob time: {}",
                SystemTime.getInstance().milliseconds() - startTime);
            logger.trace("Sending data for message relativeOffset : {} size : {}", sendInfo.relativeOffset(),
                sendInfo.sizetoSend());
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
      SendInfo sendInfo = sendInfoList.get(currentWriteIndex);
      if (sendInfo.isSendComplete()) {
        currentWriteIndex ++;
        sendInfo = sendInfoList.get(currentWriteIndex);
      }
      written = sendInfo.writeTo(channel);
      sizeWritten += written;
      logger.trace("size written in this loop : {} size written till now : {}", written, sizeWritten);
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
    if (currentOffset >= messageReadSet.sizeInBytes(indexToRead)) {
      return -1;
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
    if (len == 0) {
      return 0;
    }
    if (currentOffset >= messageReadSet.sizeInBytes(indexToRead)) {
      return -1;
    }
    ByteBuffer buf = ByteBuffer.wrap(b);
    buf.position(off);
    ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(buf);
    long sizeToRead = Math.min(len, messageReadSet.sizeInBytes(indexToRead) - currentOffset);
    long bytesWritten =
        messageReadSet.writeTo(indexToRead, Channels.newChannel(bufferStream), currentOffset, sizeToRead);
    currentOffset += bytesWritten;
    return (int) bytesWritten;
  }
}
