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

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageStoreRecovery;
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recovers a set of messages from a given start and end offset
 * from the read interface that represents the underlying store
 */
public class BlobStoreRecovery implements MessageStoreRecovery {
  private Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
      throws IOException {
    ArrayList<MessageInfo> messageRecovered = new ArrayList<MessageInfo>();
    try {
      while (startOffset < endOffset) {
        // read message header
        ByteBuffer headerVersion = ByteBuffer.allocate(MessageFormatRecord.Version_Field_Size_In_Bytes);
        if (startOffset + MessageFormatRecord.Version_Field_Size_In_Bytes > endOffset) {
          throw new IndexOutOfBoundsException("Unable to read version. Reached end of stream");
        }
        read.readInto(headerVersion, startOffset);
        startOffset += headerVersion.capacity();
        headerVersion.flip();
        short version = headerVersion.getShort();
        switch (version) {
          case MessageFormatRecord.Message_Header_Version_V1:
            ByteBuffer header = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
            header.putShort(version);
            if (startOffset + (MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize() - headerVersion.capacity())
                > endOffset) {
              throw new IndexOutOfBoundsException("Unable to read version. Reached end of stream");
            }
            read.readInto(header, startOffset);
            startOffset += header.capacity() - headerVersion.capacity();
            header.flip();
            MessageFormatRecord.MessageHeader_Format_V1 headerFormat =
                new MessageFormatRecord.MessageHeader_Format_V1(header);
            headerFormat.verifyHeader();
            ReadInputStream stream = new ReadInputStream(read, startOffset, endOffset);
            StoreKey key = factory.getStoreKey(new DataInputStream(stream));

            // read the appropriate type of message based on the relative offset that is set
            if (headerFormat.getBlobPropertiesRecordRelativeOffset()
                != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
              BlobProperties properties = MessageFormatRecord.deserializeBlobProperties(stream);
              // we do not use the user metadata or blob during recovery but we still deserialize them to check
              // for validity
              MessageFormatRecord.deserializeUserMetadata(stream);
              MessageFormatRecord.deserializeBlob(stream);
              MessageInfo info =
                  new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(),
                      Utils.addSecondsToEpochTime(properties.getCreationTimeInMs(),
                          properties.getTimeToLiveInSeconds()));
              messageRecovered.add(info);
            } else {
              DeleteRecord deleteRecord = MessageFormatRecord.deserializeDeleteRecord(stream);
              MessageInfo info =
                  new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(), true,
                      deleteRecord.getAccountId(), deleteRecord.getContainerId(), deleteRecord.getDeletionTimeInMs());
              messageRecovered.add(info);
            }
            startOffset = stream.getCurrentPosition();
            break;
          default:
            throw new MessageFormatException("Version not known while reading message - " + version,
                MessageFormatErrorCodes.Unknown_Format_Version);
        }
      }
    } catch (MessageFormatException e) {
      // log in case where we were not able to parse a message. we stop recovery at that point and return the
      // messages that have been recovered so far.
      logger.error("Message format exception while recovering messages", e);
    } catch (IndexOutOfBoundsException e) {
      // log in case where were not able to read a complete message. we stop recovery at that point and return
      // the message that have been recovered so far.
      logger.error("Trying to read more than the available bytes");
    }
    for (MessageInfo messageInfo : messageRecovered) {
      logger.info("Message Recovered key {} size {} ttl {} deleted {}", messageInfo.getStoreKey(),
          messageInfo.getSize(), messageInfo.getExpirationTimeInMs(), messageInfo.isDeleted());
    }
    return messageRecovered;
  }
}

class ReadInputStream extends InputStream {

  private final Read readable;
  private long currentPosition;
  private long endPosition;

  ReadInputStream(Read readable, long startPosition, long endPosition) {
    this.readable = readable;
    this.currentPosition = startPosition;
    this.endPosition = endPosition;
  }

  @Override
  public int read() throws IOException {
    if (currentPosition + 1 > endPosition) {
      throw new IndexOutOfBoundsException("Trying to read outside the available read window");
    }
    ByteBuffer buf = ByteBuffer.allocate(1);
    readable.readInto(buf, currentPosition);
    currentPosition += 1;
    buf.flip();
    return buf.get() & 0xFF;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    } else if (currentPosition + (len - off) > endPosition) {
      throw new IndexOutOfBoundsException("Trying to read outside the available read window");
    }

    ByteBuffer buf = ByteBuffer.wrap(b);
    buf.position(off);
    buf.limit(len);
    readable.readInto(buf, currentPosition);
    currentPosition += (buf.position() - off);
    return buf.position() - off;
  }

  public long getCurrentPosition() {
    return currentPosition;
  }
}
