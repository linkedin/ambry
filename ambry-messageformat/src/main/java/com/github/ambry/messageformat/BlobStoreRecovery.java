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

import static com.github.ambry.messageformat.MessageFormatRecord.*;


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
        ByteBuffer headerVersion = ByteBuffer.allocate(Version_Field_Size_In_Bytes);
        if (startOffset + Version_Field_Size_In_Bytes > endOffset) {
          throw new IndexOutOfBoundsException("Unable to read version. Reached end of stream");
        }
        read.readInto(headerVersion, startOffset);
        startOffset += headerVersion.capacity();
        headerVersion.flip();
        short version = headerVersion.getShort();
        if (!isValidHeaderVersion(version)) {
          throw new MessageFormatException("Version not known while reading message - " + version,
              MessageFormatErrorCodes.Unknown_Format_Version);
        }
        ByteBuffer header = ByteBuffer.allocate(getHeaderSizeForVersion(version));
        header.putShort(version);
        if (startOffset + (header.capacity() - headerVersion.capacity()) > endOffset) {
          throw new IndexOutOfBoundsException("Unable to read version. Reached end of stream");
        }
        read.readInto(header, startOffset);
        startOffset += header.capacity() - headerVersion.capacity();
        header.flip();
        MessageHeader_Format headerFormat = getMessageHeader(version, header);
        headerFormat.verifyHeader();
        ReadInputStream stream = new ReadInputStream(read, startOffset, endOffset);
        StoreKey key = factory.getStoreKey(new DataInputStream(stream));

        short lifeVersion = 0;
        if (headerFormat.hasLifeVersion()) {
          lifeVersion = headerFormat.getLifeVersion();
        }
        // read the appropriate type of message based on the relative offset that is set
        if (headerFormat.isPutRecord()) {
          // we do not use blob encryption key, user metadata or blob during recovery but we still deserialize
          // them to check for validity
          if (headerFormat.hasEncryptionKeyRecord()) {
            deserializeBlobEncryptionKey(stream);
          }
          BlobProperties properties = deserializeBlobProperties(stream);
          deserializeUserMetadata(stream);
          deserializeBlob(stream);
          MessageInfo info =
              new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(), false, false,
                  false,
                  Utils.addSecondsToEpochTime(properties.getCreationTimeInMs(), properties.getTimeToLiveInSeconds()),
                  null, properties.getAccountId(), properties.getContainerId(), properties.getCreationTimeInMs(),
                  lifeVersion);
          messageRecovered.add(info);
        } else {
          UpdateRecord updateRecord = deserializeUpdateRecord(stream);
          boolean deleted = false, ttlUpdated = false, undeleted = false;
          switch (updateRecord.getType()) {
            case DELETE:
              deleted = true;
              break;
            case TTL_UPDATE:
              ttlUpdated = true;
              break;
            case UNDELETE:
              undeleted = true;
              break;
            default:
              throw new IllegalStateException("Unknown update record type: " + updateRecord.getType());
          }
          MessageInfo info =
              new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(), deleted,
                  ttlUpdated, undeleted, updateRecord.getAccountId(), updateRecord.getContainerId(),
                  updateRecord.getUpdateTimeInMs(), lifeVersion);
          messageRecovered.add(info);
        }
        startOffset = stream.getCurrentPosition();
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
      logger.info("Message Recovered key {} size {} ttl {} deleted {} undelete {}", messageInfo.getStoreKey(),
          messageInfo.getSize(), messageInfo.getExpirationTimeInMs(), messageInfo.isDeleted(),
          messageInfo.isUndeleted());
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
