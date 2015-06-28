package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageStoreHardDelete;
import com.github.ambry.store.HardDeleteInfo;
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class takes a read set for blobs that are to be hard deleted and provides corresponding
 * replacement messages, that can then be written back by the caller to hard delete those blobs.
 */
public class BlobStoreHardDelete implements MessageStoreHardDelete {

  @Override
  public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory storeKeyFactory) {
    return new BlobStoreHardDeleteIterator(readSet, storeKeyFactory);
  }

  @Override
  public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory storeKeyFactory)
      throws IOException {
    try {
      // read message header
      ByteBuffer headerVersion = ByteBuffer.allocate(MessageFormatRecord.Version_Field_Size_In_Bytes);
      read.readInto(headerVersion, offset);
      offset += headerVersion.capacity();
      headerVersion.flip();
      short version = headerVersion.getShort();
      switch (version) {
        case MessageFormatRecord.Message_Header_Version_V1:
          ByteBuffer header = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
          header.putShort(version);
          read.readInto(header, offset);
          offset += header.capacity() - headerVersion.capacity();
          header.flip();
          MessageFormatRecord.MessageHeader_Format_V1 headerFormat =
              new MessageFormatRecord.MessageHeader_Format_V1(header);
          headerFormat.verifyHeader();
          long endOffset = headerFormat.getBlobPropertiesRecordRelativeOffset()
              != MessageFormatRecord.Message_Header_Invalid_Relative_Offset ? offset + headerFormat
              .getBlobPropertiesRecordRelativeOffset() + headerFormat.getMessageSize()
              : offset + headerFormat.getDeleteRecordRelativeOffset() + headerFormat.getMessageSize();

          ReadInputStream stream = new ReadInputStream(read, offset, endOffset);
          StoreKey key = storeKeyFactory.getStoreKey(new DataInputStream(stream));

          // read the appropriate type of message based on the relative offset that is set
          if (headerFormat.getBlobPropertiesRecordRelativeOffset()
              != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
            BlobProperties properties = MessageFormatRecord.deserializeBlobProperties(stream);
            return new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(),
                Utils.addSecondsToEpochTime(properties.getCreationTimeInMs(), properties.getTimeToLiveInSeconds()));
          } else {
            boolean deleteFlag = MessageFormatRecord.deserializeDeleteRecord(stream);
            return new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(),
                deleteFlag);
          }
        default:
          throw new MessageFormatException("Version not known while reading message - " + version,
              MessageFormatErrorCodes.Unknown_Format_Version);
      }
    } catch (MessageFormatException e) {
      // log in case where we were not able to parse a message.
      throw new IOException("Message format exception while parsing messages");
    } catch (IndexOutOfBoundsException e) {
      // log in case where were not able to read a complete message.
      throw new IOException("Trying to read more than the available bytes");
    }
  }
}

class BlobStoreHardDeleteIterator implements Iterator<HardDeleteInfo> {
  private Logger logger = LoggerFactory.getLogger(getClass());
  private int readSetIndex = 0;
  private final MessageReadSet readSet;
  private final StoreKeyFactory storeKeyFactory;

  public BlobStoreHardDeleteIterator(MessageReadSet readSet, StoreKeyFactory storeKeyFactory) {
    this.readSet = readSet;
    this.storeKeyFactory = storeKeyFactory;
  }

  @Override
  public boolean hasNext() {
    return readSetIndex != readSet.count();
  }

  @Override
  public HardDeleteInfo next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return getHardDeleteInfo(readSetIndex++);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * For the message at readSetIndex, does the following:
   1. Reads the whole blob and does a crc check. If the crc check fails, returns null - this means that the record
   is not retrievable anyway.
   2. Adds to a hard delete replacement write set.
   3. Returns the hard delete info.
   */
  private HardDeleteInfo getHardDeleteInfo(int readSetIndex) {

    HardDeleteInfo hardDeleteInfo = null;

    try {
      /* Read the version field in the header */
      ByteBuffer headerVersion = ByteBuffer.allocate(MessageFormatRecord.Version_Field_Size_In_Bytes);
      readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(headerVersion)), 0,
          MessageFormatRecord.Version_Field_Size_In_Bytes);
      headerVersion.flip();
      short version = headerVersion.getShort();
      switch (version) {
        case MessageFormatRecord.Message_Header_Version_V1:
          /* Read the rest of the header */
          ByteBuffer header = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
          header.putShort(version);
          readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(header)),
              MessageFormatRecord.Version_Field_Size_In_Bytes,
              MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize()
                  - MessageFormatRecord.Version_Field_Size_In_Bytes);
          header.flip();
          MessageFormatRecord.MessageHeader_Format_V1 headerFormat =
              new MessageFormatRecord.MessageHeader_Format_V1(header);
          headerFormat.verifyHeader();
          StoreKey storeKey = storeKeyFactory.getStoreKey(
              new DataInputStream(new MessageReadSetIndexInputStream(readSet, readSetIndex, header.capacity())));
          if (storeKey.compareTo(readSet.getKeyAt(readSetIndex)) != 0) {
            throw new MessageFormatException(
                "Id mismatch between metadata and store - metadataId " + readSet.getKeyAt(readSetIndex) + " storeId "
                    + storeKey, MessageFormatErrorCodes.Store_Key_Id_MisMatch);
          }

          if (headerFormat.getBlobPropertiesRecordRelativeOffset()
              == MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
            throw new MessageFormatException("Cleanup operation for a delete record is unsupported",
                MessageFormatErrorCodes.IO_Error);
          } else {
            BlobProperties blobProperties =
                getBlobPropertiesRecord(readSet, readSetIndex, headerFormat.getBlobPropertiesRecordRelativeOffset(),
                    headerFormat.getUserMetadataRecordRelativeOffset() - headerFormat
                        .getBlobPropertiesRecordRelativeOffset());

            long userMetadataSize =
                getUserMetadataRecordSize(readSet, readSetIndex, headerFormat.getUserMetadataRecordRelativeOffset(),
                    headerFormat.getBlobRecordRelativeOffset() - headerFormat.getUserMetadataRecordRelativeOffset());

            long blobStreamSize = getBlobRecordSize(readSet, readSetIndex, headerFormat.getBlobRecordRelativeOffset(),
                headerFormat.getMessageSize() - (headerFormat.getBlobRecordRelativeOffset() - headerFormat
                    .getBlobPropertiesRecordRelativeOffset()));

            MessageFormatInputStream replaceStream =
                new HardDeleteMessageFormatInputStream(storeKey, blobProperties, (int) userMetadataSize,
                    (int) blobStreamSize);

            hardDeleteInfo = new HardDeleteInfo(Channels.newChannel(replaceStream), replaceStream.getSize());
          }
          break;
        default:
          throw new IllegalStateException(
              "Unknown header version during hard delete" + version + "storeKey " + readSet.getKeyAt(readSetIndex));
      }
    } catch (Exception e) {
      if (e instanceof ClosedChannelException) {
        logger.info("Received closed channel exception during hard delete");
      } else {
        logger.error("Exception when reading blob: ", e);
      }
    }
    return hardDeleteInfo;
  }

  private BlobProperties getBlobPropertiesRecord(MessageReadSet readSet, int readSetIndex, long relativeOffset,
      long blobPropertiesSize)
      throws MessageFormatException, IOException {

    /* Read the field from the channel */
    ByteBuffer blobProperties = ByteBuffer.allocate((int) blobPropertiesSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobProperties)), relativeOffset,
        blobPropertiesSize);
    blobProperties.flip();

    return MessageFormatRecord.deserializeBlobProperties(new ByteBufferInputStream(blobProperties));
  }

  private long getUserMetadataRecordSize(MessageReadSet readSet, int readSetIndex, long relativeOffset,
      long userMetadataSize)
      throws MessageFormatException, IOException {

    /* Read the serialized user metadata from the channel */
    ByteBuffer userMetaData = ByteBuffer.allocate((int) userMetadataSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(userMetaData)), relativeOffset,
        userMetadataSize);
    userMetaData.flip();
    ByteBuffer deserializedUserMetadata =
        MessageFormatRecord.deserializeUserMetadata(new ByteBufferInputStream(userMetaData));
    return deserializedUserMetadata.capacity();
  }

  private long getBlobRecordSize(MessageReadSet readSet, int readSetIndex, long relativeOffset, long blobRecordSize)
      throws MessageFormatException, IOException {

    /* Read the field from the channel */
    ByteBuffer blobRecord = ByteBuffer.allocate((int) blobRecordSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobRecord)), relativeOffset,
        blobRecordSize);
    blobRecord.flip();
    BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(new ByteBufferInputStream(blobRecord));
    return blobOutput.getSize();
  }
}
