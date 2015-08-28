package com.github.ambry.messageformat;

import com.github.ambry.store.HardDeleteInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageStoreHardDelete;
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class takes a read set for blobs that are to be hard deleted and provides corresponding
 * replacement messages, that can then be written back by the caller to hard delete those blobs.
 */
public class BlobStoreHardDelete implements MessageStoreHardDelete {
  @Override
  public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory storeKeyFactory,
      List<byte[]> recoveryInfoList)
      throws IOException {
    return new BlobStoreHardDeleteIterator(readSet, storeKeyFactory, recoveryInfoList);
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
            BlobProperties properties = MessageFormatRecord.deserializeBlobProperties(stream).getBlobProperties();
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
  private final MessageReadSet readSet;
  private final StoreKeyFactory storeKeyFactory;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private int readSetIndex = 0;
  private Map<StoreKey, MessageMetadata> recoveryInfoMap;

  BlobStoreHardDeleteIterator(MessageReadSet readSet, StoreKeyFactory storeKeyFactory, List<byte[]> recoveryInfoList)
      throws IOException {
    this.readSet = readSet;
    this.storeKeyFactory = storeKeyFactory;
    this.recoveryInfoMap = new HashMap<StoreKey, MessageMetadata>();
    if (recoveryInfoList != null) {
      for (byte[] recoveryInfo : recoveryInfoList) {
        MessageMetadata messageMetadata = new MessageMetadata(recoveryInfo, storeKeyFactory);
        recoveryInfoMap.put(messageMetadata.getStoreKey(), messageMetadata);
      }
    }
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
      ByteBuffer headerVersionBuf = ByteBuffer.allocate(MessageFormatRecord.Version_Field_Size_In_Bytes);
      readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(headerVersionBuf)), 0,
          MessageFormatRecord.Version_Field_Size_In_Bytes);
      headerVersionBuf.flip();
      short headerVersion = headerVersionBuf.getShort();
      switch (headerVersion) {
        case MessageFormatRecord.Message_Header_Version_V1:
          /* Read the rest of the header */
          ByteBuffer header = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
          header.putShort(headerVersion);
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

            MessageMetadata messageMetadata = recoveryInfoMap.get(storeKey);

            int userMetadataRelativeOffset = headerFormat.getUserMetadataRecordRelativeOffset();
            short userMetadataVersion;
            int userMetadataSize;
            short blobRecordVersion;
            long blobStreamSize;
            DeserializedUserMetadata userMetadataInfo;
            DeserializedBlob blobRecordInfo;

            if (messageMetadata == null) {
              userMetadataInfo =
                  getUserMetadataInfo(readSet, readSetIndex, headerFormat.getUserMetadataRecordRelativeOffset(),
                      headerFormat.getBlobRecordRelativeOffset() - headerFormat.getUserMetadataRecordRelativeOffset());
              userMetadataSize = userMetadataInfo.getUserMetadata().capacity();
              userMetadataVersion = userMetadataInfo.getVersion();

              blobRecordInfo = getBlobRecordInfo(readSet, readSetIndex, headerFormat.getBlobRecordRelativeOffset(),
                  headerFormat.getMessageSize() - (headerFormat.getBlobRecordRelativeOffset() - headerFormat
                      .getBlobPropertiesRecordRelativeOffset()));
              blobStreamSize = blobRecordInfo.getBlobOutput().getSize();
              blobRecordVersion = blobRecordInfo.getVersion();
              messageMetadata =
                  new MessageMetadata(headerVersion, userMetadataVersion, userMetadataSize, blobRecordVersion,
                      blobStreamSize, storeKey);
            } else {
              logger.trace("Skipping crc check for user metadata and blob stream fields for key {}", storeKey);
              userMetadataVersion = messageMetadata.userMetadataVersion;
              blobRecordVersion = messageMetadata.blobRecordVersion;
              userMetadataSize = messageMetadata.userMetadataSize;
              blobStreamSize = messageMetadata.blobStreamSize;
            }

            HardDeleteMessageFormatInputStream hardDeleteStream =
                new HardDeleteMessageFormatInputStream(userMetadataRelativeOffset, userMetadataVersion,
                    userMetadataSize, blobRecordVersion, blobStreamSize);

            hardDeleteInfo = new HardDeleteInfo(Channels.newChannel(hardDeleteStream), hardDeleteStream.getSize(),
                hardDeleteStream.getHardDeleteStreamRelativeOffset(), messageMetadata.toBytes());
          }
          break;
        default:
          throw new MessageFormatException(
              "Unknown header version during hard delete " + headerVersion + " storeKey " + readSet
                  .getKeyAt(readSetIndex), MessageFormatErrorCodes.Unknown_Format_Version);
      }
    } catch (Exception e) {
      logger.error("Exception when reading blob: ", e);
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

    return MessageFormatRecord.deserializeBlobProperties(new ByteBufferInputStream(blobProperties)).getBlobProperties();
  }

  private DeserializedUserMetadata getUserMetadataInfo(MessageReadSet readSet, int readSetIndex, int relativeOffset,
      int userMetadataSize)
      throws MessageFormatException, IOException {

    /* Read the serialized user metadata from the channel */
    ByteBuffer userMetaData = ByteBuffer.allocate(userMetadataSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(userMetaData)), relativeOffset,
        userMetadataSize);
    userMetaData.flip();
    return MessageFormatRecord.deserializeUserMetadata(new ByteBufferInputStream(userMetaData));
  }

  private DeserializedBlob getBlobRecordInfo(MessageReadSet readSet, int readSetIndex, int relativeOffset,
      long blobRecordSize)
      throws MessageFormatException, IOException {

    /* Read the field from the channel */
    ByteBuffer blobRecord = ByteBuffer.allocate((int) blobRecordSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobRecord)), relativeOffset,
        blobRecordSize);
    blobRecord.flip();
    return MessageFormatRecord.deserializeBlob(new ByteBufferInputStream(blobRecord));
  }
}

class MessageMetadata {
  short headerVersion;
  short userMetadataVersion;
  int userMetadataSize;
  short blobRecordVersion;
  long blobStreamSize;
  StoreKey storeKey;

  MessageMetadata(short headerVersion, short userMetadataVersion, int userMetadataSize, short blobRecordVersion,
      long blobStreamSize, StoreKey storeKey)
      throws IOException {
    if (!MessageFormatRecord.isValidHeaderVersion(headerVersion) ||
        !MessageFormatRecord.isValidUserMetadataVersion(userMetadataVersion) ||
        !MessageFormatRecord.isValidBlobRecordVersion(blobRecordVersion)) {
      throw new IOException(
          "Unknown version during hard delete, headerVersion: " + headerVersion + " userMetadataVersion: "
              + userMetadataVersion + " blobRecordVersion: " + blobRecordVersion);
    }
    this.headerVersion = headerVersion;
    this.userMetadataVersion = userMetadataVersion;
    this.userMetadataSize = userMetadataSize;
    this.blobRecordVersion = blobRecordVersion;
    this.blobStreamSize = blobStreamSize;
    this.storeKey = storeKey;
  }

  MessageMetadata(byte[] messageMetadataBytes, StoreKeyFactory factory)
      throws IOException {
    DataInputStream stream = new DataInputStream(new ByteArrayInputStream(messageMetadataBytes));
    headerVersion = stream.readShort();

    switch (headerVersion) {
      case MessageFormatRecord.Message_Header_Version_V1:
        userMetadataVersion = stream.readShort();
        if (!MessageFormatRecord.isValidUserMetadataVersion(userMetadataVersion)) {
          throw new IOException(
              "Unknown user metadata version encountered while reading recovery metadata during hard delete "
                  + userMetadataVersion);
        }
        if (userMetadataVersion == MessageFormatRecord.UserMetadata_Version_V1) {
          userMetadataSize = stream.readInt();
        }

        blobRecordVersion = stream.readShort();
        if (!MessageFormatRecord.isValidBlobRecordVersion(blobRecordVersion)) {
          throw new IOException(
              "Unknown blob record version encountered while reading recovery metadata during hard delete "
                  + blobRecordVersion);
        }
        if (blobRecordVersion == MessageFormatRecord.Blob_Version_V1) {
          blobStreamSize = stream.readLong();
        }
        break;
      default:
        throw new IOException(
            "Unknown header version encountered while reading recovery metadata during hard delete " + headerVersion);
    }
    storeKey = factory.getStoreKey(stream);
  }

  StoreKey getStoreKey() {
    return storeKey;
  }

  byte[] toBytes()
      throws MessageFormatException {
    byte[] bytes = new byte[MessageFormatRecord.Version_Field_Size_In_Bytes +
        MessageFormatRecord.Version_Field_Size_In_Bytes +
        MessageFormatRecord.getUserMetadataSizeFieldInBytes(userMetadataVersion) +
        MessageFormatRecord.Version_Field_Size_In_Bytes +
        MessageFormatRecord.getBlobSizeFieldInBytes(blobRecordVersion) +
        storeKey.sizeInBytes()];

    ByteBuffer bufWrap = ByteBuffer.wrap(bytes);
    bufWrap.putShort(headerVersion);
    bufWrap.putShort(userMetadataVersion);
    bufWrap.putInt(userMetadataSize);
    bufWrap.putShort(blobRecordVersion);
    bufWrap.putLong(blobStreamSize);
    bufWrap.put(storeKey.toBytes());
    return bytes;
  }
}
