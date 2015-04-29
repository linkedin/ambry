package com.github.ambry.messageformat;

import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageStoreHardDelete;
import com.github.ambry.store.ReplaceInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.ZeroBytesInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreHardDelete implements MessageStoreHardDelete {
  public Iterator<ReplaceInfo> replacementIterator(MessageReadSet readSet, StoreKeyFactory storeKeyFactory) {
    return new BlobStoreHardDeleteIterator(readSet, storeKeyFactory);
  }
}

class BlobStoreHardDeleteIterator implements Iterator<ReplaceInfo> {
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
  public ReplaceInfo next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return getReplacementInfo(readSetIndex++);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private ReplaceInfo getReplacementInfo(int readSetIndex) {

    /*for the message at readSetIndex, do the following:
      1. Read the whole blob and do a crc check. If the crc check fails, return - this means that the record
         is not retrievable anyway.
      2. Add to a hard delete replacement write set.
      3. Return the replacement info.
     */
    ReplaceInfo replaceInfo = null;

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
              BlobProperties blobProperties = getReplacementBlobPropertiesRecord(readSet, readSetIndex,
                  headerFormat.getBlobPropertiesRecordRelativeOffset(),
                  headerFormat.getUserMetadataRecordRelativeOffset() - headerFormat
                      .getBlobPropertiesRecordRelativeOffset());

              ByteBuffer userMetadata = getReplacementUserMetadataRecord(readSet, readSetIndex,
                  headerFormat.getUserMetadataRecordRelativeOffset(),
                  headerFormat.getBlobRecordRelativeOffset() - headerFormat.getUserMetadataRecordRelativeOffset());

              BlobOutput blobOutput =
                  getDeserializedBlobRecord(readSet, readSetIndex, headerFormat.getBlobRecordRelativeOffset(),
                      headerFormat.getMessageSize() - (headerFormat.getBlobRecordRelativeOffset() - headerFormat
                          .getBlobPropertiesRecordRelativeOffset()));

              MessageFormatInputStream replaceStream =
                  new PutMessageFormatInputStream(storeKey, blobProperties, userMetadata, blobOutput.getStream(),
                      blobOutput.getSize(), MessageFormatRecord.Message_Header_Version_V1);

              replaceInfo = new ReplaceInfo(Channels.newChannel(replaceStream), replaceStream.getSize());
            }
            break;
          default:
            logger.error("Unknown header version " + version + "storeKey " + readSet.getKeyAt(readSetIndex));
        }
    } catch (Exception e) {
      logger.error("Exception, cause: {}", e.getCause());
    }

    return replaceInfo;
  }

  private BlobProperties getReplacementBlobPropertiesRecord(MessageReadSet readSet, int readSetIndex,
      long relativeOffset, long blobPropertiesSize)
      throws MessageFormatException, IOException {

    /* Read the field from the channel */
    ByteBuffer blobProperties = ByteBuffer.allocate((int) blobPropertiesSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobProperties)), relativeOffset,
        blobPropertiesSize);
    blobProperties.flip();

    // deserialize just to verify if it can be read.
    return MessageFormatRecord.deserializeBlobProperties(new ByteBufferInputStream(blobProperties));
  }

  private ByteBuffer getReplacementUserMetadataRecord(MessageReadSet readSet, int readSetIndex, long relativeOffset,
      long userMetadataSize)
      throws MessageFormatException, IOException {

    /* Read the serialized user metadata from the channel */
    ByteBuffer userMetaData = ByteBuffer.allocate((int) userMetadataSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(userMetaData)), relativeOffset,
        userMetadataSize);
    userMetaData.flip();

    userMetaData = MessageFormatRecord.deserializeUserMetadata(new ByteBufferInputStream(userMetaData));
    // Zero out userMetadata
    Arrays.fill(userMetaData.array(), (byte) 0);
    return userMetaData;
  }

  private BlobOutput getDeserializedBlobRecord(MessageReadSet readSet, int readSetIndex, long relativeOffset,
      long blobRecordSize)
      throws MessageFormatException, IOException {

    /* Read the field from the channel */
    ByteBuffer blobRecord = ByteBuffer.allocate((int) blobRecordSize);
    readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobRecord)), relativeOffset,
        blobRecordSize);
    blobRecord.flip();
    // verifies crc among other things by deserializing
    BlobOutput blobOutput = MessageFormatRecord.deserializeBlob(new ByteBufferInputStream(blobRecord));
    return new BlobOutput(blobOutput.getSize(), new ZeroBytesInputStream((int) blobOutput.getSize()));
  }
}
