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

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;


/**
 * Represents the message format of the individual records that are used to write a message to the store.
 * This class provides the serialization and deserialization methods for the individual records.
 * The MessageFormatInputStream classes defines the complete message using these individual records.
 */
public class MessageFormatRecord {

  // Common info for all formats
  public static final int Version_Field_Size_In_Bytes = 2;
  public static final int Crc_Size = 8;
  public static final short Message_Header_Version_V1 = 1;
  public static final short Message_Header_Version_V2 = 2;
  public static final short BlobProperties_Version_V1 = 1;
  public static final short Delete_Version_V1 = 1;
  public static final short Delete_Version_V2 = 2;
  public static final short Blob_Encryption_Key_V1 = 1;
  public static final short UserMetadata_Version_V1 = 1;
  public static final short Blob_Version_V1 = 1;
  public static final short Blob_Version_V2 = 2;
  public static final short Metadata_Content_Version_V2 = 2;
  public static final int Message_Header_Invalid_Relative_Offset = -1;

  static short headerVersionToUse = Message_Header_Version_V2;

  static boolean isValidHeaderVersion(short headerVersion) {
    switch (headerVersion) {
      case Message_Header_Version_V1:
      case Message_Header_Version_V2:
        return true;
      default:
        return false;
    }
  }

  /**
   * Get the size of the header for the given header version.
   * @param headerVersion the version of the header for which the size is to be determined.
   * @return the size of the header for the given header version.
   * @throws MessageFormatException if the given header version is invalid.
   */
  static int getHeaderSizeForVersion(short headerVersion) throws MessageFormatException {
    switch (headerVersion) {
      case Message_Header_Version_V1:
        return MessageHeader_Format_V1.getHeaderSize();
      case Message_Header_Version_V2:
        return MessageHeader_Format_V2.getHeaderSize();
      default:
        throw new MessageFormatException("Unknown header version: " + headerVersion,
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  /**
   * Format a header for the given version using the given content.
   * @param headerVersion the version of the header.
   * @param input the input content that is to be formatted in the appropriate header version.
   * @return a formatted header for the given version.
   * @throws MessageFormatException if the given header version is invalid.
   */
  static MessageHeader_Format getMessageHeader(short headerVersion, ByteBuffer input) throws MessageFormatException {
    switch (headerVersion) {
      case Message_Header_Version_V1:
        return new MessageHeader_Format_V1(input);
      case Message_Header_Version_V2:
        return new MessageHeader_Format_V2(input);
      default:
        throw new MessageFormatException("Unknown header version: " + headerVersion,
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  // Deserialization methods for individual records
  public static BlobProperties deserializeBlobProperties(InputStream stream)
      throws IOException, MessageFormatException {
    return deserializeAndGetBlobPropertiesWithVersion(stream).getBlobProperties();
  }

  static DeserializedBlobProperties deserializeAndGetBlobPropertiesWithVersion(InputStream stream)
      throws IOException, MessageFormatException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case BlobProperties_Version_V1:
        return new DeserializedBlobProperties(BlobProperties_Version_V1,
            BlobProperties_Format_V1.deserializeBlobPropertiesRecord(crcStream));
      default:
        throw new MessageFormatException("blob property version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  public static DeleteRecord deserializeDeleteRecord(InputStream stream) throws IOException, MessageFormatException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case Delete_Version_V1:
        return Delete_Format_V1.deserializeDeleteRecord(crcStream);
      case Delete_Version_V2:
        return Delete_Format_V2.deserializeDeleteRecord(crcStream);
      default:
        throw new MessageFormatException("delete record version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  public static ByteBuffer deserializeBlobEncryptionKey(InputStream stream) throws IOException, MessageFormatException {
    return deserializeAndGetBlobEncryptionKeyWithVersion(stream).getEncryptionKey();
  }

  private static DeserializedBlobEncryptionKey deserializeAndGetBlobEncryptionKeyWithVersion(InputStream stream)
      throws IOException, MessageFormatException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case Blob_Encryption_Key_V1:
        return new DeserializedBlobEncryptionKey(Blob_Encryption_Key_V1,
            BlobEncryptionKey_Format_V1.deserializeBlobEncryptionKeyRecord(crcStream));
      default:
        throw new MessageFormatException("blob encryption key record version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  public static ByteBuffer deserializeUserMetadata(InputStream stream) throws IOException, MessageFormatException {
    return deserializeAndGetUserMetadataWithVersion(stream).getUserMetadata();
  }

  static DeserializedUserMetadata deserializeAndGetUserMetadataWithVersion(InputStream stream)
      throws IOException, MessageFormatException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case UserMetadata_Version_V1:
        return new DeserializedUserMetadata(UserMetadata_Version_V1,
            UserMetadata_Format_V1.deserializeUserMetadataRecord(crcStream));
      default:
        throw new MessageFormatException("metadata version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  static boolean isValidUserMetadataVersion(short userMetadataVersion) {
    switch (userMetadataVersion) {
      case UserMetadata_Version_V1:
        return true;
      default:
        return false;
    }
  }

  public static BlobData deserializeBlob(InputStream stream) throws IOException, MessageFormatException {
    return deserializeAndGetBlobWithVersion(stream).getBlobData();
  }

  static DeserializedBlob deserializeAndGetBlobWithVersion(InputStream stream)
      throws IOException, MessageFormatException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case Blob_Version_V1:
        return new DeserializedBlob(Blob_Version_V1, Blob_Format_V1.deserializeBlobRecord(crcStream));
      case Blob_Version_V2:
        return new DeserializedBlob(Blob_Version_V2, Blob_Format_V2.deserializeBlobRecord(crcStream));
      default:
        throw new MessageFormatException("data version not supported", MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  static boolean isValidBlobRecordVersion(short blobRecordVersion) {
    switch (blobRecordVersion) {
      case Blob_Version_V1:
        return true;
      case Blob_Version_V2:
        return true;
      default:
        return false;
    }
  }

  /**
   * Deserialize a complete blob record into a {@link BlobAll} object.
   * @param stream the {@link InputStream} from which to read the blob record.
   * @param storeKeyFactory the factory for parsing store keys.
   * @return a {@link BlobAll} object with the {@link BlobInfo} and {@link BlobData} for the blob.
   * @throws IOException
   * @throws MessageFormatException
   */
  public static BlobAll deserializeBlobAll(InputStream stream, StoreKeyFactory storeKeyFactory)
      throws IOException, MessageFormatException {
    DataInputStream inputStream = new DataInputStream(stream);
    short headerVersion = inputStream.readShort();
    ByteBuffer headerBuf;
    MessageHeader_Format header;
    switch (headerVersion) {
      case Message_Header_Version_V1:
        headerBuf = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
        headerBuf.putShort(headerVersion);
        inputStream.read(headerBuf.array(), Version_Field_Size_In_Bytes,
            MessageHeader_Format_V1.getHeaderSize() - Version_Field_Size_In_Bytes);
        headerBuf.rewind();
        header = new MessageHeader_Format_V1(headerBuf);
        break;
      case Message_Header_Version_V2:
        headerBuf = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V2.getHeaderSize());
        headerBuf.putShort(headerVersion);
        inputStream.read(headerBuf.array(), Version_Field_Size_In_Bytes,
            MessageHeader_Format_V2.getHeaderSize() - Version_Field_Size_In_Bytes);
        headerBuf.rewind();
        header = new MessageHeader_Format_V2(headerBuf);
        break;
      default:
        throw new MessageFormatException("Message header version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
    header.verifyHeader();
    StoreKey storeKey = storeKeyFactory.getStoreKey(new DataInputStream(stream));
    ByteBuffer blobEncryptionKey = null;
    if (header.hasEncryptionKeyRecord()) {
      blobEncryptionKey = deserializeBlobEncryptionKey(stream);
    }
    BlobProperties blobProperties = deserializeBlobProperties(stream);
    byte[] userMetadata = deserializeUserMetadata(stream).array();
    BlobData blobData = deserializeBlob(stream);
    return new BlobAll(storeKey, blobEncryptionKey, new BlobInfo(blobProperties, userMetadata), blobData);
  }

  /**
   * An interface that has to be implemented by all versions of the Message Header Format.
   */
  public interface MessageHeader_Format {
    /**
     * @return the version of this Message Header Format
     */
    short getVersion();

    /**
     * @return the offset of the message payload (the part after the blob id) relative to the end of the header.
     */
    int getPayloadRelativeOffset();

    /**
     * @return the message payload size.
     */
    long getMessageSize();

    /**
     * @return if this is a put record, the relative offset of the Blob Properties Record from the end of the header. If
     * this is a delete record, returns {@link #Message_Header_Invalid_Relative_Offset}
     */
    int getBlobPropertiesRecordRelativeOffset();

    /**
     * @return the length of the BlobProperties record.
     */
    int getBlobPropertiesRecordSize();

    /**
     * @return if this is a delete record, the relative offset of the Delete Sub Record from the end of the header. If
     * this is a put record, returns {@link #Message_Header_Invalid_Relative_Offset}
     */
    int getDeleteRecordRelativeOffset();

    /**
     * @return if this is a put record with an encryption key sub record, the offset of the encryption key sub record
     * relative to the end of the header; else returns {@link #Message_Header_Invalid_Relative_Offset}
     */
    int getBlobEncryptionKeyRecordRelativeOffset();

    /**
     * @return the length of the Blob Encryption Key record.
     */
    int getBlobEncryptionKeyRecordSize();

    /**
     * @return the offset of the user metadata sub record relative to the end of the header.
     */
    int getUserMetadataRecordRelativeOffset();

    /**
     * @return the length of the User Metadata record.
     */
    int getUserMetadataRecordSize();

    /**
     * @return the offset of the blob sub record relative to the end of the header.
     */
    int getBlobRecordRelativeOffset();

    /**
     * @return the length of the Blob sub record.
     */
    long getBlobRecordSize();

    /**
     * @return the crc of the header.
     */
    long getCrc();

    /**
     * Verify that the header is proper.
     * @throws MessageFormatException
     */
    void verifyHeader() throws MessageFormatException;

    /**
     * @return true if this record has an encryption key sub record; false otherwise.
     */
    boolean hasEncryptionKeyRecord();

    /**
     * @return true if this is a put record; false if this is a delete record.
     */
    boolean isPutRecord();
  }

  /**
   *
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |              |               |           |               |           |           |
   * | version | payload size | Blob Property | Delete    | User Metadata | Blob      | Crc       |
   * |(2 bytes)|   (8 bytes)  | Relative      | Relative  | Relative      | Relative  | (8 bytes) |
   * |         |              | Offset        | Offset    | Offset        | Offset    |           |
   * |         |              | (4 bytes)     | (4 bytes) | (4 bytes)     | (4 bytes) |           |
   * |         |              |               |           |               |           |           |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *
   *  version         - The version of the message header
   *
   *  payload size    - The size of the message payload.
   *                    (Blob prop record size or delete record size) + user metadata size + blob size
   *
   *  blob property   - The offset at which the blob property record is located relative to this message. Only one of
   *  relative offset   blob property/delete relative offset field can exist. Non existence is indicated by -1
   *
   *  delete          - The offset at which the delete record is located relative to this message. Only one of blob
   *  relative offset   property/delete relative offset field can exist. Non existence is indicated by -1
   *
   *  user metadata   - The offset at which the user metadata record is located relative to this message. This exist
   *  relative offset   only when blob property record and blob record exist
   *
   *  blob metadata   - The offset at which the blob record is located relative to this message. This exist only when
   *  relative offset   blob property record and user metadata record exist
   *
   *  crc             - The crc of the message header
   *
   */
  public static class MessageHeader_Format_V1 implements MessageHeader_Format {
    private ByteBuffer buffer;

    private static final Logger logger = LoggerFactory.getLogger(MessageHeader_Format_V1.class);

    // total size field start offset and size
    public static final int Total_Size_Field_Offset_In_Bytes = Version_Field_Size_In_Bytes;
    public static final int Total_Size_Field_Size_In_Bytes = 8;

    // relative offset fields start offset and size
    private static final int Number_Of_Relative_Offset_Fields = 4;
    public static final int Relative_Offset_Field_Sizes_In_Bytes = 4;
    public static final int BlobProperties_Relative_Offset_Field_Offset_In_Bytes =
        Total_Size_Field_Offset_In_Bytes + Total_Size_Field_Size_In_Bytes;
    public static final int Delete_Relative_Offset_Field_Offset_In_Bytes =
        BlobProperties_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
        Delete_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int Blob_Relative_Offset_Field_Offset_In_Bytes =
        UserMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    // crc field start offset
    public static final int Crc_Field_Offset_In_Bytes =
        Blob_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static int getHeaderSize() {
      return Version_Field_Size_In_Bytes + Total_Size_Field_Size_In_Bytes + (Number_Of_Relative_Offset_Fields
          * Relative_Offset_Field_Sizes_In_Bytes) + Crc_Size;
    }

    public static void serializeHeader(ByteBuffer outputBuffer, long totalSize, int blobPropertiesRecordRelativeOffset,
        int deleteRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      checkHeaderConstraints(totalSize, blobPropertiesRecordRelativeOffset, deleteRecordRelativeOffset,
          userMetadataRecordRelativeOffset, blobRecordRelativeOffset);
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Message_Header_Version_V1);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(blobPropertiesRecordRelativeOffset);
      outputBuffer.putInt(deleteRecordRelativeOffset);
      outputBuffer.putInt(userMetadataRecordRelativeOffset);
      outputBuffer.putInt(blobRecordRelativeOffset);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getHeaderSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
      logger.trace("serializing header : version {} size {} blobpropertiesrecordrelativeoffset {} "
              + "deleterecordrelativeoffset {} usermetadatarecordrelativeoffset {} blobrecordrelativeoffset {} crc {}",
          Message_Header_Version_V1, totalSize, blobPropertiesRecordRelativeOffset, deleteRecordRelativeOffset,
          userMetadataRecordRelativeOffset, blobPropertiesRecordRelativeOffset, crc.getValue());
    }

    // checks the following constraints
    // 1. totalSize is greater than 0
    // 2. if blobPropertiesRecordRelativeOffset is greater than 0, ensures that deleteRecordRelativeOffset
    //    is set to Message_Header_Invalid_Relative_Offset and userMetadataRecordRelativeOffset
    //    and blobRecordRelativeOffset is positive
    // 3. if deleteRecordRelativeOffset is greater than 0, ensures that all the other offsets are set to
    //    Message_Header_Invalid_Relative_Offset
    private static void checkHeaderConstraints(long totalSize, int blobPropertiesRecordRelativeOffset,
        int deleteRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      // check constraints
      if (totalSize <= 0) {
        throw new MessageFormatException(
            "checkHeaderConstraints - totalSize " + totalSize + " needs to be greater than 0",
            MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (blobPropertiesRecordRelativeOffset > 0 && (
          deleteRecordRelativeOffset != Message_Header_Invalid_Relative_Offset || userMetadataRecordRelativeOffset <= 0
              || blobRecordRelativeOffset <= 0)) {
        throw new MessageFormatException(
            "checkHeaderConstraints - blobPropertiesRecordRelativeOffset is greater than 0 "
                + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
                + blobPropertiesRecordRelativeOffset + " deleteRecordRelativeOffset " + deleteRecordRelativeOffset
                + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
                + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (deleteRecordRelativeOffset > 0 && (
          blobPropertiesRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || userMetadataRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobRecordRelativeOffset != Message_Header_Invalid_Relative_Offset)) {
        throw new MessageFormatException("checkHeaderConstraints - deleteRecordRelativeOffset is greater than 0 "
            + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
            + blobPropertiesRecordRelativeOffset + " deleteRecordRelativeOffset " + deleteRecordRelativeOffset
            + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
            + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }
    }

    public MessageHeader_Format_V1(ByteBuffer input) {
      buffer = input;
    }

    @Override
    public short getVersion() {
      return buffer.getShort(0);
    }

    @Override
    public boolean isPutRecord() {
      return getBlobPropertiesRecordRelativeOffset() != Message_Header_Invalid_Relative_Offset;
    }

    @Override
    public int getPayloadRelativeOffset() {
      return isPutRecord() ? getBlobPropertiesRecordRelativeOffset() : getDeleteRecordRelativeOffset();
    }

    @Override
    public long getMessageSize() {
      return buffer.getLong(Total_Size_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobPropertiesRecordRelativeOffset() {
      return buffer.getInt(BlobProperties_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobPropertiesRecordSize() {
      return getUserMetadataRecordRelativeOffset() - getBlobPropertiesRecordRelativeOffset();
    }

    @Override
    public int getDeleteRecordRelativeOffset() {
      return buffer.getInt(Delete_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobEncryptionKeyRecordRelativeOffset() {
      return Message_Header_Invalid_Relative_Offset;
    }

    @Override
    public int getBlobEncryptionKeyRecordSize() {
      return 0;
    }

    @Override
    public boolean hasEncryptionKeyRecord() {
      return false;
    }

    @Override
    public int getUserMetadataRecordRelativeOffset() {
      return buffer.getInt(UserMetadata_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getUserMetadataRecordSize() {
      return getBlobRecordRelativeOffset() - getUserMetadataRecordRelativeOffset();
    }

    @Override
    public int getBlobRecordRelativeOffset() {
      return buffer.getInt(Blob_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public long getBlobRecordSize() {
      int messageSizeExcludingBlobRecord = getBlobRecordRelativeOffset() - getPayloadRelativeOffset();
      return getMessageSize() - messageSizeExcludingBlobRecord;
    }

    @Override
    public long getCrc() {
      return buffer.getLong(Crc_Field_Offset_In_Bytes);
    }

    @Override
    public void verifyHeader() throws MessageFormatException {
      verifyCrc();
      checkHeaderConstraints(getMessageSize(), getBlobPropertiesRecordRelativeOffset(), getDeleteRecordRelativeOffset(),
          getUserMetadataRecordRelativeOffset(), getBlobRecordRelativeOffset());
    }

    private void verifyCrc() throws MessageFormatException {
      Crc32 crc = new Crc32();
      crc.update(buffer.array(), 0, buffer.limit() - Crc_Size);
      if (crc.getValue() != getCrc()) {
        throw new MessageFormatException("Message header is corrupt", MessageFormatErrorCodes.Data_Corrupt);
      }
    }
  }

  /**
   *
   *  - - - - - - - - - - - - - - - - - - -- - -- - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
   * |         |              |                 |               |           |               |           |           |
   * | version | payload size | Blob Encryption | Blob Property | Delete    | User Metadata | Blob      | Crc       |
   * |(2 bytes)|   (8 bytes)  | Key Relative    | Relative      | Relative  | Relative      | Relative  | (8 bytes) |
   * |         |              | Offset          | Offset        | Offset    | Offset        | Offset    |           |
   * |         |              | (4 bytes)       | (4 bytes)     | (4 bytes) | (4 bytes)     | (4 bytes) |           |
   * |         |              |                 |               |           |               |           |           |
   *  - - - - - - - - - - - - - - - - - - -- - -- - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
   *
   *  version         - The version of the message header
   *
   *  payload size    - The size of the message payload.
   *                    Blob Encryption Key Record Size (if present) + (Blob prop record size or delete record size) +
   *                    user metadata size + blob size
   *
   *  Blob Encryption - The offset at which the blob encryption key record is located relative to this message.
   *  Key relative      Non-existence of blob key record is indicated by -1. Blob Keys are optionally present for Put
   *  offset            records. Blob Keys will be absent for Delete records.
   *
   *  blob property   - The offset at which the blob property record is located relative to this message. Only one of
   *  relative offset   blob property/delete relative offset field can exist. Non existence is indicated by -1
   *
   *  delete          - The offset at which the delete record is located relative to this message. Only one of blob
   *  relative offset   property/delete relative offset field can exist. Non existence is indicated by -1
   *
   *  user metadata   - The offset at which the user metadata record is located relative to this message. This exist
   *  relative offset   only when blob property record and blob record exist
   *
   *  blob metadata   - The offset at which the blob record is located relative to this message. This exist only when
   *  relative offset   blob property record and user metadata record exist
   *
   *  crc             - The crc of the message header
   *
   */
  public static class MessageHeader_Format_V2 implements MessageHeader_Format {
    private ByteBuffer buffer;

    private static final Logger logger = LoggerFactory.getLogger(MessageHeader_Format_V2.class);
    // total size field start offset and size
    public static final int Total_Size_Field_Offset_In_Bytes = Version_Field_Size_In_Bytes;
    public static final int Total_Size_Field_Size_In_Bytes = 8;

    // relative offset fields start offset and size
    private static final int Number_Of_Relative_Offset_Fields = 5;
    public static final int Relative_Offset_Field_Sizes_In_Bytes = 4;
    public static final int Blob_Encryption_Key_Relative_Offset_Field_Offset_In_Bytes =
        Total_Size_Field_Offset_In_Bytes + Total_Size_Field_Size_In_Bytes;
    public static final int BlobProperties_Relative_Offset_Field_Offset_In_Bytes =
        Blob_Encryption_Key_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int Delete_Relative_Offset_Field_Offset_In_Bytes =
        BlobProperties_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
        Delete_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int Blob_Relative_Offset_Field_Offset_In_Bytes =
        UserMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    // crc field start offset
    public static final int Crc_Field_Offset_In_Bytes =
        Blob_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static int getHeaderSize() {
      return Version_Field_Size_In_Bytes + Total_Size_Field_Size_In_Bytes + (Number_Of_Relative_Offset_Fields
          * Relative_Offset_Field_Sizes_In_Bytes) + Crc_Size;
    }

    public static void serializeHeader(ByteBuffer outputBuffer, long totalSize,
        int blobEncryptionKeyRecordRelativeOffset, int blobPropertiesRecordRelativeOffset,
        int deleteRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      checkHeaderConstraints(totalSize, blobEncryptionKeyRecordRelativeOffset, blobPropertiesRecordRelativeOffset,
          deleteRecordRelativeOffset, userMetadataRecordRelativeOffset, blobRecordRelativeOffset);
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Message_Header_Version_V2);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(blobEncryptionKeyRecordRelativeOffset);
      outputBuffer.putInt(blobPropertiesRecordRelativeOffset);
      outputBuffer.putInt(deleteRecordRelativeOffset);
      outputBuffer.putInt(userMetadataRecordRelativeOffset);
      outputBuffer.putInt(blobRecordRelativeOffset);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getHeaderSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
      logger.trace(
          "serializing header : version {} size {} blobencryptionkeyrecordrelativeoffset {} blobpropertiesrecordrelativeoffset {} "
              + "deleterecordrelativeoffset {} usermetadatarecordrelativeoffset {} blobrecordrelativeoffset {} crc {}",
          Message_Header_Version_V2, totalSize, blobEncryptionKeyRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, deleteRecordRelativeOffset, userMetadataRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, crc.getValue());
    }

    // checks the following constraints
    // 1. totalSize is greater than 0
    // 2. if blobPropertiesRecordRelativeOffset is greater than 0, ensures that deleteRecordRelativeOffset
    //    is set to Message_Header_Invalid_Relative_Offset and userMetadataRecordRelativeOffset
    //    and blobRecordRelativeOffset is positive
    // 3. if deleteRecordRelativeOffset is greater than 0, ensures that all the other offsets are set to
    //    Message_Header_Invalid_Relative_Offset
    private static void checkHeaderConstraints(long totalSize, int blobEncryptionKeyRecordRelativeOffset,
        int blobPropertiesRecordRelativeOffset, int deleteRecordRelativeOffset, int userMetadataRecordRelativeOffset,
        int blobRecordRelativeOffset) throws MessageFormatException {
      // check constraints
      if (totalSize <= 0) {
        throw new MessageFormatException(
            "checkHeaderConstraints - totalSize " + totalSize + " needs to be greater than 0",
            MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (blobPropertiesRecordRelativeOffset > 0 && (
          deleteRecordRelativeOffset != Message_Header_Invalid_Relative_Offset || userMetadataRecordRelativeOffset <= 0
              || blobRecordRelativeOffset <= 0)) {
        throw new MessageFormatException(
            "checkHeaderConstraints - blobPropertiesRecordRelativeOffset is greater than 0 "
                + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
                + blobPropertiesRecordRelativeOffset + " deleteRecordRelativeOffset " + deleteRecordRelativeOffset
                + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
                + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (deleteRecordRelativeOffset > 0 && (
          blobEncryptionKeyRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobPropertiesRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || userMetadataRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobRecordRelativeOffset != Message_Header_Invalid_Relative_Offset)) {
        throw new MessageFormatException("checkHeaderConstraints - deleteRecordRelativeOffset is greater than 0 "
            + " but other properties do not satisfy constraints" + " blobEncryptionKeyRelativeOffset "
            + blobEncryptionKeyRecordRelativeOffset + " blobPropertiesRecordRelativeOffset "
            + blobPropertiesRecordRelativeOffset + " deleteRecordRelativeOffset " + deleteRecordRelativeOffset
            + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
            + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }
    }

    public MessageHeader_Format_V2(ByteBuffer input) {
      buffer = input;
    }

    @Override
    public short getVersion() {
      return buffer.getShort(0);
    }

    @Override
    public long getMessageSize() {
      return buffer.getLong(Total_Size_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobPropertiesRecordRelativeOffset() {
      return buffer.getInt(BlobProperties_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobPropertiesRecordSize() {
      return getUserMetadataRecordRelativeOffset() - getBlobPropertiesRecordRelativeOffset();
    }

    @Override
    public boolean isPutRecord() {
      return getBlobPropertiesRecordRelativeOffset() != Message_Header_Invalid_Relative_Offset;
    }

    @Override
    public int getDeleteRecordRelativeOffset() {
      return buffer.getInt(Delete_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobEncryptionKeyRecordRelativeOffset() {
      return buffer.getInt(Blob_Encryption_Key_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getBlobEncryptionKeyRecordSize() {
      if (hasEncryptionKeyRecord()) {
        return getBlobPropertiesRecordRelativeOffset() - getBlobEncryptionKeyRecordRelativeOffset();
      } else {
        return 0;
      }
    }

    @Override
    public boolean hasEncryptionKeyRecord() {
      return getBlobEncryptionKeyRecordRelativeOffset() != Message_Header_Invalid_Relative_Offset;
    }

    @Override
    public int getUserMetadataRecordRelativeOffset() {
      return buffer.getInt(UserMetadata_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public int getUserMetadataRecordSize() {
      return getBlobRecordRelativeOffset() - getUserMetadataRecordRelativeOffset();
    }

    @Override
    public int getBlobRecordRelativeOffset() {
      return buffer.getInt(Blob_Relative_Offset_Field_Offset_In_Bytes);
    }

    @Override
    public long getBlobRecordSize() {
      int messageSizeExcludingBlobRecord = getBlobRecordRelativeOffset() - getPayloadRelativeOffset();
      return getMessageSize() - messageSizeExcludingBlobRecord;
    }

    @Override
    public int getPayloadRelativeOffset() {
      if (isPutRecord()) {
        return hasEncryptionKeyRecord() ? getBlobEncryptionKeyRecordRelativeOffset()
            : getBlobPropertiesRecordRelativeOffset();
      } else {
        return getDeleteRecordRelativeOffset();
      }
    }

    @Override
    public long getCrc() {
      return buffer.getLong(Crc_Field_Offset_In_Bytes);
    }

    @Override
    public void verifyHeader() throws MessageFormatException {
      verifyCrc();
      checkHeaderConstraints(getMessageSize(), getBlobEncryptionKeyRecordRelativeOffset(),
          getBlobPropertiesRecordRelativeOffset(), getDeleteRecordRelativeOffset(),
          getUserMetadataRecordRelativeOffset(), getBlobRecordRelativeOffset());
    }

    private void verifyCrc() throws MessageFormatException {
      Crc32 crc = new Crc32();
      crc.update(buffer.array(), 0, buffer.limit() - Crc_Size);
      if (crc.getValue() != getCrc()) {
        throw new MessageFormatException("Message header is corrupt", MessageFormatErrorCodes.Data_Corrupt);
      }
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |               |               |           |            |
   * | version |   property1   |   property2   |           |     Crc    |
   * |(2 bytes)| (1 - n bytes) | (1 - n bytes) |   .....   |  (8 bytes) |
   * |         |               |               |           |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version         - The version of the blob property record
   *
   *  properties      - Variable size properties that define the blob.
   *
   *  crc             - The crc of the blob property record
   *
   */
  public static class BlobProperties_Format_V1 {

    private static Logger logger = LoggerFactory.getLogger(BlobProperties_Format_V1.class);

    public static int getBlobPropertiesRecordSize(BlobProperties properties) {
      return Version_Field_Size_In_Bytes + BlobPropertiesSerDe.getBlobPropertiesSerDeSize(properties) + Crc_Size;
    }

    public static void serializeBlobPropertiesRecord(ByteBuffer outputBuffer, BlobProperties properties) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(BlobProperties_Version_V1);
      BlobPropertiesSerDe.serializeBlobProperties(outputBuffer, properties);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getBlobPropertiesRecordSize(properties) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static BlobProperties deserializeBlobPropertiesRecord(CrcInputStream crcStream)
        throws IOException, MessageFormatException {
      try {
        DataInputStream dataStream = new DataInputStream(crcStream);
        BlobProperties properties = BlobPropertiesSerDe.getBlobPropertiesFromStream(dataStream);
        long actualCRC = crcStream.getValue();
        long expectedCRC = dataStream.readLong();
        if (actualCRC != expectedCRC) {
          logger.error(
              "corrupt data while parsing blob properties Expected CRC " + expectedCRC + " Actual CRC " + actualCRC);
          throw new MessageFormatException("Blob property data is corrupt", MessageFormatErrorCodes.Data_Corrupt);
        }
        return properties;
      } catch (Exception e) {
        logger.error("Blob property failed to be parsed. Data may be corrupt with exception {}", e);
        throw new MessageFormatException("Blob property failed to be parsed. Data may be corrupt",
            MessageFormatErrorCodes.Data_Corrupt);
      }
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - -
   * |         |               |            |
   * | version |   delete byte |    Crc     |
   * |(2 bytes)|    (1 byte)   |  (8 bytes) |
   * |         |               |            |
   *  - - - - - - - - - - - - - - - - - - -
   *  version         - The version of the delete record
   *
   *  delete byte     - Takes value 0 or 1. If it is set to 1, it signifies that the blob is deleted. The field
   *                    is required to be able to support undelete in the future if required.
   *
   *  crc             - The crc of the delete record
   *
   */
  public static class Delete_Format_V1 {

    public static final int Delete_Field_Size_In_Bytes = 1;
    private static Logger logger = LoggerFactory.getLogger(Delete_Format_V1.class);

    public static int getDeleteRecordSize() {
      return Version_Field_Size_In_Bytes + Delete_Field_Size_In_Bytes + Crc_Size;
    }

    public static void serializeDeleteRecord(ByteBuffer outputBuffer, DeleteRecord deleteRecord) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Delete_Version_V1);
      outputBuffer.put((byte) 1);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getDeleteRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static DeleteRecord deserializeDeleteRecord(CrcInputStream crcStream)
        throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      boolean isDeleted = dataStream.readByte() == 1;
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        logger.error(
            "corrupt data while parsing delete record Expected CRC " + expectedCRC + " Actual CRC " + actualCRC);
        throw new MessageFormatException("delete record data is corrupt", MessageFormatErrorCodes.Data_Corrupt);
      }
      return new DeleteRecord(UNKNOWN_ACCOUNT_ID, UNKNOWN_CONTAINER_ID, Utils.Infinite_Time);
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |               |               |               |             |
   * | version |   AccountId   |  ContainerId  |  DeletionTime |     Crc     |
   * |(2 bytes)|    (2 byte2)  |   (2 bytes)   |   (8 bytes)   |  (8 bytes)  |
   * |         |               |               |               |             |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version         - The version of the delete record
   *
   *  AccountId     - AccountId that the blob belongs to
   *
   *  ContainerId   - ContainerId that the blob belongs to
   *
   *  Deletion Time - Time of deletion in Ms
   *
   *  Crc           - The crc of the delete record
   *
   */
  public static class Delete_Format_V2 {

    public static final int ACCOUNT_ID_FIELD_SIZE_IN_BYTES = Short.BYTES;
    public static final int CONTAINER_ID_FIELD_SIZE_IN_BYTES = Short.BYTES;
    public static final int DELETION_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;

    public static int getDeleteRecordSize() {
      return Version_Field_Size_In_Bytes + ACCOUNT_ID_FIELD_SIZE_IN_BYTES + CONTAINER_ID_FIELD_SIZE_IN_BYTES
          + DELETION_TIME_FIELD_SIZE_IN_BYTES + Crc_Size;
    }

    public static void serializeDeleteRecord(ByteBuffer outputBuffer, DeleteRecord deleteRecord) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Delete_Version_V2);
      outputBuffer.putShort(deleteRecord.getAccountId());
      outputBuffer.putShort(deleteRecord.getContainerId());
      outputBuffer.putLong(deleteRecord.getDeletionTimeInMs());
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getDeleteRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static DeleteRecord deserializeDeleteRecord(CrcInputStream crcStream)
        throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      short accountId = dataStream.readShort();
      short containerId = dataStream.readShort();
      long deletionTimeInMs = dataStream.readLong();
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        throw new MessageFormatException("delete record data is corrupt (mismatch in CRC) ",
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new DeleteRecord(accountId, containerId, deletionTimeInMs);
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |           |                      |            |
   * | version |   size    |  blob encryption key |     Crc    |
   * |(2 bytes)| (4 bytes) |  (n bytes)           |  (8 bytes) |
   * |         |           |                      |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version                - The version of the blob key
   *
   *  size                   - The size of the blob key
   *
   *  blob encryption key    - The blob encryption key in bytes
   *
   *  crc                    - The crc of the blob key record
   *
   */
  public static class BlobEncryptionKey_Format_V1 {
    public static final int Blob_Encryption_Key_Size_Field_In_Bytes = 4;
    private static Logger logger = LoggerFactory.getLogger(BlobEncryptionKey_Format_V1.class);

    public static int getBlobEncryptionKeyRecordSize(ByteBuffer blobEncryptionKey) {
      return Version_Field_Size_In_Bytes + Blob_Encryption_Key_Size_Field_In_Bytes + blobEncryptionKey.remaining()
          + Crc_Size;
    }

    public static void serializeBlobEncryptionKeyRecord(ByteBuffer outputBuffer, ByteBuffer blobEncryptionKey) {
      int startOffset = outputBuffer.position();
      int blobEncryptionKeyRecordSize = getBlobEncryptionKeyRecordSize(blobEncryptionKey);
      outputBuffer.putShort(Blob_Encryption_Key_V1);
      outputBuffer.putInt(blobEncryptionKey.remaining());
      outputBuffer.put(blobEncryptionKey);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, blobEncryptionKeyRecordSize - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    static ByteBuffer deserializeBlobEncryptionKeyRecord(CrcInputStream crcStream)
        throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      ByteBuffer blobEncryptionKey = Utils.readIntBuffer(dataStream);
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        logger.error(
            "corrupt data while parsing blob key record, expected CRC " + expectedCRC + " Actual CRC " + actualCRC);
        throw new MessageFormatException("Blob Key is corrupt", MessageFormatErrorCodes.Data_Corrupt);
      }
      return blobEncryptionKey;
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |           |            |            |
   * | version |   size    |  content   |     Crc    |
   * |(2 bytes)| (4 bytes) |  (n bytes) |  (8 bytes) |
   * |         |           |            |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - -
   *  version    - The version of the user metadata record
   *
   *  size       - The size of the user metadata content
   *
   *  content    - The actual content that represents the user metadata
   *
   *  crc        - The crc of the user metadata record
   *
   */
  public static class UserMetadata_Format_V1 {
    public static final int UserMetadata_Size_Field_In_Bytes = 4;
    private static Logger logger = LoggerFactory.getLogger(UserMetadata_Format_V1.class);

    public static int getUserMetadataSize(ByteBuffer userMetadata) {
      return Version_Field_Size_In_Bytes + UserMetadata_Size_Field_In_Bytes + userMetadata.limit() + Crc_Size;
    }

    public static void serializeUserMetadataRecord(ByteBuffer outputBuffer, ByteBuffer userMetadata) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(UserMetadata_Version_V1);
      outputBuffer.putInt(userMetadata.limit());
      outputBuffer.put(userMetadata);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getUserMetadataSize(userMetadata) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static ByteBuffer deserializeUserMetadataRecord(CrcInputStream crcStream)
        throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      int usermetadataSize = dataStream.readInt();
      byte[] userMetadaBuffer = Utils.readBytesFromStream(dataStream, usermetadataSize);
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        logger.error(
            "corrupt data while parsing user metadata Expected CRC " + expectedCRC + " Actual CRC " + actualCRC);
        throw new MessageFormatException("User metadata is corrupt", MessageFormatErrorCodes.Data_Corrupt);
      }
      return ByteBuffer.wrap(userMetadaBuffer);
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |           |            |            |
   * | version |   size    |  content   |     Crc    |
   * |(2 bytes)| (8 bytes) |  (n bytes) |  (8 bytes) |
   * |         |           |            |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - -
   *  version    - The version of the blob record
   *
   *  size       - The size of the blob content
   *
   *  content    - The actual content that represents the blob
   *
   *  crc        - The crc of the blob record
   *
   */
  public static class Blob_Format_V1 {
    public static final int Blob_Size_Field_In_Bytes = 8;
    private static Logger logger = LoggerFactory.getLogger(Blob_Format_V1.class);

    public static long getBlobRecordSize(long blobSize) {
      return Version_Field_Size_In_Bytes + Blob_Size_Field_In_Bytes + blobSize + Crc_Size;
    }

    public static void serializePartialBlobRecord(ByteBuffer outputBuffer, long blobSize) {
      outputBuffer.putShort(Blob_Version_V1);
      outputBuffer.putLong(blobSize);
    }

    public static BlobData deserializeBlobRecord(CrcInputStream crcStream) throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      long dataSize = dataStream.readLong();
      if (dataSize > Integer.MAX_VALUE) {
        throw new IOException("We only support data of max size == MAX_INT. Error while reading blob from store");
      }
      ByteBufferInputStream output = new ByteBufferInputStream(crcStream, (int) dataSize);
      long crc = crcStream.getValue();
      long streamCrc = dataStream.readLong();
      if (crc != streamCrc) {
        logger.error("corrupt data while parsing blob content expectedcrc {} actualcrc {}", crc, streamCrc);
        throw new MessageFormatException("corrupt data while parsing blob content",
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new BlobData(BlobType.DataBlob, dataSize, output);
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |           |            |            |            |
   * | version | blobType  |    size    |  content   |     Crc    |
   * |(2 bytes)| (2 bytes) |  (8 bytes) |  (n bytes) |  (8 bytes) |
   * |         |           |            |            |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version    - The version of the blob record
   *
   *  blobType   - The type of the blob
   *
   *  size       - The size of the blob content
   *
   *  content    - The actual content that represents the blob
   *
   *  crc        - The crc of the blob record
   *
   */
  public static class Blob_Format_V2 {
    public static final int Blob_Size_Field_In_Bytes = 8;
    public static final int Blob_Type_Field_In_Bytes = 2;
    private static Logger logger = LoggerFactory.getLogger(Blob_Format_V2.class);

    public static long getBlobRecordSize(long blobSize) {
      return Version_Field_Size_In_Bytes + Blob_Type_Field_In_Bytes + Blob_Size_Field_In_Bytes + blobSize + Crc_Size;
    }

    public static void serializePartialBlobRecord(ByteBuffer outputBuffer, long blobContentSize, BlobType blobType) {
      outputBuffer.putShort(Blob_Version_V2);
      outputBuffer.putShort((short) blobType.ordinal());
      outputBuffer.putLong(blobContentSize);
    }

    public static BlobData deserializeBlobRecord(CrcInputStream crcStream) throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      short blobTypeOrdinal = dataStream.readShort();
      if (blobTypeOrdinal > BlobType.values().length) {
        logger.error("corrupt data while parsing blob content BlobContentType {}", blobTypeOrdinal);
        throw new MessageFormatException("corrupt data while parsing blob content",
            MessageFormatErrorCodes.Data_Corrupt);
      }
      BlobType blobContentType = BlobType.values()[blobTypeOrdinal];
      long dataSize = dataStream.readLong();
      if (dataSize > Integer.MAX_VALUE) {
        throw new IOException("We only support data of max size == MAX_INT. Error while reading blob from store");
      }
      ByteBufferInputStream output = new ByteBufferInputStream(crcStream, (int) dataSize);
      long crc = crcStream.getValue();
      long streamCrc = dataStream.readLong();
      if (crc != streamCrc) {
        logger.error("corrupt data while parsing blob content expectedcrc {} actualcrc {}", crc, streamCrc);
        throw new MessageFormatException("corrupt data while parsing blob content",
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new BlobData(blobContentType, dataSize, output);
    }
  }

  // Metadata_Content_Format_V1 (layout below) was unused and was removed to clean up the range request handling code.
  //  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  // |         |               |            |            |          |
  // | version |   no of keys  |    key1    |     key2   |  ......  |
  // |(2 bytes)|    (4 bytes)  |            |            |  ......  |
  // |         |               |            |            |          |
  //  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  //  version         - The version of the metadata content record
  //
  //  no of keys      - total number of keys
  //
  //  key1            - first key to be part of metadata blob
  //
  //  key2            - second key to be part of metadata blob

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |              |              |            |            |          |
   * | version |  chunk size  |  total size  |    key1    |     key2   |  ......  |
   * |(2 bytes)|  (4 bytes)   |  (8 bytes)   |            |            |  ......  |
   * |         |              |              |            |            |          |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version         - The version of the metadata content record
   *
   *  chunk size      - The size of each data chunk except the last, which could possibly be smaller.
   *
   *  total size      - total size of the object this metadata describes.
   *
   *  key1            - first key to be part of metadata blob
   *
   *  key2            - second key to be part of metadata blob
   *
   */
  public static class Metadata_Content_Format_V2 {
    private static final int Chunk_Size_Field_Size_In_Bytes = 4;
    private static final int Total_Size_Field_Size_In_Bytes = 8;

    /**
     * Get the total size of the metadata content record.
     * @param keySize The size of each key in bytes.
     * @param numberOfKeys The total number of keys.
     * @return The total size in bytes.
     */
    public static int getMetadataContentSize(int keySize, int numberOfKeys) {
      return Version_Field_Size_In_Bytes + Chunk_Size_Field_Size_In_Bytes + Total_Size_Field_Size_In_Bytes + (
          numberOfKeys * keySize);
    }

    /**
     * Serialize a metadata content record.
     * @param outputBuffer The buffer to serialize into.
     * @param chunkSize The size of each data chunk except the last, which could possibly be smaller.
     * @param totalSize The total size of the object this metadata describes.
     */
    public static void serializeMetadataContentRecord(ByteBuffer outputBuffer, int chunkSize, long totalSize,
        List<StoreKey> keys) {
      if (chunkSize <= 0 || ((totalSize + chunkSize - 1) / chunkSize) != keys.size()) {
        throw new IllegalArgumentException("Invalid totalSize or chunkSize");
      }
      int keySize = keys.get(0).sizeInBytes();
      outputBuffer.putShort(Metadata_Content_Version_V2);
      outputBuffer.putInt(chunkSize);
      outputBuffer.putLong(totalSize);
      for (StoreKey storeKey : keys) {
        if (storeKey.sizeInBytes() != keySize) {
          throw new IllegalArgumentException("Keys are not of same size");
        }
        outputBuffer.put(storeKey.toBytes());
      }
    }

    /**
     * Deserialize a metadata content record from a stream.
     * @param stream The stream to read the serialized record from.
     * @param storeKeyFactory The factory to use for parsing keys in the serialized metadata content record.
     * @return A {@link CompositeBlobInfo} object with the chunk size and list of keys from the record.
     * @throws IOException
     * @throws MessageFormatException
     */
    public static CompositeBlobInfo deserializeMetadataContentRecord(DataInputStream stream,
        StoreKeyFactory storeKeyFactory) throws IOException, MessageFormatException {
      List<StoreKey> keys = new ArrayList<StoreKey>();
      int chunkSize = stream.readInt();
      long totalSize = stream.readLong();
      long numberOfKeys = (totalSize + chunkSize - 1) / chunkSize;
      for (int i = 0; i < numberOfKeys; i++) {
        StoreKey storeKey = storeKeyFactory.getStoreKey(stream);
        keys.add(storeKey);
      }
      return new CompositeBlobInfo(chunkSize, totalSize, keys);
    }
  }
}

class DeserializedBlobProperties {
  private short version;
  private BlobProperties blobProperties;

  public DeserializedBlobProperties(short version, BlobProperties blobProperties) {
    this.version = version;
    this.blobProperties = blobProperties;
  }

  public short getVersion() {
    return version;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }
}

class DeserializedBlobEncryptionKey {
  private final short version;
  private final ByteBuffer encryptionKey;

  public DeserializedBlobEncryptionKey(short version, ByteBuffer encryptionKey) {
    this.version = version;
    this.encryptionKey = encryptionKey;
  }

  public short getVersion() {
    return version;
  }

  public ByteBuffer getEncryptionKey() {
    return encryptionKey;
  }
}

class DeserializedUserMetadata {
  private final short version;
  private final ByteBuffer userMetadata;

  public DeserializedUserMetadata(short version, ByteBuffer userMetadata) {
    this.version = version;
    this.userMetadata = userMetadata;
  }

  public short getVersion() {
    return version;
  }

  public ByteBuffer getUserMetadata() {
    return userMetadata;
  }
}

class DeserializedBlob {
  private short version;
  private BlobData blobData;

  public DeserializedBlob(short version, BlobData blobData) {
    this.version = version;
    this.blobData = blobData;
  }

  public short getVersion() {
    return version;
  }

  public BlobData getBlobData() {
    return blobData;
  }
}
