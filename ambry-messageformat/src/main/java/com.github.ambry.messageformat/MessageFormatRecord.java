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
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
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
  public static final short Message_Header_Version_V3 = 3;
  public static final short BlobProperties_Version_V1 = 1;
  public static final short Update_Version_V1 = 1;
  public static final short Update_Version_V2 = 2;
  public static final short Update_Version_V3 = 3;

  public static final short Blob_Encryption_Key_V1 = 1;
  public static final short UserMetadata_Version_V1 = 1;
  public static final short Blob_Version_V1 = 1;
  public static final short Blob_Version_V2 = 2;
  public static final short Metadata_Content_Version_V2 = 2;
  public static final short Metadata_Content_Version_V3 = 3;
  public static final int Message_Header_Invalid_Relative_Offset = -1;

  // Bumping version to a high number requires several things
  // 1. Change transformers to support newer version, in open source and closed source
  // 2. Release a newer version to all ambry-frontend and ambry-server
  // 3. Change the version to a high value.
  //
  // We have to do first two steps, then can we bump the version, otherwise, other ambry-server would
  // fail to replicate blobs through GetRequest/GetResponse, they don't know how to decode the bytes.
  static short headerVersionToUse = Message_Header_Version_V3;

  private static final short Delete_Subrecord_Version_V1 = 1;
  private static final short Undelete_Subrecord_Version_V1 = 1;
  private static final short Ttl_Update_Subrecord_Version_V1 = 1;

  public static boolean isValidHeaderVersion(short headerVersion) {
    switch (headerVersion) {
      case Message_Header_Version_V1:
      case Message_Header_Version_V2:
      case Message_Header_Version_V3:
        return true;
      default:
        return false;
    }
  }

  /**
   * Return the currently used message header version.
   * @return currently used message header version
   */
  public static short getCurrentMessageHeaderVersion() {
    return headerVersionToUse;
  }

  /**
   * Get the size of the header for the given header version.
   * @param headerVersion the version of the header for which the size is to be determined.
   * @return the size of the header for the given header version.
   * @throws MessageFormatException if the given header version is invalid.
   */
  public static int getHeaderSizeForVersion(short headerVersion) throws MessageFormatException {
    switch (headerVersion) {
      case Message_Header_Version_V1:
        return MessageHeader_Format_V1.getHeaderSize();
      case Message_Header_Version_V2:
        return MessageHeader_Format_V2.getHeaderSize();
      case Message_Header_Version_V3:
        return MessageHeader_Format_V3.getHeaderSize();
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
  public static MessageHeader_Format getMessageHeader(short headerVersion, ByteBuffer input)
      throws MessageFormatException {
    switch (headerVersion) {
      case Message_Header_Version_V1:
        return new MessageHeader_Format_V1(input);
      case Message_Header_Version_V2:
        return new MessageHeader_Format_V2(input);
      case Message_Header_Version_V3:
        return new MessageHeader_Format_V3(input);
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

  public static UpdateRecord deserializeUpdateRecord(InputStream stream) throws IOException, MessageFormatException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case Update_Version_V1:
        return Update_Format_V1.deserialize(crcStream);
      case Update_Version_V2:
        return Update_Format_V2.deserialize(crcStream);
      case Update_Version_V3:
        return Update_Format_V3.deserialize(crcStream);
      default:
        throw new MessageFormatException("update record version not supported: " + version,
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
    DataInputStream inputStream =
        stream instanceof DataInputStream ? (DataInputStream) stream : new DataInputStream(stream);
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
      case Message_Header_Version_V3:
        headerBuf = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V3.getHeaderSize());
        headerBuf.putShort(headerVersion);
        inputStream.read(headerBuf.array(), Version_Field_Size_In_Bytes,
            MessageHeader_Format_V3.getHeaderSize() - Version_Field_Size_In_Bytes);
        headerBuf.rewind();
        header = new MessageHeader_Format_V3(headerBuf);
        break;
      default:
        throw new MessageFormatException("Message header version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
    header.verifyHeader();
    StoreKey storeKey = storeKeyFactory.getStoreKey(inputStream);
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
     * @return whether this message header has an life version.  Life Version counts how many times
     * a blob has been undeleted
     */
    boolean hasLifeVersion();

    /**
     * @return the life version of this Message Header Format.  Life Version counts how many
     * times a blob has been undeleted
     */
    short getLifeVersion();

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
     * this is an update record, returns {@link #Message_Header_Invalid_Relative_Offset}
     */
    int getBlobPropertiesRecordRelativeOffset();

    /**
     * @return the length of the BlobProperties record.
     */
    int getBlobPropertiesRecordSize();

    /**
     * @return if this is an update record, the relative offset of the Sub Record from the end of the header. If this is
     * a put record, returns {@link #Message_Header_Invalid_Relative_Offset}
     */
    int getUpdateRecordRelativeOffset();

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
     * @return true if this is a put record; false if this is a update record.
     */
    boolean isPutRecord();
  }

  /**
   *
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |              |               |           |               |           |           |
   * | version | payload size | Blob Property | Update    | User Metadata | Blob      | Crc       |
   * |(2 bytes)|   (8 bytes)  | Relative      | Relative  | Relative      | Relative  | (8 bytes) |
   * |         |              | Offset        | Offset    | Offset        | Offset    |           |
   * |         |              | (4 bytes)     | (4 bytes) | (4 bytes)     | (4 bytes) |           |
   * |         |              |               |           |               |           |           |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *
   *  version         - The version of the message header
   *
   *  payload size    - The size of the message payload.
   *                    (Blob prop record size or update record size) + user metadata size + blob size
   *
   *  blob property   - The offset at which the blob property record is located relative to this message. Only one of
   *  relative offset   blob property/update relative offset field can exist. Non existence is indicated by -1
   *
   *  update          - The offset at which the update record is located relative to this message. Only one of blob
   *  relative offset   property/update relative offset field can exist. Non existence is indicated by -1
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
    public static final int Update_Relative_Offset_Field_Offset_In_Bytes =
        BlobProperties_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
        Update_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
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
        int updateRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      checkHeaderConstraints(totalSize, blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset,
          userMetadataRecordRelativeOffset, blobRecordRelativeOffset);
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Message_Header_Version_V1);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(blobPropertiesRecordRelativeOffset);
      outputBuffer.putInt(updateRecordRelativeOffset);
      outputBuffer.putInt(userMetadataRecordRelativeOffset);
      outputBuffer.putInt(blobRecordRelativeOffset);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getHeaderSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
      logger.trace("serializing header : version {} size {} blobpropertiesrecordrelativeoffset {} "
              + "updaterecordrelativeoffset {} usermetadatarecordrelativeoffset {} blobrecordrelativeoffset {} crc {}",
          Message_Header_Version_V1, totalSize, blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset,
          userMetadataRecordRelativeOffset, blobPropertiesRecordRelativeOffset, crc.getValue());
    }

    // checks the following constraints
    // 1. totalSize is greater than 0
    // 2. if blobPropertiesRecordRelativeOffset is greater than 0, ensures that updateRecordRelativeOffset
    //    is set to Message_Header_Invalid_Relative_Offset and userMetadataRecordRelativeOffset
    //    and blobRecordRelativeOffset is positive
    // 3. if updateRecordRelativeOffset is greater than 0, ensures that all the other offsets are set to
    //    Message_Header_Invalid_Relative_Offset
    private static void checkHeaderConstraints(long totalSize, int blobPropertiesRecordRelativeOffset,
        int updateRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      // check constraints
      if (totalSize <= 0) {
        throw new MessageFormatException(
            "checkHeaderConstraints - totalSize " + totalSize + " needs to be greater than 0",
            MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (blobPropertiesRecordRelativeOffset > 0 && (
          updateRecordRelativeOffset != Message_Header_Invalid_Relative_Offset || userMetadataRecordRelativeOffset <= 0
              || blobRecordRelativeOffset <= 0)) {
        throw new MessageFormatException(
            "checkHeaderConstraints - blobPropertiesRecordRelativeOffset is greater than 0 "
                + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
                + blobPropertiesRecordRelativeOffset + " updateRecordRelativeOffset " + updateRecordRelativeOffset
                + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
                + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (updateRecordRelativeOffset > 0 && (
          blobPropertiesRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || userMetadataRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobRecordRelativeOffset != Message_Header_Invalid_Relative_Offset)) {
        throw new MessageFormatException("checkHeaderConstraints - updateRecordRelativeOffset is greater than 0 "
            + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
            + blobPropertiesRecordRelativeOffset + " updateRecordRelativeOffset " + updateRecordRelativeOffset
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
    public boolean hasLifeVersion() {
      return false;
    }

    @Override
    public short getLifeVersion() {
      return 0;
    }

    @Override
    public boolean isPutRecord() {
      return getBlobPropertiesRecordRelativeOffset() != Message_Header_Invalid_Relative_Offset;
    }

    @Override
    public int getPayloadRelativeOffset() {
      return isPutRecord() ? getBlobPropertiesRecordRelativeOffset() : getUpdateRecordRelativeOffset();
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
    public int getUpdateRecordRelativeOffset() {
      return buffer.getInt(Update_Relative_Offset_Field_Offset_In_Bytes);
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
      checkHeaderConstraints(getMessageSize(), getBlobPropertiesRecordRelativeOffset(), getUpdateRecordRelativeOffset(),
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
   * | version | payload size | Blob Encryption | Blob Property | Update    | User Metadata | Blob      | Crc       |
   * |(2 bytes)|   (8 bytes)  | Key Relative    | Relative      | Relative  | Relative      | Relative  | (8 bytes) |
   * |         |              | Offset          | Offset        | Offset    | Offset        | Offset    |           |
   * |         |              | (4 bytes)       | (4 bytes)     | (4 bytes) | (4 bytes)     | (4 bytes) |           |
   * |         |              |                 |               |           |               |           |           |
   *  - - - - - - - - - - - - - - - - - - -- - -- - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
   *
   *  version         - The version of the message header
   *
   *  payload size    - The size of the message payload.
   *                    Blob Encryption Key Record Size (if present) + (Blob prop record size or update record size) +
   *                    user metadata size + blob size
   *
   *  Blob Encryption - The offset at which the blob encryption key record is located relative to this message.
   *  Key relative      Non-existence of blob key record is indicated by -1. Blob Keys are optionally present for Put
   *  offset            records. Blob Keys will be absent for update records.
   *
   *  blob property   - The offset at which the blob property record is located relative to this message. Only one of
   *  relative offset   blob property/update relative offset field can exist. Non existence is indicated by -1
   *
   *  update          - The offset at which the update record is located relative to this message. Only one of blob
   *  relative offset   property/update relative offset field can exist. Non existence is indicated by -1
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
    public static final int Update_Relative_Offset_Field_Offset_In_Bytes =
        BlobProperties_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
        Update_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
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
        int updateRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      checkHeaderConstraints(totalSize, blobEncryptionKeyRecordRelativeOffset, blobPropertiesRecordRelativeOffset,
          updateRecordRelativeOffset, userMetadataRecordRelativeOffset, blobRecordRelativeOffset);
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Message_Header_Version_V2);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(blobEncryptionKeyRecordRelativeOffset);
      outputBuffer.putInt(blobPropertiesRecordRelativeOffset);
      outputBuffer.putInt(updateRecordRelativeOffset);
      outputBuffer.putInt(userMetadataRecordRelativeOffset);
      outputBuffer.putInt(blobRecordRelativeOffset);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getHeaderSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
      logger.trace(
          "serializing header : version {} size {} blobencryptionkeyrecordrelativeoffset {} blobpropertiesrecordrelativeoffset {} "
              + "updaterecordrelativeoffset {} usermetadatarecordrelativeoffset {} blobrecordrelativeoffset {} crc {}",
          Message_Header_Version_V2, totalSize, blobEncryptionKeyRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset, userMetadataRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, crc.getValue());
    }

    // checks the following constraints
    // 1. totalSize is greater than 0
    // 2. if blobPropertiesRecordRelativeOffset is greater than 0, ensures that updateRecordRelativeOffset
    //    is set to Message_Header_Invalid_Relative_Offset and userMetadataRecordRelativeOffset
    //    and blobRecordRelativeOffset is positive
    // 3. if updateRecordRelativeOffset is greater than 0, ensures that all the other offsets are set to
    //    Message_Header_Invalid_Relative_Offset
    private static void checkHeaderConstraints(long totalSize, int blobEncryptionKeyRecordRelativeOffset,
        int blobPropertiesRecordRelativeOffset, int updateRecordRelativeOffset, int userMetadataRecordRelativeOffset,
        int blobRecordRelativeOffset) throws MessageFormatException {
      // check constraints
      if (totalSize <= 0) {
        throw new MessageFormatException(
            "checkHeaderConstraints - totalSize " + totalSize + " needs to be greater than 0",
            MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (blobPropertiesRecordRelativeOffset > 0 && (
          updateRecordRelativeOffset != Message_Header_Invalid_Relative_Offset || userMetadataRecordRelativeOffset <= 0
              || blobRecordRelativeOffset <= 0)) {
        throw new MessageFormatException(
            "checkHeaderConstraints - blobPropertiesRecordRelativeOffset is greater than 0 "
                + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
                + blobPropertiesRecordRelativeOffset + " updateRecordRelativeOffset " + updateRecordRelativeOffset
                + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
                + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (updateRecordRelativeOffset > 0 && (
          blobEncryptionKeyRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobPropertiesRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || userMetadataRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobRecordRelativeOffset != Message_Header_Invalid_Relative_Offset)) {
        throw new MessageFormatException("checkHeaderConstraints - updateRecordRelativeOffset is greater than 0 "
            + " but other properties do not satisfy constraints" + " blobEncryptionKeyRelativeOffset "
            + blobEncryptionKeyRecordRelativeOffset + " blobPropertiesRecordRelativeOffset "
            + blobPropertiesRecordRelativeOffset + " updateRecordRelativeOffset " + updateRecordRelativeOffset
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
    public boolean hasLifeVersion() {
      return false;
    }

    @Override
    public short getLifeVersion() {
      return 0;
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
    public int getUpdateRecordRelativeOffset() {
      return buffer.getInt(Update_Relative_Offset_Field_Offset_In_Bytes);
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
        return getUpdateRecordRelativeOffset();
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
          getBlobPropertiesRecordRelativeOffset(), getUpdateRecordRelativeOffset(),
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
   *  - - - - - - - - - - -  - - - - - - - - - - - - - -- - -- - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
   * |         |           |              |                 |               |           |               |           |           |
   * | version |  life     | payload size | Blob Encryption | Blob Property | Update    | User Metadata | Blob      | Crc       |
   * |(2 bytes)|  version  |   (8 bytes)  | Key Relative    | Relative      | Relative  | Relative      | Relative  | (8 bytes) |
   * |         | (2 bytes) |              | Offset          | Offset        | Offset    | Offset        | Offset    |           |
   * |         |           |              | (4 bytes)       | (4 bytes)     | (4 bytes) | (4 bytes)     | (4 bytes) |           |
   * |         |           |              |                 |               |           |               |           |           |
   *  - - - - - - - - - - - - - - - - - - - - - - - - -- - -- - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
   *
   *  version         - The version of the message header
   *
   *  life version    - The life version of the message header
   *
   *  payload size    - The size of the message payload.
   *                    Blob Encryption Key Record Size (if present) + (Blob prop record size or update record size) +
   *                    user metadata size + blob size
   *
   *  Blob Encryption - The offset at which the blob encryption key record is located relative to this message.
   *  Key relative      Non-existence of blob key record is indicated by -1. Blob Keys are optionally present for Put
   *  offset            records. Blob Keys will be absent for update records.
   *
   *  blob property   - The offset at which the blob property record is located relative to this message. Only one of
   *  relative offset   blob property/update relative offset field can exist. Non existence is indicated by -1
   *
   *  update          - The offset at which the update record is located relative to this message. Only one of blob
   *  relative offset   property/update relative offset field can exist. Non existence is indicated by -1
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
  public static class MessageHeader_Format_V3 implements MessageHeader_Format {
    private ByteBuffer buffer;

    private static final Logger logger = LoggerFactory.getLogger(MessageHeader_Format_V3.class);
    // total size field start offset and size
    public static final int Life_Version_Field_Offset_In_Bytes = Version_Field_Size_In_Bytes;
    public static final int Life_Version_Field_Size_In_Bytes = 2;
    public static final int Total_Size_Field_Offset_In_Bytes =
        Life_Version_Field_Size_In_Bytes + Life_Version_Field_Offset_In_Bytes;
    public static final int Total_Size_Field_Size_In_Bytes = 8;

    // relative offset fields start offset and size
    private static final int Number_Of_Relative_Offset_Fields = 5;
    public static final int Relative_Offset_Field_Sizes_In_Bytes = 4;
    public static final int Blob_Encryption_Key_Relative_Offset_Field_Offset_In_Bytes =
        Total_Size_Field_Offset_In_Bytes + Total_Size_Field_Size_In_Bytes;
    public static final int BlobProperties_Relative_Offset_Field_Offset_In_Bytes =
        Blob_Encryption_Key_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int Update_Relative_Offset_Field_Offset_In_Bytes =
        BlobProperties_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
        Update_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;
    public static final int Blob_Relative_Offset_Field_Offset_In_Bytes =
        UserMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    // crc field start offset
    public static final int Crc_Field_Offset_In_Bytes =
        Blob_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static int getHeaderSize() {
      return Version_Field_Size_In_Bytes + Life_Version_Field_Size_In_Bytes + Total_Size_Field_Size_In_Bytes + (
          Number_Of_Relative_Offset_Fields * Relative_Offset_Field_Sizes_In_Bytes) + Crc_Size;
    }

    public static void serializeHeader(ByteBuffer outputBuffer, short lifeVersion, long totalSize,
        int blobEncryptionKeyRecordRelativeOffset, int blobPropertiesRecordRelativeOffset,
        int updateRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      checkHeaderConstraints(totalSize, lifeVersion, blobEncryptionKeyRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset, userMetadataRecordRelativeOffset,
          blobRecordRelativeOffset);
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Message_Header_Version_V3);
      outputBuffer.putShort(lifeVersion);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(blobEncryptionKeyRecordRelativeOffset);
      outputBuffer.putInt(blobPropertiesRecordRelativeOffset);
      outputBuffer.putInt(updateRecordRelativeOffset);
      outputBuffer.putInt(userMetadataRecordRelativeOffset);
      outputBuffer.putInt(blobRecordRelativeOffset);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getHeaderSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
      logger.trace(
          "serializing header : version {} lifeVersion {} size {} blobEncryptionKeyRecordRelativeOffset {} blobPropertiesRecordRelativeOffset {} "
              + "updateRecordRelativeOffset {} userMetadataRecordRelativeOffset {} blobRecordRelativeOffset {} crc {}",
          Message_Header_Version_V3, lifeVersion, totalSize, blobEncryptionKeyRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset, userMetadataRecordRelativeOffset,
          blobPropertiesRecordRelativeOffset, crc.getValue());
    }

    // checks the following constraints
    // 1. totalSize is greater than 0
    // 2. if blobPropertiesRecordRelativeOffset is greater than 0, ensures that updateRecordRelativeOffset
    //    is set to Message_Header_Invalid_Relative_Offset and userMetadataRecordRelativeOffset
    //    and blobRecordRelativeOffset is positive
    // 3. if updateRecordRelativeOffset is greater than 0, ensures that all the other offsets are set to
    //    Message_Header_Invalid_Relative_Offset
    private static void checkHeaderConstraints(long totalSize, short lifeVersion,
        int blobEncryptionKeyRecordRelativeOffset, int blobPropertiesRecordRelativeOffset,
        int updateRecordRelativeOffset, int userMetadataRecordRelativeOffset, int blobRecordRelativeOffset)
        throws MessageFormatException {
      // check constraints
      if (totalSize <= 0) {
        throw new MessageFormatException(
            "checkHeaderConstraints - totalSize " + totalSize + " needs to be greater than 0",
            MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (lifeVersion < 0) {
        throw new MessageFormatException(
            "checkHeaderConstraints - lifeVersion " + lifeVersion + " needs to be greater than or equal to 0",
            MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (blobPropertiesRecordRelativeOffset > 0 && (
          updateRecordRelativeOffset != Message_Header_Invalid_Relative_Offset || userMetadataRecordRelativeOffset <= 0
              || blobRecordRelativeOffset <= 0)) {
        throw new MessageFormatException(
            "checkHeaderConstraints - blobPropertiesRecordRelativeOffset is greater than 0 "
                + " but other properties do not satisfy constraints" + " blobPropertiesRecordRelativeOffset "
                + blobPropertiesRecordRelativeOffset + " updateRecordRelativeOffset " + updateRecordRelativeOffset
                + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
                + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }

      if (updateRecordRelativeOffset > 0 && (
          blobEncryptionKeyRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobPropertiesRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || userMetadataRecordRelativeOffset != Message_Header_Invalid_Relative_Offset
              || blobRecordRelativeOffset != Message_Header_Invalid_Relative_Offset)) {
        throw new MessageFormatException("checkHeaderConstraints - updateRecordRelativeOffset is greater than 0 "
            + " but other properties do not satisfy constraints" + " blobEncryptionKeyRelativeOffset "
            + blobEncryptionKeyRecordRelativeOffset + " blobPropertiesRecordRelativeOffset "
            + blobPropertiesRecordRelativeOffset + " updateRecordRelativeOffset " + updateRecordRelativeOffset
            + " userMetadataRecordRelativeOffset " + userMetadataRecordRelativeOffset + " blobRecordRelativeOffset "
            + blobRecordRelativeOffset, MessageFormatErrorCodes.Header_Constraint_Error);
      }
    }

    public MessageHeader_Format_V3(ByteBuffer input) {
      buffer = input;
    }

    @Override
    public short getVersion() {
      return buffer.getShort(0);
    }

    @Override
    public short getLifeVersion() {
      return buffer.getShort(Life_Version_Field_Offset_In_Bytes);
    }

    @Override
    public boolean hasLifeVersion() {
      return true;
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
    public int getUpdateRecordRelativeOffset() {
      return buffer.getInt(Update_Relative_Offset_Field_Offset_In_Bytes);
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
        return getUpdateRecordRelativeOffset();
      }
    }

    @Override
    public long getCrc() {
      return buffer.getLong(Crc_Field_Offset_In_Bytes);
    }

    @Override
    public void verifyHeader() throws MessageFormatException {
      verifyCrc();
      checkHeaderConstraints(getMessageSize(), getLifeVersion(), getBlobEncryptionKeyRecordRelativeOffset(),
          getBlobPropertiesRecordRelativeOffset(), getUpdateRecordRelativeOffset(),
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
   *  version         - The version of the update record
   *
   *  delete byte     - Takes value 0 or 1. If it is set to 1, it signifies that the blob is deleted. The field
   *                    is required to be able to support undelete in the future if required.
   *
   *  crc             - The crc of the update record
   *
   */
  public static class Update_Format_V1 {

    private static final int Delete_Field_Size_In_Bytes = 1;

    public static int getRecordSize() {
      return Version_Field_Size_In_Bytes + Delete_Field_Size_In_Bytes + Crc_Size;
    }

    public static void serialize(ByteBuffer outputBuffer, UpdateRecord updateRecord) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Update_Version_V1);
      outputBuffer.put((byte) 1);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    static UpdateRecord deserialize(CrcInputStream crcStream) throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      boolean isDeleted = dataStream.readByte() == 1;
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        throw new MessageFormatException(
            "update record data is corrupt. Expected CRC: " + expectedCRC + ", Actual CRC: " + actualCRC,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new UpdateRecord(UNKNOWN_ACCOUNT_ID, UNKNOWN_CONTAINER_ID, Utils.Infinite_Time, new DeleteSubRecord());
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |               |               |               |             |
   * | version |   AccountId   |  ContainerId  |  UpdateTime   |     Crc     |
   * |(2 bytes)|    (2 byte2)  |   (2 bytes)   |   (8 bytes)   |  (8 bytes)  |
   * |         |               |               |               |             |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version       - The version of the update record
   *
   *  AccountId     - AccountId that the blob belongs to
   *
   *  ContainerId   - ContainerId that the blob belongs to
   *
   *  UpdateTime    - Time of update in Ms
   *
   *  Crc           - The crc of the update record
   *
   */
  public static class Update_Format_V2 {

    private static final int ACCOUNT_ID_FIELD_SIZE_IN_BYTES = Short.BYTES;
    private static final int CONTAINER_ID_FIELD_SIZE_IN_BYTES = Short.BYTES;
    private static final int UPDATE_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;

    public static int getRecordSize() {
      return Version_Field_Size_In_Bytes + ACCOUNT_ID_FIELD_SIZE_IN_BYTES + CONTAINER_ID_FIELD_SIZE_IN_BYTES
          + UPDATE_TIME_FIELD_SIZE_IN_BYTES + Crc_Size;
    }

    public static void serialize(ByteBuffer outputBuffer, UpdateRecord updateRecord) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Update_Version_V2);
      outputBuffer.putShort(updateRecord.getAccountId());
      outputBuffer.putShort(updateRecord.getContainerId());
      outputBuffer.putLong(updateRecord.getUpdateTimeInMs());
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    static UpdateRecord deserialize(CrcInputStream crcStream) throws IOException, MessageFormatException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      short accountId = dataStream.readShort();
      short containerId = dataStream.readShort();
      long updateTimeInMs = dataStream.readLong();
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        throw new MessageFormatException(
            "update record data is corrupt. Expected CRC: " + expectedCRC + ", Actual CRC: " + actualCRC,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new UpdateRecord(accountId, containerId, updateTimeInMs, new DeleteSubRecord());
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - -
   * |         |               |               |               |             |            |
   * | version |   AccountId   |  ContainerId  |  UpdateTime   |  Sub record |    Crc     |
   * |(2 bytes)|    (2 byte2)  |   (2 bytes)   |   (8 bytes)   |  (n bytes)  |  (8 bytes) |
   * |         |               |               |               |             |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - -
   *  version       - The version of the update record
   *
   *  AccountId     - AccountId that the blob belongs to
   *
   *  ContainerId   - ContainerId that the blob belongs to
   *
   *  UpdateTime    - Time of update in Ms
   *
   *  Sub record    - the sub record related to this update
   *
   *  Crc           - The crc of the update record
   *
   */
  public static class Update_Format_V3 {

    private static final int RECORD_TYPE_FIELD_SIZE_IN_BYTES = Short.BYTES;
    private static final int ACCOUNT_ID_FIELD_SIZE_IN_BYTES = Short.BYTES;
    private static final int CONTAINER_ID_FIELD_SIZE_IN_BYTES = Short.BYTES;
    private static final int UPDATE_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;

    /**
     * @param type the type of the sub record in this update record.
     * @return the size of the record if the record were serialized
     */
    public static int getRecordSize(SubRecord.Type type) {
      int subRecordSize;
      switch (type) {
        case DELETE:
          subRecordSize = Delete_Sub_Format_V1.getRecordSize();
          break;
        case TTL_UPDATE:
          subRecordSize = Ttl_Update_Sub_Format_V1.getRecordSize();
          break;
        case UNDELETE:
          subRecordSize = Undelete_Sub_Format_V1.getRecordSize();
          break;
        default:
          throw new IllegalArgumentException("Unknown update record type: " + type);
      }
      return Version_Field_Size_In_Bytes + ACCOUNT_ID_FIELD_SIZE_IN_BYTES + CONTAINER_ID_FIELD_SIZE_IN_BYTES
          + UPDATE_TIME_FIELD_SIZE_IN_BYTES + RECORD_TYPE_FIELD_SIZE_IN_BYTES + subRecordSize + Crc_Size;
    }

    /**
     * Serializes {@code updateRecord} into {@code outputBuffer} in version 3.
     * @param outputBuffer the buffer to write the serialized bytes into.
     * @param updateRecord the {@link UpdateRecord} to serialize.
     */
    public static void serialize(ByteBuffer outputBuffer, UpdateRecord updateRecord) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Update_Version_V3);
      outputBuffer.putShort(updateRecord.getAccountId());
      outputBuffer.putShort(updateRecord.getContainerId());
      outputBuffer.putLong(updateRecord.getUpdateTimeInMs());
      outputBuffer.putShort((short) updateRecord.getType().ordinal());
      switch (updateRecord.getType()) {
        case DELETE:
          Delete_Sub_Format_V1.serialize(outputBuffer, updateRecord.getDeleteSubRecord());
          break;
        case TTL_UPDATE:
          Ttl_Update_Sub_Format_V1.serialize(outputBuffer, updateRecord.getTtlUpdateSubRecord());
          break;
        case UNDELETE:
          Undelete_Sub_Format_V1.serialize(outputBuffer, updateRecord.getUndeleteSubRecord());
          break;
        default:
          throw new IllegalArgumentException("Unknown update record type: " + updateRecord.getType());
      }
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getRecordSize(updateRecord.getType()) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    /**
     * @param crcStream the stream that contains the serialized form of an {@link UpdateRecord} of version 3.
     * @return the deserialized {@link UpdateRecord}
     * @throws IOException if there are problems reading from stream.
     * @throws MessageFormatException if the format of the message is unexpected.
     */
    static UpdateRecord deserialize(CrcInputStream crcStream) throws IOException, MessageFormatException {
      UpdateRecord updateRecord;
      DataInputStream dataStream = new DataInputStream(crcStream);
      short accountId = dataStream.readShort();
      short containerId = dataStream.readShort();
      long updateTimeInMs = dataStream.readLong();
      SubRecord.Type type = SubRecord.Type.values()[dataStream.readShort()];
      switch (type) {
        case DELETE:
          updateRecord = new UpdateRecord(accountId, containerId, updateTimeInMs, getDeleteSubRecord(dataStream));
          break;
        case TTL_UPDATE:
          updateRecord = new UpdateRecord(accountId, containerId, updateTimeInMs, getTtlUpdateSubRecord(dataStream));
          break;
        case UNDELETE:
          updateRecord = new UpdateRecord(accountId, containerId, updateTimeInMs, getUndeleteSubRecord(dataStream));
          break;
        default:
          throw new IllegalArgumentException("Unknown update record type: " + type);
      }
      long actualCRC = crcStream.getValue();
      long expectedCRC = dataStream.readLong();
      if (actualCRC != expectedCRC) {
        throw new MessageFormatException(
            "update record data is corrupt. Expected CRC: " + expectedCRC + ", Actual CRC: " + actualCRC,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return updateRecord;
    }

    /**
     * @param inputStream the stream that contains the serialized form of a {@link DeleteSubRecord}.
     * @return the deserialized {@link DeleteSubRecord}
     * @throws IOException if there are problems reading from stream.
     * @throws MessageFormatException if the format of the message is unexpected.
     */
    private static DeleteSubRecord getDeleteSubRecord(DataInputStream inputStream)
        throws IOException, MessageFormatException {
      short version = inputStream.readShort();
      switch (version) {
        case Delete_Subrecord_Version_V1:
          return Delete_Sub_Format_V1.deserialize(inputStream);
        default:
          throw new MessageFormatException("delete record version not supported: " + version,
              MessageFormatErrorCodes.Unknown_Format_Version);
      }
    }

    /**
     * @param inputStream the stream that contains the serialized form of a {@link UndeleteSubRecord}.
     * @return the deserialized {@link UndeleteSubRecord}
     * @throws IOException if there are problems reading from stream.
     * @throws MessageFormatException if the format of the message is unexpected.
     */
    private static UndeleteSubRecord getUndeleteSubRecord(DataInputStream inputStream)
        throws IOException, MessageFormatException {
      short version = inputStream.readShort();
      switch (version) {
        case Undelete_Subrecord_Version_V1:
          return Undelete_Sub_Format_V1.deserialize(inputStream);
        default:
          throw new MessageFormatException("undelete record version not supported: " + version,
              MessageFormatErrorCodes.Unknown_Format_Version);
      }
    }

    /**
     * @param inputStream the stream that contains the serialized form of a {@link TtlUpdateSubRecord}.
     * @return the deserialized {@link TtlUpdateSubRecord}
     * @throws IOException if there are problems reading from stream.
     * @throws MessageFormatException if the format of the message is unexpected.
     */
    private static TtlUpdateSubRecord getTtlUpdateSubRecord(DataInputStream inputStream)
        throws IOException, MessageFormatException {
      short version = inputStream.readShort();
      switch (version) {
        case Ttl_Update_Subrecord_Version_V1:
          return Ttl_Update_Sub_Format_V1.deserialize(inputStream);
        default:
          throw new MessageFormatException("ttl update record version not supported: " + version,
              MessageFormatErrorCodes.Unknown_Format_Version);
      }
    }
  }

  /**
   *  - - - - -
   * |         |
   * | version |
   * |(2 bytes)|
   * |         |
   *  - - - - -
   *  version         - The version of the delete record
   */
  private static class Delete_Sub_Format_V1 {

    static int getRecordSize() {
      return Version_Field_Size_In_Bytes;
    }

    static void serialize(ByteBuffer outputBuffer, DeleteSubRecord deleteSubRecord) {
      outputBuffer.putShort(Delete_Subrecord_Version_V1);
    }

    static DeleteSubRecord deserialize(DataInputStream stream) {
      return new DeleteSubRecord();
    }
  }

  /**
   *  - - - - -
   * |         |
   * | version |
   * |(2 bytes)|
   * |         |
   *  - - - - -
   *  version         - The version of the undelete record
   */
  private static class Undelete_Sub_Format_V1 {

    static int getRecordSize() {
      return Version_Field_Size_In_Bytes;
    }

    static void serialize(ByteBuffer outputBuffer, UndeleteSubRecord undeleteSubRecord) {
      outputBuffer.putShort(Undelete_Subrecord_Version_V1);
    }

    static UndeleteSubRecord deserialize(DataInputStream stream) {
      return new UndeleteSubRecord();
    }
  }

  /**
   *  - - - - - - - - - - - - - - -
   * |         |                   |
   * | version | UpdatedExpiryTime |
   * |(2 bytes)|   (8 bytes)       |
   * |         |                   |
   *  - - - - - - - - - - - - - - -
   *  version         - The version of the ttl update record
   *
   *  UpdatedExpiryTime  - Time in ms to which the expiry time was updated
   *
   */
  private static class Ttl_Update_Sub_Format_V1 {

    private static final int EXPIRE_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;

    static int getRecordSize() {
      return Version_Field_Size_In_Bytes + EXPIRE_TIME_FIELD_SIZE_IN_BYTES;
    }

    static void serialize(ByteBuffer outputBuffer, TtlUpdateSubRecord ttlUpdateSubRecord) {
      outputBuffer.putShort(Ttl_Update_Subrecord_Version_V1);
      outputBuffer.putLong(ttlUpdateSubRecord.getUpdatedExpiryTimeMs());
    }

    static TtlUpdateSubRecord deserialize(DataInputStream stream) throws IOException {
      long updatedExpiryTimeMs = stream.readLong();
      return new TtlUpdateSubRecord(updatedExpiryTimeMs);
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
      ByteBuf byteBuf = Utils.readNettyByteBufFromCrcInputStream(crcStream, (int) dataSize);
      long crc = crcStream.getValue();
      long streamCrc = dataStream.readLong();
      if (crc != streamCrc) {
        logger.error("corrupt data while parsing blob content expectedcrc {} actualcrc {}", crc, streamCrc);
        throw new MessageFormatException("corrupt data while parsing blob content",
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new BlobData(BlobType.DataBlob, dataSize, byteBuf);
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
      ByteBuf byteBuf = Utils.readNettyByteBufFromCrcInputStream(crcStream, (int) dataSize);
      long crc = crcStream.getValue();
      long streamCrc = dataStream.readLong();
      if (crc != streamCrc) {
        logger.error("corrupt data while parsing blob content expectedcrc {} actualcrc {}", crc, streamCrc);
        throw new MessageFormatException("corrupt data while parsing blob content",
            MessageFormatErrorCodes.Data_Corrupt);
      }
      return new BlobData(blobContentType, dataSize, byteBuf);
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
     * @throws IOException if stream data is not in correct format.
     */
    public static CompositeBlobInfo deserializeMetadataContentRecord(DataInputStream stream,
        StoreKeyFactory storeKeyFactory) throws IOException {
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

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |              |            |            |          |            |            |          |
   * | version |  total size  | # of keys  |  size of   |   key1   |  size of   |    key2    |  ......  |
   * |(2 bytes)|  (8 bytes)   | (4 bytes)  | key1 blob  |          | key2 blob  |            |  ......  |
   * |         |              |            | (8 bytes)  |          | (8 bytes)  |            |          |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version           - The version of the metadata content record
   *
   *  total size        - total size of the object this metadata describes.
   *
   *  # of keys         - number of keys in the metadata blob
   *
   *  size of key1 blob - size of the data referenced by key1
   *
   *  key1              - first key to be part of metadata blob
   *
   *  size of key2 blob - size of the data referenced by key2
   *
   *  key2              - second key to be part of metadata blob
   *
   */
  public static class Metadata_Content_Format_V3 {
    private static final int NUM_OF_KEYS_FIELD_SIZE_IN_BYTES = 4;
    private static final int SIZE_OF_BLOB_FIELD_SIZE_IN_BYTES = 8;
    private static final int TOTAL_SIZE_FIELD_SIZE_IN_BYTES = 8;

    /**
     * Get the total size of the metadata content record.
     * @param keySize The size of each key in bytes.
     * @param numberOfKeys The total number of keys.
     * @return The total size in bytes.
     */
    public static int getMetadataContentSize(int keySize, int numberOfKeys) {
      return Version_Field_Size_In_Bytes + TOTAL_SIZE_FIELD_SIZE_IN_BYTES + NUM_OF_KEYS_FIELD_SIZE_IN_BYTES
          + numberOfKeys * (keySize + SIZE_OF_BLOB_FIELD_SIZE_IN_BYTES);
    }

    /**
     * Serialize a metadata content record.
     * @param outputBuffer output buffer of the serialized metadata content
     * @param totalSize total size of the blob data content
     * @param keysAndContentSizes list of data blob keys referenced by the metadata
     *                            blob along with the data content sizes of each data blob
     */
    public static void serializeMetadataContentRecord(ByteBuffer outputBuffer, long totalSize,
        List<Pair<StoreKey, Long>> keysAndContentSizes) {
      int keySize = keysAndContentSizes.get(0).getFirst().sizeInBytes();
      outputBuffer.putShort(Metadata_Content_Version_V3);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(keysAndContentSizes.size());
      long sum = 0;
      for (Pair<StoreKey, Long> keyAndContentSize : keysAndContentSizes) {
        if (keyAndContentSize.getFirst().sizeInBytes() != keySize) {
          throw new IllegalArgumentException("Keys are not of same size");
        }
        outputBuffer.putLong(keyAndContentSize.getSecond());
        outputBuffer.put(keyAndContentSize.getFirst().toBytes());
        sum += keyAndContentSize.getSecond();
      }
      if (sum != totalSize) {
        throw new IllegalArgumentException("Key content sizes do not equal total size");
      }
    }

    /**
     * Deserialize a metadata content record from a stream.
     * @param stream The stream to read the serialized record from.
     * @param storeKeyFactory The factory to use for parsing keys in the serialized metadata content record.
     * @return A {@link CompositeBlobInfo} object with the chunk size and list of keys from the record.
     * @throws IOException
     */
    public static CompositeBlobInfo deserializeMetadataContentRecord(DataInputStream stream,
        StoreKeyFactory storeKeyFactory) throws IOException {
      List<Pair<StoreKey, Long>> keysAndContentSizes = new ArrayList<>();
      long totalSize = stream.readLong();
      long sum = 0;
      int numberOfKeys = stream.readInt();
      for (int i = 0; i < numberOfKeys; i++) {
        long contentSize = stream.readLong();
        StoreKey storeKey = storeKeyFactory.getStoreKey(stream);
        keysAndContentSizes.add(new Pair<>(storeKey, contentSize));
        sum += contentSize;
      }
      if (sum != totalSize) {
        throw new IllegalArgumentException("Key content sizes do not equal total size");
      }
      return new CompositeBlobInfo(keysAndContentSizes);
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
