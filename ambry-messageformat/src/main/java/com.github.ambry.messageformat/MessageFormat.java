package com.github.ambry.messageformat;

import com.github.ambry.utils.Crc32;
import java.nio.ByteBuffer;

/**
 * Represents the message format of the data that gets written to the store
 */
public class MessageFormat {

  // Common info for all formats
  public static final int Version_Field_Size_In_Bytes = 2;
  public static final int Crc_Size = 8;
  public static final short Message_Header_Version_V1 = 1;

  // Methods below defines the write format for the current version. W.r.t write there is always only one version,
  // the most recent one

  // Size methods for the current version
  public static int getCurrentVersionHeaderSize() {
    return MessageHeader_Format_V1.getHeaderSize();
  }

  public static int getCurrentVersionBlobPropertyRecordSize(BlobProperties properties) {
    return SystemMetadata_Format_V1.getBlobPropertyRecordSize(properties);
  }

  public static int getCurrentVersionDeleteRecordSize() {
    return SystemMetadata_Format_V1.getDeleteRecordSize();
  }

  public static int getCurrentVersionTTLRecordSize() {
    return SystemMetadata_Format_V1.getTTLRecordSize();
  }

  public static int getCurrentVersionUserMetadataSize(ByteBuffer userMetadata) {
    return UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
  }

  public static long getCurrentVersionDataSize(long dataSize) {
    return Data_Format_V1.getDataSize(dataSize);
  }

  // Serialization methods for the current version

  public static void serializeCurrentVersionHeader(ByteBuffer outputBuffer,
                                                   long totalSize,
                                                   int systemMetadataRelativeOffset,
                                                   int userMetadataRelativeOffset,
                                                   int dataRelativeOffset) {
    MessageHeader_Format_V1.serializeHeader(outputBuffer, totalSize, systemMetadataRelativeOffset,
                                            userMetadataRelativeOffset, dataRelativeOffset);
  }


  public static void serializeCurrentVersionBlobPropertyRecord(ByteBuffer outputBuffer, BlobProperties properties) {
    SystemMetadata_Format_V1.serializeBlobPropertyRecord(outputBuffer, properties);
  }

  public static void serializeCurrentVersionDeleteRecord(ByteBuffer outputBuffer, boolean deleteFlag) {
    SystemMetadata_Format_V1.serializeDeleteRecord(outputBuffer, deleteFlag);
  }

  public static void serializeCurrentVersionTTLRecord(ByteBuffer outputBuffer, long ttlValue) {
    SystemMetadata_Format_V1.serializeTTLRecord(outputBuffer, ttlValue);
  }

  public static void serializeCurrentVersionUserMetadata(ByteBuffer outputBuffer, ByteBuffer userMetadata) {
    UserMetadata_Format_V1.serializeData(outputBuffer, userMetadata);
  }

  public static void  serializeCurrentVersionPartialData(ByteBuffer outputBuffer, long dataSize) {
    Data_Format_V1.serializePartialData(outputBuffer, dataSize);
  }


  public static class MessageHeader_Format_V1 {
    ByteBuffer buffer;

    public static final int Total_Size_Field_Offset_In_Bytes = Version_Field_Size_In_Bytes;

    public static final int Total_Size_Field_Size_In_Bytes = 8;

    public static final int SystemMetadata_Relative_Offset_Field_Offset_In_Bytes =
            Total_Size_Field_Offset_In_Bytes + Total_Size_Field_Size_In_Bytes;

    public static final int Relative_Offset_Field_Sizes_In_Bytes = 4;

    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
            SystemMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static final int Data_Relative_Offset_Field_Offset_In_Bytes =
            UserMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static final int Crc_Field_Offset_In_Bytes =
            Data_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static int getHeaderSize() {
      return Version_Field_Size_In_Bytes +
             Total_Size_Field_Size_In_Bytes +
             (3 * Relative_Offset_Field_Sizes_In_Bytes) +
             Crc_Size;
    }

    public static void serializeHeader(ByteBuffer outputBuffer,
                                       long totalSize,
                                       int systemMetadataRelativeOffset,
                                       int userMetadataRelativeOffset,
                                       int dataRelativeOffset) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort((short)1);
      outputBuffer.putLong(totalSize);
      outputBuffer.putInt(systemMetadataRelativeOffset);
      outputBuffer.putInt(userMetadataRelativeOffset);
      outputBuffer.putInt(dataRelativeOffset);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getHeaderSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }


    public MessageHeader_Format_V1(ByteBuffer input) {
      this.buffer = input;
    }

    public short getVersion() {
      return buffer.getShort(0);
    }

    public long getMessageSize() {
      return buffer.getLong(Total_Size_Field_Offset_In_Bytes);
    }

    public int getSystemMetadataRelativeOffset() {
      return buffer.getInt(SystemMetadata_Relative_Offset_Field_Offset_In_Bytes);
    }

    public int getUserMetadataRelativeOffset() {
      return buffer.getInt(UserMetadata_Relative_Offset_Field_Offset_In_Bytes);
    }

    public int getDataRelativeOffset() {
      return buffer.getInt(Data_Relative_Offset_Field_Offset_In_Bytes);
    }

    public long getCrc() {
      return buffer.getLong(Crc_Field_Offset_In_Bytes);
    }
  }

  public static enum SystemMetadataType {
    BlobPropertyRecord,
    DeleteRecord,
    TTLRecord
  }

  public static class SystemMetadata_Format_V1 {

    public static final int Delete_Field_Size_In_Bytes = 1;
    public static final short SystemMetadata_Type_Field_Size_In_Bytes = 2;

    public static int getBlobPropertyRecordSize(BlobProperties properties) {
      return Version_Field_Size_In_Bytes +
             SystemMetadata_Type_Field_Size_In_Bytes +
             BlobPropertySerDe.getBlobPropertySize(properties) +
             Crc_Size;
    }

    public static int getDeleteRecordSize() {
      return Version_Field_Size_In_Bytes +
             SystemMetadata_Type_Field_Size_In_Bytes +
             Delete_Field_Size_In_Bytes +
             Crc_Size;
    }

    public static int getTTLRecordSize() {
      return Version_Field_Size_In_Bytes +
             SystemMetadata_Type_Field_Size_In_Bytes +
             BlobPropertySerDe.TTL_Field_Size_In_Bytes +
             Crc_Size;
    }

    public static void serializeBlobPropertyRecord(ByteBuffer outputBuffer, BlobProperties properties) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort((short)1);
      outputBuffer.putShort((short)SystemMetadataType.BlobPropertyRecord.ordinal());
      BlobPropertySerDe.putBlobPropertyToBuffer(outputBuffer, properties);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getBlobPropertyRecordSize(properties) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static void serializeDeleteRecord(ByteBuffer outputBuffer, boolean deleteFlag) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort((short)1);
      outputBuffer.putShort((short)SystemMetadataType.DeleteRecord.ordinal());
      outputBuffer.put(deleteFlag ? (byte) 1 : (byte) 0);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getDeleteRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static void serializeTTLRecord(ByteBuffer outputBuffer, long ttl) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort((short)1);
      outputBuffer.putShort((short)SystemMetadataType.TTLRecord.ordinal());
      outputBuffer.putLong(ttl);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getTTLRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }
  }

  public static class UserMetadata_Format_V1 {
    public static final int UserMetadata_Size_Field_In_Bytes = 4;

    public static int getUserMetadataSize(ByteBuffer userMetadata) {
      return Version_Field_Size_In_Bytes +
             UserMetadata_Size_Field_In_Bytes +
             userMetadata.limit() +
             Crc_Size;
    }

    public static void serializeData(ByteBuffer outputBuffer, ByteBuffer userMetadata) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort((short)1);
      outputBuffer.putInt(userMetadata.limit());
      outputBuffer.put(userMetadata);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getUserMetadataSize(userMetadata) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }
  }

  public static class Data_Format_V1 {
    public static final int Data_Size_Field_In_Bytes = 8;

    public static long getDataSize(long dataSize) {
      return Version_Field_Size_In_Bytes +
             Data_Size_Field_In_Bytes +
             dataSize +
             Crc_Size;
    }

    public static void serializePartialData(ByteBuffer outputBuffer, long dataSize) {
      outputBuffer.putShort((short)1);
      outputBuffer.putLong(dataSize);
    }
  }
}
