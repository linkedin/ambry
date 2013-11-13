package com.github.ambry.messageformat;

import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.io.InputStream;
import java.io.IOException;

/**
 * Represents the message format of the data that gets written to the store
 */
public class MessageFormat {

  // Common info for all formats
  public static final int Version_Field_Size_In_Bytes = 2;
  public static final int Crc_Size = 8;

  public static final int Header_Current_Version = 1;
  public static final int SystemMetadata_Current_Version = 1;
  public static final int UserMetadata_Current_Version = 1;
  public static final int Data_Current_Version = 1;

  public static final short Message_Header_Version_V1 = 1;
  public static final short SystemMetadata_Version_V1 = 1;
  public static final short UserMetadata_Version_V1 = 1;
  public static final short Data_Version_V1 = 1;
  public static final int Message_Header_Invalid_Relative_Offset = -1;

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
    UserMetadata_Format_V1.serializeUserMetadata(outputBuffer, userMetadata);
  }

  // We only serialize partial data for blobs. This is to support streaming serialization of data.
  public static void  serializeCurrentVersionPartialData(ByteBuffer outputBuffer, long dataSize) {
    Data_Format_V1.serializePartialData(outputBuffer, dataSize);
  }


  // Deserialization methods for all data types

  public static BlobProperties deserializeBlobProperties(InputStream stream) throws IOException, DataCorruptException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case SystemMetadata_Version_V1:
        return SystemMetadata_Format_V1.deserializeBlobProperties(crcStream);
      default:
        throw new DataCorruptException("blob property version not supported");
    }
  }

  public static boolean deserializeDeleteRecord(InputStream stream) throws IOException, DataCorruptException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case SystemMetadata_Version_V1:
        return SystemMetadata_Format_V1.deserializeDeleteRecord(crcStream);
      default:
        throw new DataCorruptException("delete record version not supported");
    }
  }

  public static long deserializeTTLRecord(InputStream stream) throws IOException, DataCorruptException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case SystemMetadata_Version_V1:
        return SystemMetadata_Format_V1.deserializeTTLRecord(crcStream);
      default:
        throw new DataCorruptException("ttl record version not supported");
    }
  }

  public static ByteBuffer deserializeMetadata(InputStream stream) throws IOException, DataCorruptException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case UserMetadata_Version_V1:
        return UserMetadata_Format_V1.deserializeUserMetadata(crcStream);
      default:
        throw new DataCorruptException("ttl record version not supported");
    }
  }

  public static ByteBufferInputStream deserializeData(InputStream stream) throws IOException, DataCorruptException {
    CrcInputStream crcStream = new CrcInputStream(stream);
    DataInputStream inputStream = new DataInputStream(crcStream);
    short version = inputStream.readShort();
    switch (version) {
      case Data_Version_V1:
        return Data_Format_V1.deserializeData(crcStream);
      default:
        throw new DataCorruptException("ttl record version not supported");
    }
  }




  public static class MessageHeader_Format_V1 {
    ByteBuffer buffer;

    public static final int Total_Size_Field_Offset_In_Bytes = Version_Field_Size_In_Bytes;

    public static final int Total_Size_Field_Size_In_Bytes = 8;

    public static final int SystemMetadata_Relative_Offset_Field_Offset_In_Bytes =
            Total_Size_Field_Offset_In_Bytes + Total_Size_Field_Size_In_Bytes;

    public static final int Relative_Offset_Field_Sizes_In_Bytes = 4;

    private static final int Number_Of_Relative_Offset_Fields = 3;

    public static final int UserMetadata_Relative_Offset_Field_Offset_In_Bytes =
            SystemMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static final int Data_Relative_Offset_Field_Offset_In_Bytes =
            UserMetadata_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static final int Crc_Field_Offset_In_Bytes =
            Data_Relative_Offset_Field_Offset_In_Bytes + Relative_Offset_Field_Sizes_In_Bytes;

    public static int getHeaderSize() {
      return Version_Field_Size_In_Bytes +
             Total_Size_Field_Size_In_Bytes +
             (Number_Of_Relative_Offset_Fields * Relative_Offset_Field_Sizes_In_Bytes) +
             Crc_Size;
    }

    public static void serializeHeader(ByteBuffer outputBuffer,
                                       long totalSize,
                                       int systemMetadataRelativeOffset,
                                       int userMetadataRelativeOffset,
                                       int dataRelativeOffset) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(Message_Header_Version_V1);
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

  public static enum SystemMetadataRecordType {
    BlobPropertyRecord,
    DeleteRecord,
    TTLRecord
  }

  public static class SystemMetadata_Format_V1 {

    public static final int Delete_Field_Size_In_Bytes = 1;
    public static final short SystemMetadata_Type_Field_Size_In_Bytes = 2;
    private static Logger logger = LoggerFactory.getLogger(SystemMetadata_Format_V1.class);

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
      outputBuffer.putShort(SystemMetadata_Version_V1);
      outputBuffer.putShort((short)SystemMetadataRecordType.BlobPropertyRecord.ordinal());
      BlobPropertySerDe.putBlobPropertyToBuffer(outputBuffer, properties);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getBlobPropertyRecordSize(properties) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static void serializeDeleteRecord(ByteBuffer outputBuffer, boolean deleteFlag) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(SystemMetadata_Version_V1);
      outputBuffer.putShort((short)SystemMetadataRecordType.DeleteRecord.ordinal());
      outputBuffer.put(deleteFlag ? (byte) 1 : (byte) 0);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getDeleteRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static void serializeTTLRecord(ByteBuffer outputBuffer, long ttl) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(SystemMetadata_Version_V1);
      outputBuffer.putShort((short)SystemMetadataRecordType.TTLRecord.ordinal());
      outputBuffer.putLong(ttl);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getTTLRecordSize() - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static BlobProperties deserializeBlobProperties(CrcInputStream crcStream) throws IOException, DataCorruptException {
      try {
        DataInputStream dataStream = new DataInputStream(crcStream);
        SystemMetadataRecordType type = SystemMetadataRecordType.values()[dataStream.readShort()];

        // ensure the type fields match
        if (type != SystemMetadataRecordType.BlobPropertyRecord)
          throw new IllegalArgumentException("Invalid system metadata type");
        BlobProperties properties = BlobPropertySerDe.getBlobPropertyFromStream(dataStream);
        long crc = crcStream.getValue();
        if (crc != dataStream.readLong()) {
          logger.error("corrupt data while parsing blob properties");
          throw new DataCorruptException("Blob property data is corrupt");
        }
        return properties;
      }
      catch (Exception e) {
        logger.error("Blob property failed to be parsed. Data may be corrupt with exception {}", e);
        throw new DataCorruptException("Blob property failed to be parsed. Data may be corrupt");
      }
    }

    public static boolean deserializeDeleteRecord(CrcInputStream crcStream) throws IOException, DataCorruptException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      SystemMetadataRecordType type = SystemMetadataRecordType.values()[dataStream.readShort()];

      // ensure the type fields match
      if (type != SystemMetadataRecordType.DeleteRecord)
        throw new IllegalArgumentException("Invalid system metadata type");
      boolean isDeleted = dataStream.readByte() == 1 ? true : false;
      long crc = crcStream.getValue();
      if (crc != dataStream.readLong()) {
        logger.error("corrupt data while parsing blob properties");
        throw new DataCorruptException("Blob property data is corrupt");
      }
      return isDeleted;
    }

    public static long deserializeTTLRecord(CrcInputStream crcStream) throws IOException, DataCorruptException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      SystemMetadataRecordType type = SystemMetadataRecordType.values()[dataStream.readShort()];

      // ensure the type fields match
      if (type != SystemMetadataRecordType.TTLRecord)
        throw new IllegalArgumentException("Invalid system metadata type");
      long ttl = dataStream.readLong();
      long crc = crcStream.getValue();
      if (crc != dataStream.readLong()) {
        logger.error("corrupt data while parsing blob properties");
        throw new DataCorruptException("Blob property data is corrupt");
      }
      return ttl;
    }
  }

  public static class UserMetadata_Format_V1 {
    public static final int UserMetadata_Size_Field_In_Bytes = 4;
    private static Logger logger = LoggerFactory.getLogger(UserMetadata_Format_V1.class);

    public static int getUserMetadataSize(ByteBuffer userMetadata) {
      return Version_Field_Size_In_Bytes +
             UserMetadata_Size_Field_In_Bytes +
             userMetadata.limit() +
             Crc_Size;
    }

    public static void serializeUserMetadata(ByteBuffer outputBuffer, ByteBuffer userMetadata) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(UserMetadata_Version_V1);
      outputBuffer.putInt(userMetadata.limit());
      outputBuffer.put(userMetadata);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, getUserMetadataSize(userMetadata) - Crc_Size);
      outputBuffer.putLong(crc.getValue());
    }

    public static ByteBuffer deserializeUserMetadata(CrcInputStream crcStream) throws IOException, DataCorruptException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      int usermetadataSize = dataStream.readInt();
      byte[] userMetadaBuffer = new byte[usermetadataSize];
      dataStream.read(userMetadaBuffer);
      long crc = crcStream.getValue();
      if (crc != dataStream.readLong()) {
        logger.error("corrupt data while parsing user metadata");
        throw new DataCorruptException("User metadata is corrupt");
      }
      return ByteBuffer.wrap(userMetadaBuffer);
    }
  }

  public static class Data_Format_V1 {
    public static final int Data_Size_Field_In_Bytes = 8;
    private static Logger logger = LoggerFactory.getLogger(Data_Format_V1.class);

    public static long getDataSize(long dataSize) {
      return Version_Field_Size_In_Bytes +
             Data_Size_Field_In_Bytes +
             dataSize +
             Crc_Size;
    }

    public static void serializePartialData(ByteBuffer outputBuffer, long dataSize) {
      outputBuffer.putShort(UserMetadata_Version_V1);
      outputBuffer.putLong(dataSize);
    }

    public static ByteBufferInputStream deserializeData(CrcInputStream crcStream) throws IOException, DataCorruptException {
      DataInputStream dataStream = new DataInputStream(crcStream);
      long dataSize = dataStream.readLong();
      // we only support data of max size = MAX_INT for now
      ByteBufferInputStream output = new ByteBufferInputStream(dataStream, (int)dataSize);
      long crc = crcStream.getValue();
      if (crc != dataStream.readLong()) {
        logger.error("corrupt data while parsing user metadata");
        throw new DataCorruptException("User metadata is corrupt");
      }
      return output;
    }
  }
}
