package com.github.ambry.messageformat;

import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


// A class that derives from blob properties. It is mainly used to set
// properties in BlobProperties that are not user editable
class SystemMetadata extends BlobProperties {
  public SystemMetadata(long blobSize, String serviceId, String ownerId, String contentType, boolean isPrivate,
      long timeToLiveInSeconds, long creationTime) {
    super(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds);
    this.creationTimeInMs = creationTime;
  }
}

/**
 * Serializes and deserializes BlobProperties
 */
public class BlobPropertiesSerDe {

  private static final short Version1 = 1;
  private static final int Version_Field_Size_In_Bytes = 2;
  private static final int TTL_Field_Size_In_Bytes = 8;
  private static final int Private_Field_Size_In_Bytes = 1;
  private static final int CreationTime_Field_Size_In_Bytes = 8;
  private static final int Variable_Field_Size_In_Bytes = 4;
  private static final int BlobSize_Field_Size_In_Bytes = 8;

  public static int getBlobPropertiesSize(BlobProperties properties) {
    return Version_Field_Size_In_Bytes +
        TTL_Field_Size_In_Bytes +
        Private_Field_Size_In_Bytes +
        CreationTime_Field_Size_In_Bytes +
        BlobSize_Field_Size_In_Bytes +
        Variable_Field_Size_In_Bytes + Utils.getNullableStringLength(properties.getContentType()) +
        Variable_Field_Size_In_Bytes + Utils.getNullableStringLength(properties.getOwnerId()) +
        Variable_Field_Size_In_Bytes + Utils.getNullableStringLength(properties.getServiceId());
  }

  public static BlobProperties getBlobPropertiesFromStream(DataInputStream stream)
      throws IOException {
    long version = stream.readShort();
    if (version == Version1) {
      long ttl = stream.readLong();
      boolean isPrivate = stream.readByte() == 1 ? true : false;
      long creationTime = stream.readLong();
      long blobSize = stream.readLong();
      String contentType = Utils.readIntString(stream);
      String ownerId = Utils.readIntString(stream);
      String serviceId = Utils.readIntString(stream);
      return new SystemMetadata(blobSize, serviceId, ownerId, contentType, isPrivate, ttl, creationTime);
    } else {
      throw new IllegalArgumentException("stream has unknown blob property version " + version);
    }
  }

  public static void putBlobPropertiesToBuffer(ByteBuffer outputBuffer, BlobProperties properties) {
    outputBuffer.putShort(Version1);
    outputBuffer.putLong(properties.getTimeToLiveInSeconds());
    outputBuffer.put(properties.isPrivate() ? (byte) 1 : (byte) 0);
    outputBuffer.putLong(properties.getCreationTimeInMs());
    outputBuffer.putLong(properties.getBlobSize());
    Utils.serializeNullableString(outputBuffer, properties.getContentType());
    Utils.serializeNullableString(outputBuffer, properties.getOwnerId());
    Utils.serializeNullableString(outputBuffer, properties.getServiceId());
  }
}
