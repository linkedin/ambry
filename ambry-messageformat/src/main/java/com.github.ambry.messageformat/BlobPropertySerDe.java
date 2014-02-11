package com.github.ambry.messageformat;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.io.IOException;

// A class that derives from blob properties. It is mainly used to set
// properties in BlobProperties that are not user editable
class SystemMetadata extends BlobProperties {
  public SystemMetadata(long blobSize,
                        String serviceId,
                        String ownerId,
                        String contentType,
                        boolean isPrivate,
                        long timeToLiveInMs,
                        int userMetadataSize,
                        long creationTime) {
    super(blobSize, serviceId, ownerId, contentType, isPrivate, userMetadataSize, timeToLiveInMs);
    this.creationTimeInMs = creationTime;
  }
}
/**
 * Serializes and deserializes BlobProperties
 */
public class BlobPropertySerDe {

  public static final int TTL_Field_Size_In_Bytes = 8;
  public static final int Private_Field_Size_In_Bytes = 1;
  public static final int CreationTime_Field_Size_In_Bytes = 8;
  public static final int Variable_Field_Size_In_Bytes = 4;
  public static final int BlobSize_Field_Size_In_Bytes = 8;
  public static final int UserMetadataSize_Field_Size_In_Bytes = 4;

  public static int getBlobPropertySize(BlobProperties properties) {
    return TTL_Field_Size_In_Bytes +
            Private_Field_Size_In_Bytes +
            CreationTime_Field_Size_In_Bytes +
            BlobSize_Field_Size_In_Bytes +
            UserMetadataSize_Field_Size_In_Bytes +
            Variable_Field_Size_In_Bytes + Utils.getNullableStringLength(properties.getContentType()) +
            Variable_Field_Size_In_Bytes + Utils.getNullableStringLength(properties.getOwnerId()) +
            Variable_Field_Size_In_Bytes + Utils.getNullableStringLength(properties.getServiceId());
  }

  public static BlobProperties getBlobPropertyFromStream(DataInputStream stream) throws IOException {
    long ttl = stream.readLong();
    boolean isPrivate = stream.readByte() == 1 ? true : false;
    long creationTime = stream.readLong();
    long blobSize = stream.readLong();
    int userMetadataSize = stream.readInt();
    String contentType = Utils.readIntString(stream);
    String ownerId = Utils.readIntString(stream);
    String serviceId = Utils.readIntString(stream);
    return new SystemMetadata(blobSize, serviceId, ownerId, contentType, isPrivate, ttl, userMetadataSize, creationTime);
  }

  public static void putBlobPropertyToBuffer(ByteBuffer outputBuffer, BlobProperties properties) {
    outputBuffer.putLong(properties.getTimeToLiveInMs());
    outputBuffer.put(properties.isPrivate() ? (byte) 1 : (byte) 0);
    outputBuffer.putLong(properties.getCreationTimeInMs());
    outputBuffer.putLong(properties.getBlobSize());
    outputBuffer.putInt(properties.getUserMetadataSize());
    Utils.serializeNullableString(outputBuffer, properties.getContentType());
    Utils.serializeNullableString(outputBuffer, properties.getOwnerId());
    Utils.serializeNullableString(outputBuffer, properties.getServiceId());
  }
}