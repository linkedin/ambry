package com.github.ambry.messageformat;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.io.IOException;

/**
 * Serializes and deserializes BlobProperties
 */
public class BlobPropertySerDe {

  public static final int TTL_Field_Size_In_Bytes = 8;
  public static final int Private_Field_Size_In_Bytes = 1;
  public static final int CreationTime_Field_Size_In_Bytes = 8;
  public static final int Variable_Field_Size_In_Bytes = 4;
  public static final int BlobSize_Field_Size_In_Bytes = 8;

  public static int getBlobPropertySize(BlobProperties properties) {
    return TTL_Field_Size_In_Bytes +
           Private_Field_Size_In_Bytes +
           CreationTime_Field_Size_In_Bytes +
           BlobSize_Field_Size_In_Bytes +
           Variable_Field_Size_In_Bytes + (properties.getContentType() == null ? 0 : properties.getContentType().length()) +
           Variable_Field_Size_In_Bytes + (properties.getMemberId() == null ? 0 : properties.getMemberId().length()) +
           Variable_Field_Size_In_Bytes + (properties.getParentBlobId() == null ? 0 : properties.getParentBlobId().length()) +
           Variable_Field_Size_In_Bytes + (properties.getServiceId() == null ? 0 : properties.getServiceId().length());
  }

  public static BlobProperties getBlobPropertyFromStream(DataInputStream stream) throws IOException {
    long ttl = stream.readLong();
    boolean isPrivate = stream.readByte() == 1 ? true : false;
    long creationTime = stream.readLong();
    long blobSize = stream.readLong();
    String contentType = Utils.readIntString(stream);
    String memberId = Utils.readIntString(stream);
    String parentBlobId = Utils.readIntString(stream);
    String serviceId = Utils.readIntString(stream);
    return new BlobProperties(ttl, isPrivate, contentType, memberId, parentBlobId, blobSize, serviceId, creationTime);
  }

  public static void putBlobPropertyToBuffer(ByteBuffer outputBuffer, BlobProperties properties) {
    outputBuffer.putLong(properties.getTimeToLive());
    outputBuffer.put(properties.isPrivate() ? (byte) 1 : (byte) 0);
    outputBuffer.putLong(properties.getCreationTime());
    outputBuffer.putLong(properties.getBlobSize());
    serializeNullableString(outputBuffer, properties.getContentType());
    serializeNullableString(outputBuffer, properties.getMemberId());
    serializeNullableString(outputBuffer, properties.getParentBlobId());
    serializeNullableString(outputBuffer, properties.getServiceId());
  }

  private static void serializeNullableString(ByteBuffer outputBuffer, String value) {
    if (value == null)
      outputBuffer.putInt(0);
    else {
      outputBuffer.putInt(value.length());
      outputBuffer.put(value.getBytes());
    }
  }
}
