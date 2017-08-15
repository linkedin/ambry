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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Serializes and deserializes BlobProperties
 */
public class BlobPropertiesSerDe {

  static final short Version1 = 1;
  static final short Version2 = 2;
  private static final short currentVersion = Version2;
  private static final int Version_Field_Size_In_Bytes = 2;
  private static final int TTL_Field_Size_In_Bytes = 8;
  private static final int Private_Field_Size_In_Bytes = 1;
  private static final int CreationTime_Field_Size_In_Bytes = 8;
  private static final int Variable_Field_Size_In_Bytes = 4;
  private static final int BlobSize_Field_Size_In_Bytes = 8;

  public static int getBlobPropertiesSerDeSize(BlobProperties properties) {
    return Version_Field_Size_In_Bytes + TTL_Field_Size_In_Bytes + Private_Field_Size_In_Bytes
        + CreationTime_Field_Size_In_Bytes + BlobSize_Field_Size_In_Bytes + Variable_Field_Size_In_Bytes
        + Utils.getNullableStringLength(properties.getContentType()) + Variable_Field_Size_In_Bytes
        + Utils.getNullableStringLength(properties.getOwnerId()) + Variable_Field_Size_In_Bytes
        + Utils.getNullableStringLength(properties.getServiceId()) + Short.BYTES + Short.BYTES;
  }

  public static BlobProperties getBlobPropertiesFromStream(DataInputStream stream) throws IOException {
    short version = stream.readShort();
    BlobProperties toReturn;
    long ttl = stream.readLong();
    boolean isPrivate = stream.readByte() == 1;
    long creationTime = stream.readLong();
    long blobSize = stream.readLong();
    String contentType = Utils.readIntString(stream);
    String ownerId = Utils.readIntString(stream);
    String serviceId = Utils.readIntString(stream);
    switch (version) {
      case Version1:
        toReturn = new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, ttl, creationTime,
            Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID);
        break;
      case Version2:
        short accountId = stream.readShort();
        short containerId = stream.readShort();
        toReturn =
            new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, ttl, creationTime, accountId,
                containerId);
        break;
      default:
        throw new IllegalArgumentException("stream has unknown blob property version " + version);
    }
    return toReturn;
  }

  /**
   * Serialize {@link BlobProperties} to buffer in the {@link #currentVersion}
   * @param outputBuffer the {@link ByteBuffer} to which {@link BlobProperties} needs to be serialized
   * @param properties the {@link BlobProperties} that needs to be serialized
   */
  public static void serializeBlobProperties(ByteBuffer outputBuffer, BlobProperties properties) {
    outputBuffer.putShort(currentVersion);
    outputBuffer.putLong(properties.getTimeToLiveInSeconds());
    outputBuffer.put(properties.isPrivate() ? (byte) 1 : (byte) 0);
    outputBuffer.putLong(properties.getCreationTimeInMs());
    outputBuffer.putLong(properties.getBlobSize());
    Utils.serializeNullableString(outputBuffer, properties.getContentType());
    Utils.serializeNullableString(outputBuffer, properties.getOwnerId());
    Utils.serializeNullableString(outputBuffer, properties.getServiceId());
    outputBuffer.putShort(properties.getAccountId());
    outputBuffer.putShort(properties.getContainerId());
  }
}
