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

  static final short VERSION_1 = 1;
  static final short VERSION_2 = 2;
  private static final short CURRENT_VERSION = VERSION_2;
  private static final int VERSION_FIELD_SIZE_IN_BYTES = Short.BYTES;
  private static final int TTL_FIELD_SIZE_IN_BYTES = Long.BYTES;
  private static final int PRIVATE_FIELD_SIZE_IN_BYTES = Byte.BYTES;
  private static final int CREATION_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;
  private static final int VARIABLE_FIELD_SIZE_IN_BYTES = Integer.BYTES;
  private static final int BLOB_SIZE_FIELD_SIZE_IN_BYTES = Long.BYTES;

  public static int getBlobPropertiesSerDeSize(BlobProperties properties) {
    return VERSION_FIELD_SIZE_IN_BYTES + TTL_FIELD_SIZE_IN_BYTES + PRIVATE_FIELD_SIZE_IN_BYTES
        + CREATION_TIME_FIELD_SIZE_IN_BYTES + BLOB_SIZE_FIELD_SIZE_IN_BYTES + VARIABLE_FIELD_SIZE_IN_BYTES
        + Utils.getNullableStringLength(properties.getContentType()) + VARIABLE_FIELD_SIZE_IN_BYTES
        + Utils.getNullableStringLength(properties.getOwnerId()) + VARIABLE_FIELD_SIZE_IN_BYTES
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
      case VERSION_1:
        toReturn = new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, ttl, creationTime,
            Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID);
        break;
      case VERSION_2:
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
   * Serialize {@link BlobProperties} to buffer in the {@link #CURRENT_VERSION}
   * @param outputBuffer the {@link ByteBuffer} to which {@link BlobProperties} needs to be serialized
   * @param properties the {@link BlobProperties} that needs to be serialized
   */
  public static void serializeBlobProperties(ByteBuffer outputBuffer, BlobProperties properties) {
    if (outputBuffer.remaining() < getBlobPropertiesSerDeSize(properties)) {
      throw new IllegalArgumentException("Outut buffer does not have sufficient space to serialize blob properties");
    }
    outputBuffer.putShort(CURRENT_VERSION);
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
