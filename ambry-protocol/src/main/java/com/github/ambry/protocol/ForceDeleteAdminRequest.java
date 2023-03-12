/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.protocol;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * An admin request used to force to write a delete record without a Put for a given blob id
 * from a particular server host.
 */
public class ForceDeleteAdminRequest extends AdminRequest {
  private static final short VERSION_1 = 1;
  private final StoreKey storeKey;  // Blob Id
  private final short lifeVersion;  // life version
  private final long sizeInBytes;   // request size in bytes

  /**
   * Reads from a stream and constructs a {@link ForceDeleteAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @param factory the {@link StoreKeyFactory} to construct a {@link StoreKey} from stream.
   * @return the {@link ForceDeleteAdminRequest} constructed from the the {@code stream}.
   * @throws IOException if there is any problem reading from the stream.
   */
  public static ForceDeleteAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest,
      StoreKeyFactory factory) throws IOException {
    short versionId = stream.readShort();
    if (versionId != VERSION_1) {
      throw new IllegalStateException("Unrecognized version for ForceDeleteAdminRequest: " + versionId);
    }
    StoreKey storeKey = factory.getStoreKey(stream);
    short lifeVersion = stream.readShort();
    return new ForceDeleteAdminRequest(storeKey, lifeVersion, adminRequest);
  }

  /**
   * Constructor for {@link ForceDeleteAdminRequest}.
   * @param key The {@link StoreKey}.
   * @param lifeVersion life version of the Blob
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public ForceDeleteAdminRequest(StoreKey key, short lifeVersion, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.ForceDelete, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.storeKey = key;
    this.lifeVersion = lifeVersion;
    // Header + format version + StoreKey + lifeVersion
    this.sizeInBytes = super.sizeInBytes() + Short.BYTES + storeKey.sizeInBytes() + Short.BYTES;
  }

  /**
   * @return the store key
   */
  public StoreKey getStoreKey() {
    return storeKey;
  }

  /**
   * @return the lifeVersion
   */
  public short getLifeVersion() {
    return lifeVersion;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ForceDeleteAdminRequest[")
        .append("Client=")
        .append(clientId)
        .append(", CorrelationId=")
        .append(correlationId)
        .append(", Key=")
        .append(storeKey.getID())
        .append(", lifeVersion=")
        .append(lifeVersion);
    return sb.toString();
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_1);
    bufferToSend.writeBytes(storeKey.toBytes());
    bufferToSend.writeShort(lifeVersion);
  }
}
