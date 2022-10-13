/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 * An admin request used to download all the message info records for a given blob id
 * from a particular server host.
 */
public class BlobIndexAdminRequest extends AdminRequest {
  private static final short VERSION_1 = 1;
  private final StoreKey storeKey;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link BlobIndexAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @param factory the {@link StoreKeyFactory} to construct a {@link StoreKey} from stream.
   * @return the {@link BlobIndexAdminRequest} constructed from the the {@code stream}.
   * @throws IOException if there is any problem reading from the stream.
   */
  public static BlobIndexAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest,
      StoreKeyFactory factory) throws IOException {
    short versionId = stream.readShort();
    if (versionId != VERSION_1) {
      throw new IllegalStateException("Unrecognized version for BlobIndexAdminRequest: " + versionId);
    }
    StoreKey storeKey = factory.getStoreKey(stream);
    return new BlobIndexAdminRequest(storeKey, adminRequest);
  }

  /**
   * Constructor for {@link BlobIndexAdminRequest}.
   * @param key The {@link StoreKey}.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public BlobIndexAdminRequest(StoreKey key, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.BlobIndex, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.storeKey = key;
    // Header + Version + StoreKey
    this.sizeInBytes = super.sizeInBytes() + Short.BYTES + storeKey.sizeInBytes();
  }

  /**
   * @return the BlobId in string format
   */
  public StoreKey getStoreKey() {
    return storeKey;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlobIndexAdminRequest[")
        .append("Client=")
        .append(clientId)
        .append(", CorrelationId=")
        .append(correlationId)
        .append(", Key=")
        .append(storeKey.getID());
    return sb.toString();
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_1);
    bufferToSend.writeBytes(storeKey.toBytes());
  }
}
