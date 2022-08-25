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

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * An admin request used to download all the message info records for a given blob id
 * from a particular server host.
 */
public class BlobIndexAdminRequest extends AdminRequest {
  private static final short VERSION_1 = 1;
  private static final int BLOB_ID_LENGTH = 4;
  private final String blobId;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link BlobIndexAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link BlobIndexAdminRequest} constructed from the the {@code stream}.
   * @throws IOException if there is any problem reading from the stream.
   */
  public static BlobIndexAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest) throws IOException {
    short versionId = stream.readShort();
    if (versionId != VERSION_1) {
      throw new IllegalStateException("Unrecognized version for BlobIndexAdminRequest: " + versionId);
    }
    String blobId = Utils.readIntString(stream);
    return new BlobIndexAdminRequest(blobId, adminRequest);
  }

  /**
   * Constructor for {@link BlobIndexAdminRequest}.
   * @param blobId The blob id in string format.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public BlobIndexAdminRequest(String blobId, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.BlobIndex, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.blobId = blobId;
    this.sizeInBytes = super.sizeInBytes() + Short.BYTES + BLOB_ID_LENGTH + blobId.length();
  }

  /**
   * @return the BlobId in string format
   */
  public String getBlobId() {
    return blobId;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "BlobIndexAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", BlobId=" + blobId
        + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_1);
    bufferToSend.writeInt(blobId.length());
    bufferToSend.writeBytes(blobId.getBytes());
  }
}
