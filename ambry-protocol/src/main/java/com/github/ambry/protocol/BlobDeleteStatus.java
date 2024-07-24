/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.server.ServerErrorCode;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * Delete status for a blob
 */
public class BlobDeleteStatus {
  private final BlobId blobId;
  private final ServerErrorCode status;

  public BlobDeleteStatus(BlobId blobId, ServerErrorCode status) {
    this.blobId = blobId;
    this.status = status;
  }

  /**
   * @return the {@link BlobId} of the blob that was deleted
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the {@link ServerErrorCode} that represents the status of the delete operation
   */
  public ServerErrorCode getStatus() {
    return status;
  }

  /**
   * Reads the serialized form of {@link BlobDeleteStatus} from the given {@link DataInputStream}.
   * @param stream the stream to read from
   * @param clusterMap the {@link ClusterMap} to use to deserialize the {@link BlobId}
   * @return a {@link BlobDeleteStatus} object
   * @throws IOException
   */
  public static BlobDeleteStatus readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    BlobId blobId = new BlobId(stream, clusterMap);
    ServerErrorCode status = ServerErrorCode.values()[stream.readShort()];
    return new BlobDeleteStatus(blobId, status);
  }

  /**
   * Writes the serialized form of {@link BlobDeleteStatus} to the given {@link ByteBuf}.
   * @param byteBuf the buffer to write to
   */
  public void writeTo(ByteBuf byteBuf) {
    byteBuf.writeBytes(blobId.toBytes());
    byteBuf.writeShort(status.ordinal());
  }

  /**
   * @return the size of the serialized form of this object in bytes
   */
  public long sizeInBytes() {
    return blobId.sizeInBytes() + Short.BYTES;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlobId=").append(blobId).append(", Status=").append(status);
    return sb.toString();
  }
}
