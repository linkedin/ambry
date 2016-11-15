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
package com.github.ambry.commons;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;


/**
 * BlobId uniquely identifies a stored blob as well as the Partition in which the blob is stored.
 */
public class BlobId extends StoreKey {
  private Short version = 1;
  private static short Version_Size_In_Bytes = 2;
  private PartitionId partitionId;
  private String uuid;
  private static int UUID_Size_In_Bytes = 4;

  /**
   * Constructs a new unique BlobId for the specified partition.
   *
   * @param partitionId of Partition in which blob is to be stored.
   */
  public BlobId(PartitionId partitionId) {
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition ID cannot be null");
    }
    this.partitionId = partitionId;
    this.uuid = UUID.randomUUID().toString();
  }

  /**
   * Re-constructs existing blobId by deserializing from BlobId "string"
   *
   * @param id of Blob as output by BlobId.getID()
   * @param clusterMap of the cluster that the blob id belongs to
   * @throws IOException
   */
  public BlobId(String id, ClusterMap clusterMap) throws IOException {
    this(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(id)))), clusterMap, true);
  }

  /**
   * Re-constructs existing blobId by deserializing from data input stream
   *
   * @param stream from which to deserialize the blobid
   * @param clusterMap of the cluster that the blob id belongs to
   * @throws IOException
   */
  public BlobId(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    this(stream, clusterMap, false);
  }

  /**
   * Re-constructs existing blobId by deserializing from data input stream. This constructor includes an optional check
   * that the stream has no more available bytes after reading.
   *
   * @param stream from which to deserialize the blobid
   * @param clusterMap of the cluster that the blob id belongs to
   * @param ensureFullyRead {@code true} if the stream should have no more available bytes after deserializing the blob
   *                        ID.
   * @throws IOException
   */
  private BlobId(DataInputStream stream, ClusterMap clusterMap, boolean ensureFullyRead) throws IOException {
    this.version = stream.readShort();
    if (version == 1) {
      partitionId = clusterMap.getPartitionIdFromStream(stream);
      if (partitionId == null) {
        throw new IllegalArgumentException("Partition ID cannot be null");
      }
      uuid = Utils.readIntString(stream);
      if (ensureFullyRead && stream.read() != -1) {
        throw new IllegalArgumentException("Stream should have no more available bytes to read");
      }
    } else {
      throw new IllegalArgumentException("version " + version + " not supported for blob id");
    }
  }

  public short sizeInBytes() {
    return (short) (Version_Size_In_Bytes + partitionId.getBytes().length + UUID_Size_In_Bytes + uuid.length());
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.putShort(version);
    idBuf.put(partitionId.getBytes());
    idBuf.putInt(uuid.getBytes().length);
    idBuf.put(uuid.getBytes());
    return idBuf.array();
  }

  @Override
  public String getID() {
    return Base64.encodeBase64URLSafeString(toBytes());
  }

  @Override
  public String getLongForm() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(getID());
    sb.append(":").append(version);
    sb.append(":").append(partitionId);
    sb.append(":").append(uuid).append("]");
    return sb.toString();
  }

  @Override
  public String toString() {
    return getID();
  }

  @Override
  public int compareTo(StoreKey o) {
    BlobId other = (BlobId) o;

    int result = version.compareTo(other.version);
    if (result == 0) {
      result = partitionId.compareTo(other.partitionId);
      if (result == 0) {
        result = uuid.compareTo(other.uuid);
      }
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BlobId blobId = (BlobId) o;

    if (!version.equals(blobId.version)) {
      return false;
    }
    if (!partitionId.equals(blobId.partitionId)) {
      return false;
    }
    if (!uuid.equals(blobId.uuid)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{version, partitionId, uuid});
  }
}
