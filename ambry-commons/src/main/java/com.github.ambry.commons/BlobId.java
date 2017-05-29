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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
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

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.clustermap.Datacenter.*;


/**
 * <p>
 *   BlobId uniquely identifies a stored blob. A blobId is a reference that is returned back to a caller when posting
 * a blob, and later will be required to fetch the blob.
 * </p>
 *
 * <p>
 *   There are two versions of format for blob de/serialization. Version 1, which includes {@code partitionId} of the
 * blob. The {@code partitionId} is the {@link com.github.ambry.clustermap.Partition} to which this blob is assigned.
 * </p>
 * <pre>
 * +---------------------------------------------+
 * | version | partitionId | uuidSize | uuid     |
 * | (short) | (n bytes)   | (short)  | (n bytes)|
 * +---------------------------------------------+
 * </pre>
 * <p>
 *   Version 2, which includes {@code accountId}, {@code containerId}, {@code datacenterId}, and {@code partitionId}
 * of the blob. The {@code accountId} is the id of the {@link Account} the blob belongs to, the {@code containerId} is
 * the {@link Container} the blob belongs to, and the {@code datacenterId} is the id of the datacenter where this blob
 * was originally posted (not through replication). The {@code partitionId} is the {@link com.github.ambry.clustermap.Partition}
 * to which this blob is assigned.
 * </p>
 * <pre>
 * +---------+-----------+-------------+--------------+-------------+----------+----------+
 * | version | accountId | containerId | datacenterId | partitionId | uuidSize | uuid     |
 * | (short) | (short)   | (short)     | (short)      | (n bytes)   | (short)  | (n bytes)|
 * +---------+-----------+-------------+--------------+-------------+----------+----------+
 * </pre>
 */
public class BlobId extends StoreKey {
  static final short BLOB_ID_V1 = 1;
  static final short BLOB_ID_V2 = 2;
  private static final short VERSION_SIZE_IN_BYTES = 2;
  private static final int UUID_SIZE_IN_BYTES = 4;
  private static final short ACCOUNT_ID_SIZE_IN_BYTES = 2;
  private static final short CONTAINER_ID_SIZE_IN_BYTES = 2;
  private static final short DATACENTER_ID_SIZE_IN_BYTES = 2;
  // the version to indicate the serialized format.
  private Short version;
  private short accountId;
  private short containerId;
  private short datacenterId;
  private PartitionId partitionId;
  private String uuid;

  /**
   * Constructs a new unique BlobId. This will construct a blob in version 2, so the serialization embeds accountId,
   * containerId, datacenterId, and partitionId.
   * @param accountId The id of the {@link Account} to be embedded into the blob.
   * @param containerId The id of the {@link Container} to be embedded into the blob.
   * @param datacenterId The id of the datacenter to be embedded into the blob.
   * @param partitionId The partition where this blob is to be stored.
   */
  public BlobId(short accountId, short containerId, short datacenterId, PartitionId partitionId) {
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition ID cannot be null");
    }
    this.version = BLOB_ID_V2;
    this.accountId = accountId;
    this.containerId = containerId;
    this.datacenterId = datacenterId;
    this.partitionId = partitionId;
    this.uuid = UUID.randomUUID().toString();
  }

  /**
   * Constructs a new unique BlobId for the specified partition. This will construct a blob in version 1, so the
   * serialization does not embed account id, container id, or datacenter id.
   * @param partitionId The partition where this blob is to be stored.
   */
  public BlobId(PartitionId partitionId) {
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition ID cannot be null");
    }
    this.version = BLOB_ID_V1;
    this.accountId = LEGACY_ACCOUNT_ID;
    this.containerId = LEGACY_CONTAINER_ID;
    this.datacenterId = LEGACY_DATACENTER_ID;
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
   * @param clusterMap of the cluster that the partition of the blob id belongs to
   * @param ensureFullyRead {@code true} if the stream should have no more available bytes after deserializing the blob
   *                        ID.
   * @throws IOException
   */
  private BlobId(DataInputStream stream, ClusterMap clusterMap, boolean ensureFullyRead) throws IOException {
    this.version = stream.readShort();
    switch (version) {
      case BLOB_ID_V1:
        accountId = LEGACY_ACCOUNT_ID;
        containerId = LEGACY_CONTAINER_ID;
        datacenterId = LEGACY_DATACENTER_ID;
        break;
      case BLOB_ID_V2:
        accountId = stream.readShort();
        containerId = stream.readShort();
        datacenterId = stream.readShort();
        break;
      default:
        throw new IllegalArgumentException("blob id version " + version + " not supported for blob id");
    }
    partitionId = clusterMap.getPartitionIdFromStream(stream);
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition ID cannot be null");
    }
    uuid = Utils.readIntString(stream);
    if (ensureFullyRead && stream.read() != -1) {
      throw new IllegalArgumentException("Stream should have no more available bytes to read");
    }
  }

  /**
   * Size of blob id when it is serialized into bytes.
   * @return The byte count of the serialized blob id.
   */
  public short sizeInBytes() {
    switch (version) {
      case BLOB_ID_V1:
        return (short) (VERSION_SIZE_IN_BYTES + partitionId.getBytes().length + UUID_SIZE_IN_BYTES + uuid.length());
      case BLOB_ID_V2:
        return (short) (VERSION_SIZE_IN_BYTES + ACCOUNT_ID_SIZE_IN_BYTES + CONTAINER_ID_SIZE_IN_BYTES
            + DATACENTER_ID_SIZE_IN_BYTES + partitionId.getBytes().length + UUID_SIZE_IN_BYTES + uuid.length());
      default:
        throw new IllegalArgumentException("blob id version=" + version + " not supported");
    }
  }

  /**
   * Gets the {@link PartitionId} this blob belongs to.
   * @return The {@link PartitionId}.
   */
  public PartitionId getPartition() {
    return partitionId;
  }

  /**
   * Gets the id of the {@link Account} who created this blob. If this information was not available when the
   * blob id was formed, it will return -1.
   * @return The id of the {@link Account} who created this blob.
   */
  public short getAccountId() {
    return accountId;
  }

  /**
   * Gets the id of the {@link Container} where this blob belongs to. If this information was not available when
   * the blob id was formed, it will return -1.
   * @return The id of the {@link Container} where this blob belongs to.
   */
  public short getContainerId() {
    return containerId;
  }

  /**
   * Gets the id of the datacenter where this blob was originally posted. If this information was not available
   * when the blob id was formed, it will return -1.
   * @return
   */
  public short getDatacenterId() {
    return datacenterId;
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.putShort(version);
    switch (version) {
      case BLOB_ID_V1:
        break;
      case BLOB_ID_V2:
        idBuf.putShort(accountId);
        idBuf.putShort(containerId);
        idBuf.putShort(datacenterId);
        break;
      default:
        throw new IllegalArgumentException("blob id version=" + version + " not supported");
    }
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
    switch (version) {
      case BLOB_ID_V1:
        break;
      case BLOB_ID_V2:
        sb.append(":").append(accountId);
        sb.append(":").append(containerId);
        sb.append(":").append(datacenterId);
        break;
      default:
        throw new IllegalArgumentException("blob id version=" + version + " not supported");
    }
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
    if (!(o instanceof BlobId)) {
      return false;
    }

    BlobId blobId = (BlobId) o;

    if (accountId != blobId.accountId) {
      return false;
    }
    if (containerId != blobId.containerId) {
      return false;
    }
    if (datacenterId != blobId.datacenterId) {
      return false;
    }
    if (!version.equals(blobId.version)) {
      return false;
    }
    if (!partitionId.equals(blobId.partitionId)) {
      return false;
    }
    return uuid.equals(blobId.uuid);
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{version, partitionId, uuid});
  }
}
