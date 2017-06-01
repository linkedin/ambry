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
import com.github.ambry.clustermap.Datacenter;
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
 *   BlobId uniquely identifies a stored blob. A blobId is a reference that is returned back to a caller when
 *   posting a blob, and later will be required to fetch the blob.
 * </p>
 * <p>
 *   There are two versions of format for blob de/serialization. Version 1, which includes {@code partitionId}
 *   of the blob. The {@code partitionId} is the {@link com.github.ambry.clustermap.Partition} to which this
 *   blob is assigned.
 * </p>
 * <pre>
 * +---------------------------------------------+
 * | version | partitionId | uuidSize | uuid     |
 * | (short) | (n bytes)   | (short)  | (n bytes)|
 * +---------------------------------------------+
 * </pre>
 * <p>
 *   Version 2, which includes{@code flag}, {@code accountId}, {@code containerId}, {@code datacenterId}, and
 *   {@code partitionId} of the blob. {@code flag} is a single byte that carries the meta information for this
 *   blobId. The {@code datacenterId} is the id of the datacenter where this blob was originally posted (not
 *   through replication). The {@code accountId} is the id of the {@link Account} the blob belongs to. The
 *   {@code containerId} is the {@link Container} the blob belongs to. The {@code partitionId} is the
 *   {@link com.github.ambry.clustermap.Partition} to which this blob is assigned.
 * </p>
 * <pre>
 * +---------+-------+--------------+-----------+-------------+-------------+----------+----------+
 * | version | flag  | datacenterId | accountId | containerId | partitionId | uuidSize | uuid     |
 * | (short) | (byte)| (byte)       | (short)   | (short)     | (n bytes)   | (short)  | (n bytes)|
 * +---------+----------------------+-----------+-------------+-------------+----------+----------+
 * </pre>
 */
public class BlobId extends StoreKey {
  static final short BLOB_ID_V1 = 1;
  static final short BLOB_ID_V2 = 2;
  static final byte DEFAULT_FLAG = 0;
  private static final short VERSION_SIZE_IN_BYTES = 2;
  private static final int UUID_SIZE_IN_BYTES = 4;
  private static final short FLAG_SIZE_IN_BYTES = 1;
  private static final short DATACENTER_ID_SIZE_IN_BYTES = 1;
  private static final short ACCOUNT_ID_SIZE_IN_BYTES = 2;
  private static final short CONTAINER_ID_SIZE_IN_BYTES = 2;
  // the version to indicate the serialized format.
  private Short version;
  private Byte flag;
  private Byte datacenterId;
  private Short accountId;
  private Short containerId;
  private PartitionId partitionId;
  private String uuid;

  /**
   * Constructs a new unique BlobId. This will construct a blob in version 2, so the serialization embeds accountId,
   * containerId, datacenterId, and partitionId.
   * @param flag A byte to embed additional information of this blobId.
   * @param datacenterId The id of the datacenter to be embedded into the blob.
   * @param accountId The id of the {@link Account} to be embedded into the blob.
   * @param containerId The id of the {@link Container} to be embedded into the blob.
   * @param partitionId The partition where this blob is to be stored.
   */
  BlobId(Byte flag, Byte datacenterId, Short accountId, Short containerId, PartitionId partitionId) {
    this.flag = flag;
    this.datacenterId = datacenterId;
    this.accountId = accountId;
    this.containerId = containerId;
    this.partitionId = partitionId;
    uuid = UUID.randomUUID().toString();
    populateMissingFieldsAndDetermineVersion();
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
        flag = DEFAULT_FLAG;
        datacenterId = LEGACY_DATACENTER_ID;
        accountId = LEGACY_ACCOUNT_ID;
        containerId = LEGACY_CONTAINER_ID;
        break;
      case BLOB_ID_V2:
        flag = stream.readByte();
        datacenterId = stream.readByte();
        accountId = stream.readShort();
        containerId = stream.readShort();
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
   * Re-constructs existing blobId by deserializing from BlobId "string"
   *
   * @param id of Blob as output by BlobId.getID()
   * @param clusterMap of the cluster that the blob id belongs to
   * @throws IOException
   */
  public static BlobId fromStringId(String id, ClusterMap clusterMap) throws IOException {
    return new BlobId(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(id)))),
        clusterMap, true);
  }

  /**
   * Re-constructs existing blobId by deserializing from data input stream
   *
   * @param stream from which to deserialize the blobid
   * @param clusterMap of the cluster that the blob id belongs to
   * @throws IOException
   */
  public static BlobId fromDataInputStream(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    return new BlobId(stream, clusterMap, false);
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
        return (short) (VERSION_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + DATACENTER_ID_SIZE_IN_BYTES
            + ACCOUNT_ID_SIZE_IN_BYTES + CONTAINER_ID_SIZE_IN_BYTES + partitionId.getBytes().length + UUID_SIZE_IN_BYTES
            + uuid.length());
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
   * @return The id of the datacenter where this blob was originally posted.
   */
  public short getDatacenterId() {
    return datacenterId;
  }

  /**
   * Gets the flag metadata of this blob id. If this information was not available when the blob id was formed, it
   * will return 0.
   * @return The flag of the blobId.
   */
  public short getFlag() {
    return flag;
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.putShort(version);
    switch (version) {
      case BLOB_ID_V1:
        break;

      case BLOB_ID_V2:
        idBuf.put(flag);
        idBuf.put(datacenterId);
        idBuf.putShort(accountId);
        idBuf.putShort(containerId);
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
        sb.append(":").append(flag);
        sb.append(":").append(datacenterId);
        sb.append(":").append(accountId);
        sb.append(":").append(containerId);
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

    if (!version.equals(blobId.version)) {
      return false;
    }
    if (!flag.equals(blobId.flag)) {
      return false;
    }
    if (!datacenterId.equals(blobId.datacenterId)) {
      return false;
    }
    if (!accountId.equals(blobId.accountId)) {
      return false;
    }
    if (!containerId.equals(blobId.containerId)) {
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

  /**
   * Populate the missing fields with legacy values, and determine the version of format to serialize. Only
   * when all the four fields of {@code flag}, {@code datacenterId}, {@code accountId}, and {@code containerId}
   * are available, the serialization format version will be set to {@link #BLOB_ID_V2}, otherwise it will be
   * {@link #BLOB_ID_V1}.
   */
  private void populateMissingFieldsAndDetermineVersion() {
    version = flag == null && accountId == null && containerId == null && datacenterId == null ? BlobId.BLOB_ID_V1
        : BlobId.BLOB_ID_V2;
    if (partitionId == null) {
      throw new IllegalStateException("partitionId is null");
    }
    if (flag == null) {
      flag = DEFAULT_FLAG;
    }
    if (datacenterId == null) {
      datacenterId = Datacenter.LEGACY_DATACENTER_ID;
    }
    if (accountId == null) {
      accountId = Account.LEGACY_ACCOUNT_ID;
    }
    if (containerId == null) {
      containerId = Container.LEGACY_CONTAINER_ID;
    }
  }
}
