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
import com.github.ambry.clustermap.ClusterMapUtils;
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
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * <p>
 *   BlobId primarily consists of a uuid to uniquely identifies a stored blob. A blobId is a reference that is
 *   returned back to a caller when posting a blob, and later will be required to fetch the blob.
 * </p>
 * <p>
 *   There are two versions of format for BlobId de/serialization. Version 1, which includes {@code partitionId}
 *   of the blob. The {@code partitionId} is the {@link com.github.ambry.clustermap.Partition} to which this
 *   blob is assigned.
 * </p>
 * <pre>
 * +---------------------------------------------+
 * | version | partitionId | uuidSize | uuid     |
 * | (short) | (n bytes)   | (int)    | (n bytes)|
 * +---------------------------------------------+
 * </pre>
 * <p>
 *   Version 2, which includes {@code flag}, {@code accountId}, {@code containerId}, {@code datacenterId},
 *   and {@code partitionId} of the blob. {@code flag} is a single byte that carries the meta information
 *   for this blobId. It will be a place holder and assigned {@link #DEFAULT_FLAG} (i.e., no bit is set)
 *   before the assignment of each bit is determined. The {@code datacenterId} is the id of the datacenter
 *   where this blob was originally posted (not through replication). The {@code accountId} is the id of
 *   the {@link Account} the blob belongs to. The {@code containerId} is the {@link Container} the blob
 *   belongs to. The {@code partitionId} is the {@link com.github.ambry.clustermap.Partition} to which this
 *   blob is assigned.
 * </p>
 * <pre>
 * +---------+-------+--------------+-----------+-------------+-------------+----------+----------+
 * | version | flag  | datacenterId | accountId | containerId | partitionId | uuidSize | uuid     |
 * | (short) | (byte)| (byte)       | (short)   | (short)     | (n bytes)   | (int)    | (n bytes)|
 * +---------+----------------------+-----------+-------------+-------------+----------+----------+
 * </pre>
 */
public class BlobId extends StoreKey {
  public static final byte DEFAULT_FLAG = 0;
  // version 1 of the serialized format
  static final short BLOB_ID_V1 = 1;
  // version 2 of the serialized format
  static final short BLOB_ID_V2 = 2;
  private static final short CURRENT_VERSION = BLOB_ID_V1;
  private static final short VERSION_FIELD_LENGTH_IN_BYTES = Short.BYTES;
  private static final short UUID_SIZE_FIELD_LENGTH_IN_BYTES = Integer.BYTES;
  private static final short FLAG_FIELD_LENGTH_IN_BYTES = Byte.BYTES;
  private static final short DATACENTER_ID_FIELD_LENGTH_IN_BYTES = Byte.BYTES;
  private static final short ACCOUNT_ID_FIELD_LENGTH_IN_BYTES = Short.BYTES;
  private static final short CONTAINER_ID_FIELD_LENGTH_IN_BYTES = Short.BYTES;
  // the version to indicate the serialized format.
  private final Short version;
  private final Byte flag;
  private final Byte datacenterId;
  private final Short accountId;
  private final Short containerId;
  private final PartitionId partitionId;
  private final String uuid;

  /**
   * Constructs a new BlobId by taking arguments for the required fields. The constructed BlobId will be serialized
   * into {@link #CURRENT_VERSION}. If {@code CURRENT_VERSION == BLOB_ID_V1}, it will serialize itself into
   * {@code BLOB_ID_V1}, ignoring {@code flag}, {@code datacenterId}, {@code containerId}, and {@code containerId}
   * regardless they are set or not.
   * @param flag A byte to embed additional information of this blobId. Will be reset to {@link #DEFAULT_FLAG} if
   *             {@link #CURRENT_VERSION} is {@link #BLOB_ID_V1}.
   * @param datacenterId The id of the datacenter to be embedded into the blob. Will be reset to
   *             {@link ClusterMapUtils#UNKNOWN_DATACENTER_ID} if {@link #CURRENT_VERSION} is {@link #BLOB_ID_V1}.
   * @param accountId The id of the {@link Account} to be embedded into the blob. Will be reset to
   *             {@link Account#UNKNOWN_ACCOUNT_ID} if {@link #CURRENT_VERSION} is {@link #BLOB_ID_V1}.
   * @param containerId The id of the {@link Container} to be embedded into the blob. Will be reset to
   *             {@link Container#UNKNOWN_CONTAINER_ID} if {@link #CURRENT_VERSION} is {@link #BLOB_ID_V1}.
   * @param partitionId The partition where this blob is to be stored. Cannot be {@code null}.
   */
  public BlobId(byte flag, byte datacenterId, short accountId, short containerId, PartitionId partitionId) {
    if (partitionId == null) {
      throw new IllegalArgumentException("partitionId cannot be null");
    }
    version = getCurrentVersion();
    switch (version) {
      case BLOB_ID_V1:
        this.flag = DEFAULT_FLAG;
        this.datacenterId = UNKNOWN_DATACENTER_ID;
        this.accountId = UNKNOWN_ACCOUNT_ID;
        this.containerId = UNKNOWN_CONTAINER_ID;
        break;

      case BLOB_ID_V2:
        this.flag = flag;
        this.datacenterId = datacenterId;
        this.accountId = accountId;
        this.containerId = containerId;
        break;

      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
    }
    this.partitionId = partitionId;
    uuid = UUID.randomUUID().toString();
  }

  /**
   * Re-constructs existing blobId by deserializing from data input stream. This constructor includes an optional check
   * that the stream has no more available bytes after reading.
   *
   * @param stream from which to deserialize the blobId
   * @param clusterMap of the cluster that the partition of the blobId belongs to
   * @param ensureFullyRead {@code true} if the stream should have no more available bytes after deserializing the blob
   *                        ID.
   * @throws IOException
   */
  private BlobId(DataInputStream stream, ClusterMap clusterMap, boolean ensureFullyRead) throws IOException {
    version = stream.readShort();
    switch (version) {
      case BLOB_ID_V1:
        flag = DEFAULT_FLAG;
        datacenterId = UNKNOWN_DATACENTER_ID;
        accountId = UNKNOWN_ACCOUNT_ID;
        containerId = UNKNOWN_CONTAINER_ID;
        break;

      case BLOB_ID_V2:
        flag = stream.readByte();
        datacenterId = stream.readByte();
        accountId = stream.readShort();
        containerId = stream.readShort();
        break;

      default:
        throw new IllegalArgumentException("blobId version " + version + " not supported.");
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
   * Re-constructs existing blobId by deserializing from BlobId "string".
   *
   * @param id of Blob as output by BlobId.getID().
   * @param clusterMap of the cluster that the blobId belongs to.
   * @throws IOException
   */
  public BlobId(String id, ClusterMap clusterMap) throws IOException {
    this(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(id)))), clusterMap, true);
  }

  /**
   * Re-constructs existing blobId by deserializing from data input stream
   *
   * @param stream from which to deserialize the blobId.
   * @param clusterMap of the cluster that the blobId belongs to.
   * @throws IOException
   */
  public BlobId(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    this(stream, clusterMap, false);
  }

  /**
   * Size of blobId when it is serialized into bytes.
   * @return The byte count of the serialized blobId.
   */
  public short sizeInBytes() {
    short sizeForBlobIdV1 =
        (short) (VERSION_FIELD_LENGTH_IN_BYTES + partitionId.getBytes().length + UUID_SIZE_FIELD_LENGTH_IN_BYTES
            + uuid.getBytes().length);
    switch (version) {
      case BLOB_ID_V1:
        return sizeForBlobIdV1;

      case BLOB_ID_V2:
        return (short) (FLAG_FIELD_LENGTH_IN_BYTES + DATACENTER_ID_FIELD_LENGTH_IN_BYTES
            + ACCOUNT_ID_FIELD_LENGTH_IN_BYTES + CONTAINER_ID_FIELD_LENGTH_IN_BYTES + sizeForBlobIdV1);

      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
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
   * blobId was formed, it will return {@link Account#UNKNOWN_ACCOUNT_ID}.
   * @return The id of the {@link Account} who created this blob.
   */
  public short getAccountId() {
    return accountId;
  }

  /**
   * Gets the id of the {@link Container} where this blob belongs to. If this information was not available when
   * the blobId was formed, it will return {@link Container#UNKNOWN_CONTAINER_ID}.
   * @return The id of the {@link Container} where this blob belongs to.
   */
  public short getContainerId() {
    return containerId;
  }

  /**
   * Gets the id of the datacenter where this blob was originally posted. If this information was not available
   * when the blobId was formed, it will return {@link ClusterMapUtils#UNKNOWN_DATACENTER_ID}.
   * @return The id of the datacenter where this blob was originally posted.
   */
  public short getDatacenterId() {
    return datacenterId;
  }

  /**
   * Gets the flag metadata of this blobId. If this information was not available when the blobId was formed, it
   * will return {@link #DEFAULT_FLAG}.
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
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
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
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
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
      result = flag.compareTo(other.flag);
      if (result == 0) {
        result = datacenterId.compareTo(other.datacenterId);
        if (result == 0) {
          result = accountId.compareTo(other.accountId);
          if (result == 0) {
            result = containerId.compareTo(other.containerId);
            if (result == 0) {
              result = partitionId.compareTo(other.partitionId);
              if (result == 0) {
                result = uuid.compareTo(other.uuid);
              }
            }
          }
        }
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
   * Gets the value of {@link #CURRENT_VERSION}.
   * @return The value of {@link #CURRENT_VERSION}.
   */
  protected short getCurrentVersion() {
    return CURRENT_VERSION;
  }
}
