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
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * BlobId consists of a uuid to uniquely identify a stored blob. A blobId is a reference that is
 * returned back to a caller when posting a blob, and later will be required to fetch the blob. The id can
 * also embed other important metadata associated with the blob, and these are version dependent.
 * <br>
 * There are three BlobId versions:
 * <br>
 * Version 1, which includes {@code partitionId}
 * of the blob. The {@code partitionId} is the {@link PartitionId} to which this blob is assigned.
 * <br>
 * <pre>
 * +---------------------------------------------+
 * | version | partitionId | uuidSize | uuid     |
 * | (short) | (n bytes)   | (int)    | (n bytes)|
 * +---------------------------------------------+
 * </pre>
 * <br>
 * Version 2, which includes {@code flag}, {@code accountId}, {@code containerId}, {@code datacenterId},
 * and {@code partitionId} of the blob. {@code flag} is a single byte that is unused for V2. The {@code datacenterId} is
 * the id of the datacenter where this blob was originally posted (not through replication). The {@code accountId} is
 * the id of the {@link Account} the blob belongs to. The {@code containerId} is the {@link Container} the blob
 * belongs to. The {@code partitionId} is the {@link com.github.ambry.clustermap.Partition} to which this
 * blob is assigned.
 * <br>
 * <pre>
 * +---------+-------+--------------+-----------+-------------+-------------+----------+----------+
 * | version | flag  | datacenterId | accountId | containerId | partitionId | uuidSize | uuid     |
 * | (short) | (byte)| (byte)       | (short)   | (short)     | (n bytes)   | (int)    | (n bytes)|
 * +---------+----------------------+-----------+-------------+-------------+----------+----------+
 * </pre>
 * <br>
 * Version 3, which is the same as Version 2 with one exception. The least significant bit of the flag byte will be used
 * to distinguish between different types of ids. V3 makes the distinction of two types of ids: One that is created
 * natively by the router in the context of a PUT operation; and the second that is "crafted" outside of the router.
 * Crafting an id outside of the router is useful if we want to convert V1 and V2 ids that exist in storage without any
 * ownership information (accounts and containers) associated with them into a V3 version that has the right ownership.
 * <br>
 * <pre>
 * +---------+-------+--------------+-----------+-------------+-------------+----------+----------+
 * | version | flag  | datacenterId | accountId | containerId | partitionId | uuidSize | uuid     |
 * | (short) | (byte)| (byte)       | (short)   | (short)     | (n bytes)   | (int)    | (n bytes)|
 * +---------+----------------------+-----------+-------------+-------------+----------+----------+
 *
 * Flag format: 1 Byte
 * +--------------+-------------+---------------+
 * |  1 to 5 bits |    6th bit  | 7 and 8th bit |
 * |  un-assigned | IsEncrypted |  BlobIdType   |
 * +--------------+-------------+---------------+
 *
 * </pre>
 *
 * <br>
 * Version 4, which is the same as Version 3 now but indicates that .
 * <br>
 * <pre>
 * +---------+-------+--------------+-----------+-------------+-------------+----------+----------+
 * | version | flag  | datacenterId | accountId | containerId | partitionId | uuidSize | uuid     |
 * | (short) | (byte)| (byte)       | (short)   | (short)     | (n bytes)   | (int)    | (n bytes)|
 * +---------+----------------------+-----------+-------------+-------------+----------+----------+
 *
 * Flag format: 1 Byte
 * +--------------+-------------+---------------+
 * |  1 to 5 bits |    6th bit  | 7 and 8th bit |
 * |  un-assigned | IsEncrypted |  BlobIdType   |
 * +--------------+-------------+---------------+
 *
 * </pre>
 *
 * <br>
 * Version 5, which is the same as Version 4 but with additional info in the flag byte.  Flag bits 4 and 5
 * are used to specify the Blob Data Type, which can be Simple, Metadata, or DataChunk.
 * <br>
 * <pre>
 * +---------+-------+--------------+-----------+-------------+-------------+----------+----------+
 * | version | flag  | datacenterId | accountId | containerId | partitionId | uuidSize | uuid     |
 * | (short) | (byte)| (byte)       | (short)   | (short)     | (n bytes)   | (int)    | (n bytes)|
 * +---------+----------------------+-----------+-------------+-------------+----------+----------+
 *
 * Flag format: 1 Byte
 * +--------------+---------------+-------------+---------------+
 * |  1 to 3 bits | 4 and 5th bit |    6th bit  | 7 and 8th bit |
 * |  un-assigned | BlobDataType  | IsEncrypted |  BlobIdType   |
 * +--------------+---------------+-------------+---------------|
 * </pre>
 *
 * <br>
 * Version 6, which has a more compact encoding for the UUID where it is encoded as a 16 byte value instead of a
 * size-prefixed hexadecimal string.
 * <br>
 * <pre>
 * +---------+--------+--------------+-----------+-------------+-------------+------------+
 * | version | flag   | datacenterId | accountId | containerId | partitionId | uuid       |
 * | (short) | (byte) | (byte)       | (short)   | (short)     | (n bytes)   | (16 bytes) |
 * +---------+--------+--------------+-----------+-------------+-------------+------------+
 *
 * Flag format: 1 Byte
 * +--------------+---------------+-------------+---------------+
 * |  1 to 3 bits | 4 and 5th bit |    6th bit  | 7 and 8th bit |
 * |  un-assigned | BlobDataType  | IsEncrypted |  BlobIdType   |
 * +--------------+---------------+-------------+---------------|
 * </pre>
 */

public class BlobId extends StoreKey {
  public static final short BLOB_ID_V1 = 1;
  public static final short BLOB_ID_V2 = 2;
  public static final short BLOB_ID_V3 = 3;
  public static final short BLOB_ID_V4 = 4;
  public static final short BLOB_ID_V5 = 5;
  public static final short BLOB_ID_V6 = 6;
  private static final short VERSION_FIELD_LENGTH_IN_BYTES = Short.BYTES;
  private static final short UUID_SIZE_FIELD_LENGTH_IN_BYTES = Integer.BYTES;
  private static final short FLAG_FIELD_LENGTH_IN_BYTES = Byte.BYTES;
  private static final short DATACENTER_ID_FIELD_LENGTH_IN_BYTES = Byte.BYTES;
  private static final short ACCOUNT_ID_FIELD_LENGTH_IN_BYTES = Short.BYTES;
  private static final short CONTAINER_ID_FIELD_LENGTH_IN_BYTES = Short.BYTES;
  private static final int BLOB_ID_TYPE_MASK = 0x3;
  private static final int IS_ENCRYPTED_MASK = 0x4;
  private static final int BLOB_DATA_TYPE_MASK = 0x18;
  private static final int BLOB_DATA_TYPE_SHIFT = 3;

  private final short version;
  private final BlobIdType type;
  private final byte datacenterId;
  private final short accountId;
  private final short containerId;
  private final PartitionId partitionId;
  /**
   * For blob IDs with valid UUIDs, we can use a {@link UUID} object to save a few bytes (32 bytes instead of 112 bytes
   * for a {@link String}).
   */
  private final UUID uuid;
  /**
   * Ideally we could always use a {@link UUID} object instead of a {@link String} to save some memory, but I am not
   * sure if previous code versions allowed constructing blob IDs with custom strings in this field, as opposed to
   * always calling {@link UUID#randomUUID()}.
   */
  private final String uuidStr;
  private final boolean isEncrypted;
  private final BlobDataType blobDataType;

  /**
   * Constructs a new BlobId by taking arguments for the required fields.
   * Not all the fields in the constructor may be used in constructing it. The current active version determines what
   * fields will be used.
   * @param version the version in which this blob should be created.
   * @param type The {@link BlobIdType} of the blob to be created. Only relevant for V3 and above.
   * @param datacenterId The id of the datacenter to be embedded into the blob. Only relevant for V2 and above.
   * @param accountId The id of the {@link Account} to be embedded into the blob. Only relevant for V2 and above.
   * @param containerId The id of the {@link Container} to be embedded into the blob. Only relevant for V2 and above.
   * @param partitionId The partition where this blob is to be stored. Cannot be {@code null}.
   * @param isEncrypted {@code true} if blob that this blobId represents is encrypted. {@code false} otherwise.
   *                                Valid for {@link BlobId#BLOB_ID_V4} and above.
   * @param blobDataType The blob data type.
   */
  public BlobId(short version, BlobIdType type, byte datacenterId, short accountId, short containerId,
      PartitionId partitionId, boolean isEncrypted, BlobDataType blobDataType) {
    this(version, type, datacenterId, accountId, containerId, partitionId, isEncrypted, blobDataType,
        UUID.randomUUID().toString());
  }

  /**
   * Internal private method to construct a BlobId by taking arguments for the required fields.
   * Not all the fields in the constructor may be used in constructing it. The current active version determines what
   * fields will be used.
   * @param version the version in which this blob should be created.
   * @param type The {@link BlobIdType} of the blob to be created. Only relevant for V3 and above.
   * @param datacenterId The id of the datacenter to be embedded into the blob. Only relevant for V2 and above.
   * @param accountId The id of the {@link Account} to be embedded into the blob. Only relevant for V2 and above.
   * @param containerId The id of the {@link Container} to be embedded into the blob. Only relevant for V2 and above.
   * @param partitionId The partition where this blob is to be stored. Cannot be {@code null}.
   * @param isEncrypted {@code true} if blob that this blobId represents is encrypted. {@code false} otherwise
   * @param blobDataType The blob data type.
   * @param uuidStr The uuid that is to be used to construct this id.
   */
  BlobId(short version, BlobIdType type, byte datacenterId, short accountId, short containerId, PartitionId partitionId,
      boolean isEncrypted, BlobDataType blobDataType, String uuidStr) {
    if (partitionId == null) {
      throw new IllegalArgumentException("partitionId cannot be null");
    }
    switch (version) {
      case BLOB_ID_V1:
        this.type = BlobIdType.NATIVE;
        this.datacenterId = UNKNOWN_DATACENTER_ID;
        this.accountId = UNKNOWN_ACCOUNT_ID;
        this.containerId = UNKNOWN_CONTAINER_ID;
        this.isEncrypted = false;
        this.blobDataType = null;
        this.uuid = null;
        this.uuidStr = uuidStr;
        break;
      case BLOB_ID_V2:
        this.type = BlobIdType.NATIVE;
        this.datacenterId = datacenterId;
        this.accountId = accountId;
        this.containerId = containerId;
        this.isEncrypted = false;
        this.blobDataType = null;
        this.uuid = null;
        this.uuidStr = uuidStr;
        break;
      case BLOB_ID_V3:
        this.type = type;
        this.datacenterId = datacenterId;
        this.accountId = accountId;
        this.containerId = containerId;
        this.isEncrypted = false;
        this.blobDataType = null;
        this.uuid = null;
        this.uuidStr = uuidStr;
        break;
      case BLOB_ID_V4:
        this.type = type;
        this.datacenterId = datacenterId;
        this.accountId = accountId;
        this.containerId = containerId;
        this.isEncrypted = isEncrypted;
        this.blobDataType = null;
        this.uuid = null;
        this.uuidStr = uuidStr;
        break;
      case BLOB_ID_V5:
        this.type = type;
        this.datacenterId = datacenterId;
        this.accountId = accountId;
        this.containerId = containerId;
        this.isEncrypted = isEncrypted;
        this.blobDataType = Objects.requireNonNull(blobDataType, "blobDataType can't be null for id version 5");
        this.uuid = null;
        this.uuidStr = uuidStr;
        break;
      case BLOB_ID_V6:
        this.type = type;
        this.datacenterId = datacenterId;
        this.accountId = accountId;
        this.containerId = containerId;
        this.isEncrypted = isEncrypted;
        this.blobDataType = Objects.requireNonNull(blobDataType, "blobDataType can't be null for id version 6");
        this.uuid = UUID.fromString(uuidStr);
        this.uuidStr = null;
        break;
      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
    }
    this.version = version;
    this.partitionId = partitionId;
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
    BlobIdPreamble preamble = new BlobIdPreamble(stream);
    version = preamble.version;
    type = preamble.type;
    datacenterId = preamble.datacenterId;
    accountId = preamble.accountId;
    containerId = preamble.containerId;
    isEncrypted = preamble.isEncrypted;
    blobDataType = preamble.blobDataType;
    partitionId = clusterMap.getPartitionIdFromStream(stream);
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition ID cannot be null");
    }
    switch (version) {
      case BLOB_ID_V6:
        uuid = UuidSerDe.deserialize(stream);
        uuidStr = null;
        break;
      default:
        uuid = null;
        uuidStr = Utils.readIntString(stream);
        break;
    }
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
    switch (version) {
      case BLOB_ID_V1:
        return (short) (VERSION_FIELD_LENGTH_IN_BYTES + partitionId.getBytes().length + UUID_SIZE_FIELD_LENGTH_IN_BYTES
            + getUuid().getBytes().length);
      case BLOB_ID_V2:
      case BLOB_ID_V3:
      case BLOB_ID_V4:
      case BLOB_ID_V5:
        return (short) (VERSION_FIELD_LENGTH_IN_BYTES + FLAG_FIELD_LENGTH_IN_BYTES + DATACENTER_ID_FIELD_LENGTH_IN_BYTES
            + ACCOUNT_ID_FIELD_LENGTH_IN_BYTES + CONTAINER_ID_FIELD_LENGTH_IN_BYTES + partitionId.getBytes().length
            + UUID_SIZE_FIELD_LENGTH_IN_BYTES + getUuid().getBytes().length);
      case BLOB_ID_V6:
        return (short) (VERSION_FIELD_LENGTH_IN_BYTES + FLAG_FIELD_LENGTH_IN_BYTES + DATACENTER_ID_FIELD_LENGTH_IN_BYTES
            + ACCOUNT_ID_FIELD_LENGTH_IN_BYTES + CONTAINER_ID_FIELD_LENGTH_IN_BYTES + partitionId.getBytes().length
            + UuidSerDe.SIZE_IN_BYTES);
      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
    }
  }

  /**
   * @return the version of this BlobId.
   */
  public short getVersion() {
    return version;
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
   * @return true if accountId and containerId in key match given accountId and containerId from store.
   * Always return true if BlobId version is {@link #BLOB_ID_V1}.
   */
  @Override
  public boolean isAccountContainerMatch(short accountId, short containerId) {
    if (getVersion() == BLOB_ID_V1) {
      return true;
    }
    return (this.accountId == accountId) && (this.containerId == containerId);
  }

  /**
   * Gets the id of the datacenter where this blob was originally posted. If this information was not available
   * when the blobId was formed, it will return {@link ClusterMapUtils#UNKNOWN_DATACENTER_ID}.
   * @return The id of the datacenter where this blob was originally posted.
   */
  public byte getDatacenterId() {
    return datacenterId;
  }

  /**
   * Gets the BlobId type of this blobId. If this information was not available when the blobId was formed, it
   * will return {@link BlobIdType#NATIVE}.
   * @return The BlobIdType of the blobId.
   */
  public BlobIdType getType() {
    return type;
  }

  /**
   * @return the {@link BlobDataType} for the blob.
   */
  public BlobDataType getBlobDataType() {
    return blobDataType;
  }

  /**
   * Check if encrypted bit in blobId is set based on original blobId string.
   * @return {@code true} if encrypted bit in this string is set. {@code false} otherwise
   * @throws IOException If parsing a string blobId fails.
   */
  public static boolean isEncrypted(String blobIdString) throws IOException {
    DataInputStream stream =
        new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(blobIdString))));
    return (stream.readShort() >= BLOB_ID_V3) && ((stream.readByte() & IS_ENCRYPTED_MASK) != 0);
  }

  /**
   * @return the uuid string associated with this BlobId
   */
  public String getUuid() {
    return uuid != null ? uuid.toString() : uuidStr;
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.putShort(version);
    byte flag;
    switch (version) {
      case BLOB_ID_V1:
        break;
      case BLOB_ID_V2:
      case BLOB_ID_V3:
        flag = (byte) (type.ordinal() & BLOB_ID_TYPE_MASK);
        idBuf.put(flag);
        idBuf.put(datacenterId);
        idBuf.putShort(accountId);
        idBuf.putShort(containerId);
        break;
      case BLOB_ID_V4:
        flag = (byte) (type.ordinal() & BLOB_ID_TYPE_MASK);
        flag |= isEncrypted ? IS_ENCRYPTED_MASK : 0;
        idBuf.put(flag);
        idBuf.put(datacenterId);
        idBuf.putShort(accountId);
        idBuf.putShort(containerId);
        break;
      case BLOB_ID_V5:
      case BLOB_ID_V6:
        flag = (byte) (type.ordinal() & BLOB_ID_TYPE_MASK);
        flag |= isEncrypted ? IS_ENCRYPTED_MASK : 0;
        flag |= (blobDataType.ordinal() << BLOB_DATA_TYPE_SHIFT);
        idBuf.put(flag);
        idBuf.put(datacenterId);
        idBuf.putShort(accountId);
        idBuf.putShort(containerId);
        break;
      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
    }
    idBuf.put(partitionId.getBytes());
    switch (version) {
      case BLOB_ID_V6:
        UuidSerDe.serialize(uuid, idBuf);
        break;
      default:
        byte[] uuidBytes = getUuid().getBytes();
        idBuf.putInt(uuidBytes.length);
        idBuf.put(uuidBytes);
        break;
    }
    return idBuf.array();
  }

  @Override
  public byte[] getUuidBytesArray() {
    ByteBuffer uuidBuf;
    switch (version) {
      case BLOB_ID_V1:
      case BLOB_ID_V2:
      case BLOB_ID_V3:
      case BLOB_ID_V4:
      case BLOB_ID_V5:
        byte[] uuidBytes = getUuid().getBytes();
        uuidBuf = ByteBuffer.allocate((short) uuidBytes.length);
        uuidBuf.put(uuidBytes);
        break;
      case BLOB_ID_V6:
        uuidBuf = ByteBuffer.allocate(UuidSerDe.SIZE_IN_BYTES);
        UuidSerDe.serialize(uuid, uuidBuf);
        break;
      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
    }
    return uuidBuf.array();
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
      case BLOB_ID_V3:
      case BLOB_ID_V4:
      case BLOB_ID_V5:
      case BLOB_ID_V6:
        sb.append(":").append(type);
        sb.append(":").append(datacenterId);
        sb.append(":").append(accountId);
        sb.append(":").append(containerId);
        break;
      default:
        throw new IllegalArgumentException("blobId version=" + version + " not supported");
    }
    sb.append(":").append(partitionId);
    sb.append(":").append(getUuid()).append("]");
    return sb.toString();
  }

  @Override
  public String toString() {
    return getID();
  }

  /**
   * Compare two BlobIds.
   * <br>
   * <br>
   * Starting with V3, only UUIDs will be used for comparison. UUID is the component ensuring uniqueness of
   * different blob ids. Rest of the information embedded in the BlobId is really the associated "metadata"
   * and is not meant for distinguishing blobs.
   * <br>
   * @param o the StoreKey to compare with.
   * @return 0 if this key is equal to the given key; a value less than 0 if this key is less than the given key;
   *         and a value greater than 0 if this key is greater than the given key.
   */
  @Override
  public int compareTo(StoreKey o) {
    if (this == o) {
      return 0;
    }
    BlobId other = (BlobId) o;
    int result = Short.compare(getVersionComparisonGroup(), other.getVersionComparisonGroup());
    if (result == 0) {
      switch (version) {
        case BLOB_ID_V1:
          result = partitionId.compareTo(other.partitionId);
          if (result == 0) {
            result = uuidStr.compareTo(other.uuidStr);
          }
          break;
        case BLOB_ID_V2:
          result = type.compareTo(other.type);
          if (result == 0) {
            result = Byte.compare(datacenterId, other.datacenterId);
            if (result == 0) {
              result = Short.compare(accountId, other.accountId);
              if (result == 0) {
                result = Short.compare(containerId, other.containerId);
                if (result == 0) {
                  result = partitionId.compareTo(other.partitionId);
                  if (result == 0) {
                    result = uuidStr.compareTo(other.uuidStr);
                  }
                }
              }
            }
          }
          break;
        case BLOB_ID_V3:
        case BLOB_ID_V4:
        case BLOB_ID_V5:
          result = uuidStr.compareTo(other.uuidStr);
          break;
        case BLOB_ID_V6:
          result = uuid.compareTo(other.uuid);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized blobId version " + version);
      }
    }
    return result;
  }

  /**
   * This gets a "version comparison group" to be used in the {@link #compareTo} method. If two blob IDs are in
   * different groups, they cannot be deemed equal to each other. This allows for comparison strategies that rely
   * on version-specific features or fields.
   * @return the "version comparison group" number.
   */
  private short getVersionComparisonGroup() {
    switch (version) {
      case BLOB_ID_V1:
        return 1;
      case BLOB_ID_V2:
        return 2;
      case BLOB_ID_V3:
      case BLOB_ID_V4:
      case BLOB_ID_V5:
        return 3;
      case BLOB_ID_V6:
        return 4;
      default:
        throw new IllegalArgumentException("Unrecognized blobId version " + version);
    }
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
    return compareTo(blobId) == 0;
  }

  @Override
  public int hashCode() {
    return (uuid != null ? uuid : uuidStr).hashCode();
  }

  /**
   * @return all valid versions of BlobId.
   */
  public static Short[] getAllValidVersions() {
    return new Short[]{BLOB_ID_V1, BLOB_ID_V2, BLOB_ID_V3, BLOB_ID_V4, BLOB_ID_V5, BLOB_ID_V6};
  }

  /**
   * Create a {@link BlobIdType#CRAFTED} BlobId for the given input BlobId.
   *
   * This method is useful in retrofitting account and container information to older blob ids, those that were created
   * before the concept of accounts and containers were introduced.
   *
   * The method is "idempotent" in the sense that if the input is a crafted id in the same version as the target version
   * for crafting with the same account and container associated with it as the account and container in the params,
   * then the returned id will be exactly the same as the input.
   * @param inputId The input BlobId for which a new BlobId is to be crafted. The input id can be of any version
   *                and of any type.
   * @param targetVersion the version in which the new blob id should be crafted.
   * @param accountId The id of the {@link Account} to be embedded in the converted id.
   * @param containerId The id of the {@link Container} to be embedded in the converted id.
   * @return The output BlobId will be a BlobId in the target version of type {@link BlobIdType#CRAFTED} with the given
   *         account id and container id association.
   */
  public static BlobId craft(BlobId inputId, short targetVersion, short accountId, short containerId) {
    if (targetVersion < BLOB_ID_V3) {
      throw new IllegalArgumentException("Target version for crafting must be V3 or higher");
    }
    return new BlobId(targetVersion, BlobIdType.CRAFTED, inputId.getDatacenterId(), accountId, containerId,
        inputId.partitionId, inputId.isEncrypted, inputId.blobDataType, inputId.getUuid());
  }

  /**
   * Returns whether a given Blob id is a crafted id.
   * @param idStr the blobId in string form.
   * @return true if the id is a crafted id; false otherwise.
   * @throws IOException if the input is not a valid Blob id.
   */
  public static boolean isCrafted(String idStr) throws IOException {
    BlobIdPreamble blobIdPreamble =
        new BlobIdPreamble(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(idStr)))));
    return blobIdPreamble.type == BlobIdType.CRAFTED;
  }

  /**
   * Returns the version of a given Blob id.
   * @param idStr the blobId in string form.
   * @return the blob ID version.
   * @throws IOException if the input is not a valid Blob id.
   */
  public static short getVersion(String idStr) throws IOException {
    BlobIdPreamble blobIdPreamble =
        new BlobIdPreamble(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(idStr)))));
    return blobIdPreamble.version;
  }

  /**
   * Returns the account id and container id associated with the given blob. Note that the blob id may not have a valid
   * account and container id associated with it, in which case this will return {@link Account#UNKNOWN_ACCOUNT_ID} and
   * {@link Container#UNKNOWN_CONTAINER_ID} respectively.
   * @param idStr the id of the blob for which the account and container ids are to be fetched.
   * @return a {@link Pair} whose first value is the account id and the second value is the container id of this blob.
   * @throws IOException if the input is not a valid Blob id.
   */
  public static Pair<Short, Short> getAccountAndContainerIds(String idStr) throws IOException {
    BlobIdPreamble blobIdPreamble =
        new BlobIdPreamble(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(idStr)))));
    return new Pair<>(blobIdPreamble.accountId, blobIdPreamble.containerId);
  }

  /**
   * Returns the blob data type of a given Blob id.
   * @param idStr the blobId in string form.
   * @return the {@Link BlobDataType} indicating the data type of the blob.
   * @throws IOException if the input is not a valid Blob id.
   */
  public static BlobDataType getBlobDataType(String idStr) throws IOException {
    BlobIdPreamble blobIdPreamble =
        new BlobIdPreamble(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(idStr)))));
    return blobIdPreamble.blobDataType;
  }

  /**
   * Indicates the context in which a {@link BlobId} gets created.
   */
  public enum BlobIdType {
    /**
     * Indicates natively created BlobId (in the context of a PUT operation)
     */
    NATIVE,

    /**
     * Indicates BlobId that was crafted (for example, converted from an older version) and not natively created in the
     * context of a PUT operation.
     */
    CRAFTED
  }

  /**
   * Indicates the type of blob this is.
   */
  public enum BlobDataType {
    /**
     * Indicates a fully contained blob.
     */
    SIMPLE,

    /**
     * Indicates a composite (metadata) blob.
     */
    METADATA,

    /**
     * Indicates a data chunk within a composite blob.
     */
    DATACHUNK
  }

  /**
   * A serde to store UUIDs in a more compact byte representation than their canonical string representation.
   */
  private static class UuidSerDe {
    static final int SIZE_IN_BYTES = 2 * Long.BYTES;

    /**
     * Write a compact representation of a {@link UUID} into a {@link ByteBuffer}. This will advance the buffer cursor.
     * @param uuid the {@link UUID} to serialize.
     * @param buf the {@link ByteBuffer} to write into.
     */
    static void serialize(UUID uuid, ByteBuffer buf) {
      buf.putLong(uuid.getMostSignificantBits());
      buf.putLong(uuid.getLeastSignificantBits());
    }

    /**
     * Deserialize a compact UUID from a {@link DataInputStream}.
     * @param stream the {@link DataInputStream}.
     * @return the {@link UUID}.
     * @throws IOException if there are errors reading from the stream.
     */
    static UUID deserialize(DataInputStream stream) throws IOException {
      long mostSigBits = stream.readLong();
      long leastSigBits = stream.readLong();
      return new UUID(mostSigBits, leastSigBits);
    }
  }

  /**
   * A class that can hold all the information embedded in a BlobId up to and not including the {@link PartitionId}
   * The preamble can be parsed off a blob id string without a {@link ClusterMap}.
   */
  private static class BlobIdPreamble {
    final short version;
    final BlobIdType type;
    final byte datacenterId;
    final short accountId;
    final short containerId;
    final boolean isEncrypted;
    final BlobDataType blobDataType;

    /**
     * Construct a BlobIdPreamble object by reading all the fields from a BlobId up to and not including the
     * {@link PartitionId}.
     * @param stream the {@link DataInputStream} from which to read.
     * @throws IOException if there is an error reading from the stream.
     */
    BlobIdPreamble(DataInputStream stream) throws IOException {
      version = stream.readShort();
      byte blobIdFlag;
      switch (version) {
        case BLOB_ID_V1:
          type = BlobIdType.NATIVE;
          datacenterId = UNKNOWN_DATACENTER_ID;
          accountId = UNKNOWN_ACCOUNT_ID;
          containerId = UNKNOWN_CONTAINER_ID;
          isEncrypted = false;
          blobDataType = null;
          break;
        case BLOB_ID_V2:
          stream.readByte();
          type = BlobIdType.NATIVE;
          datacenterId = stream.readByte();
          accountId = stream.readShort();
          containerId = stream.readShort();
          isEncrypted = false;
          blobDataType = null;
          break;
        case BLOB_ID_V3:
          blobIdFlag = stream.readByte();
          type = BlobIdType.values()[blobIdFlag & BLOB_ID_TYPE_MASK];
          datacenterId = stream.readByte();
          accountId = stream.readShort();
          containerId = stream.readShort();
          isEncrypted = false;
          blobDataType = null;
          break;
        case BLOB_ID_V4:
          blobIdFlag = stream.readByte();
          type = BlobIdType.values()[blobIdFlag & BLOB_ID_TYPE_MASK];
          isEncrypted = (blobIdFlag & IS_ENCRYPTED_MASK) != 0;
          datacenterId = stream.readByte();
          accountId = stream.readShort();
          containerId = stream.readShort();
          blobDataType = null;
          break;
        case BLOB_ID_V5:
        case BLOB_ID_V6:
          blobIdFlag = stream.readByte();
          type = BlobIdType.values()[blobIdFlag & BLOB_ID_TYPE_MASK];
          isEncrypted = (blobIdFlag & IS_ENCRYPTED_MASK) != 0;
          datacenterId = stream.readByte();
          accountId = stream.readShort();
          containerId = stream.readShort();
          int dataTypeOrdinal = (blobIdFlag & BLOB_DATA_TYPE_MASK) >> BLOB_DATA_TYPE_SHIFT;
          blobDataType = BlobDataType.values()[dataTypeOrdinal];
          break;
        default:
          throw new IllegalArgumentException("blobId version " + version + " is not supported.");
      }
    }
  }
}

