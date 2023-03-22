package com.github.ambry.store;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.codec.binary.Base64;

import static com.github.ambry.commons.BlobId.*;


public class CompactBlobId extends StoreKey {
  private final short version;
  private final BlobId.BlobIdType type;
  private final byte datacenterId;
  private final short accountId;
  private final short containerId;
  private final byte[] uuid;
  private final boolean isEncrypted;
  private final BlobId.BlobDataType blobDataType;

  private static final int BLOB_ID_TYPE_MASK = 0x3;
  private static final int IS_ENCRYPTED_MASK = 0x4;
  private static final int BLOB_DATA_TYPE_MASK = 0x18;
  private static final int BLOB_DATA_TYPE_SHIFT = 3;
  private static final int DATACENTER_ID_MASK = 0xe0;
  private static final int DATACENTER_ID_SHIFT = 5;
  private static final int DATACENTER_ID_MAX = 1 << (Byte.SIZE - DATACENTER_ID_SHIFT);

  public static StoreKey fromStoreKey(StoreKey key) {
    if (key == null || !(key instanceof BlobId)) {
      return key;
    }
    BlobId blobId = (BlobId) key;
    if (blobId.getVersion() != BLOB_ID_V6) {
      throw new IllegalArgumentException("Blob id " + blobId + " is not at version 6");
    }
    if (blobId.getDatacenterId() >= DATACENTER_ID_MAX) {
      throw new IllegalArgumentException(
          "Blob id " + blobId + " datacenter id " + blobId.getDatacenterId() + " is greater then the max number: "
              + DATACENTER_ID_MAX);
    }
    return new CompactBlobId(blobId.getVersion(), blobId.getType(), blobId.getDatacenterId(), blobId.getAccountId(),
        blobId.getContainerId(), blobId.getUuidBytesArray(), blobId.isEncrypted(), blobId.getBlobDataType());
  }

  public static StoreKey toStoreKey(StoreKey key, ReplicaId replicaId) {
    if (key == null || !(key instanceof CompactBlobId)) {
      return key;
    }
    CompactBlobId cBlobId = (CompactBlobId) key;
    return new BlobId(cBlobId.version, cBlobId.type, cBlobId.datacenterId, cBlobId.accountId, cBlobId.containerId,
        replicaId.getPartitionId(), cBlobId.isEncrypted, cBlobId.blobDataType, cBlobId.uuid.toString());
  }

  public CompactBlobId(DataInputStream input) throws IOException {
    version = input.readByte();
    switch (version) {
      case BLOB_ID_V6:
        byte flag = input.readByte();
        type = BlobIdType.values()[flag & BLOB_DATA_TYPE_MASK];
        isEncrypted = (flag & IS_ENCRYPTED_MASK) == 0;
        blobDataType = BlobDataType.values()[(flag & BLOB_DATA_TYPE_MASK) >> BLOB_DATA_TYPE_SHIFT];
        datacenterId = (byte) ((flag & DATACENTER_ID_MASK) >> DATACENTER_ID_SHIFT);
        accountId = input.readShort();
        containerId = readVarshort(input);
        byte[] bytes = new byte[UuidSerDe.SIZE_IN_BYTES];
        input.read(bytes);
        uuid = bytes;
      default:
        throw new IllegalArgumentException("blobId version " + version + " is not supported.");
    }
  }

  public CompactBlobId(short version, BlobId.BlobIdType type, byte datacenterId, short accountId, short containerId,
      byte[] uuidBytes, boolean isEncrypted, BlobId.BlobDataType blobDataType) {
    this.version = version;
    this.type = type;
    this.datacenterId = datacenterId;
    this.accountId = accountId;
    this.containerId = containerId;
    this.uuid = uuidBytes;
    this.isEncrypted = isEncrypted;
    this.blobDataType = blobDataType;
  }

  @Override
  public short sizeInBytes() {
    switch (version) {
      case BlobId.BLOB_ID_V6:
        //@formatter:off
        return (short)(1 + // version
               1 + // id type + isEncrypted + data type + datacenter id
               2 + // account id
               3 + // container id varint, 3 is the max size
               uuid.length);
        //@formatter:on
      default:
        throw new IllegalArgumentException("BlobId version=" + version + " not supported");
    }
  }

  @Override
  public String getID() {
    return Base64.encodeBase64URLSafeString(toBytes());
  }

  @Override
  public boolean isAccountContainerMatch(short accountId, short containerId) {
    return this.accountId == accountId && this.containerId == containerId;
  }

  @Override
  public short getAccountId() {
    return this.accountId;
  }

  @Override
  public short getContainerId() {
    return this.containerId;
  }

  @Override
  public String getLongForm() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(getID());
    sb.append(":").append(version);
    sb.append(":").append(type);
    sb.append(":").append(datacenterId);
    sb.append(":").append(accountId);
    sb.append(":").append(containerId);
    sb.append(":").append(uuid.toString()).append("]");
    return sb.toString();
  }

  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(sizeInBytes());
    idBuf.put((byte) (version & 0xFF));
    byte flag;
    flag = (byte) (type.ordinal() & BLOB_ID_TYPE_MASK);
    flag |= isEncrypted ? IS_ENCRYPTED_MASK : 0;
    flag |= (blobDataType.ordinal() << BLOB_DATA_TYPE_SHIFT);
    flag |= ((datacenterId & 0XFF) << DATACENTER_ID_SHIFT);
    idBuf.put(flag);
    idBuf.putShort(accountId);
    putVarshort(idBuf, containerId);
    idBuf.put(uuid);
    return idBuf.array();
  }

  @Override
  public byte[] getUuidBytesArray() {
    return this.uuid;
  }

  static void putVarshort(ByteBuffer buffer, short containerId) {
    byte b;
    while (true) {
      b = (byte) (containerId & 0x7F);
      containerId >>= 7;
      if (containerId != 0) {
        b |= 0x80;
        buffer.put(b);
      } else {
        buffer.put(b);
        break;
      }
    }
  }

  static short readVarshort(DataInputStream input) throws IOException {
    short ret = 0;
    int i = 0;
    int b = 0;
    while (true) {
      b = input.read() & 0xFF;
      if ((b & 0x80) == 0) {
        break;
      }
      ret |= b << (i * 7);
      i++;
    }
    if (i > 2) {
      throw new IllegalStateException("Failed to construct var short");
    }
    ret |= b << (i * 7);
    return ret;
  }

  @Override
  public int compareTo(StoreKey o) {
    return 0;
  }
}
