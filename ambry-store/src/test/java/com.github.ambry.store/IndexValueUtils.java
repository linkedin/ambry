package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;

import static com.github.ambry.store.IndexValue.*;


public class IndexValueUtils {

  private static IndexValue getIndexValue(long size, Offset offset) {
    return getIndexValue(size, offset, (byte) 0, Utils.Infinite_Time, offset.getOffset());
  }

  private static IndexValue getIndexValue(long size, Offset offset, byte flags, long expiresAtMs,
      long originalMessageOffset) {
    ByteBuffer value = ByteBuffer.allocate(INDEX_VALUE_SIZE_IN_BYTES_V0);
    value.putLong(size);
    value.putLong(offset.getOffset());
    value.put(flags);
    value.putLong(expiresAtMs);
    value.putLong(originalMessageOffset);
    value.position(0);
    return new IndexValue(offset.getName(), value, (short) PersistentIndex.VERSION_0);
  }

  private static IndexValue getIndexValue(long size, Offset offset, byte flags, long expiresAtMs) {
    return getIndexValue(size, offset, flags, expiresAtMs, offset.getOffset());
  }

  private static IndexValue getIndexValue(long size, Offset offset, long expiresAtMs) {
    return getIndexValue(size, offset, (byte) 0, expiresAtMs, offset.getOffset());
  }

  static IndexValue getIndexValue(long size, Offset offset, long operationTimeInSecs, short serviceId,
      short containerId, short version) {
    return getIndexValue(size, offset, Utils.Infinite_Time, operationTimeInSecs, serviceId, containerId, version);
  }

  static IndexValue getIndexValue(long size, Offset offset, long expirationTimeInMs, long operationTimeInSecs,
      short serviceId, short containerId, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset, expirationTimeInMs);
    } else {
      return new IndexValue(size, offset, expirationTimeInMs, operationTimeInSecs, serviceId, containerId);
    }
  }

  static IndexValue getIndexValue(long size, Offset offset, byte flags, long expirationTimeInMs,
      long operationTimeInSecs, short serviceId, short containerId, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset, flags, expirationTimeInMs);
    } else {
      return new IndexValue(size, offset, flags, expirationTimeInMs, operationTimeInSecs, serviceId, containerId);
    }
  }

  static IndexValue getIndexValue(long size, Offset offset, long expirationTimeInMs, long operationTimeInSecs,
      short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset, expirationTimeInMs);
    } else {
      return new IndexValue(size, offset, expirationTimeInMs, operationTimeInSecs);
    }
  }

  static IndexValue getIndexValue(long size, Offset offset, long operationTimeInSecs, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset);
    } else {
      return new IndexValue(size, offset, operationTimeInSecs);
    }
  }

  static IndexValue getIndexValue(IndexValue value, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs());
    } else {
      return new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
          value.getOperationTimeInSecs(), value.getServiceId(), value.getContainerId());
    }
  }
}
