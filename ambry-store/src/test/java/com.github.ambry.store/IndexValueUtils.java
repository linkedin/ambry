/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;


/**
 * IndexValue utils to assist in constructing {@link IndexValue} in different versions. For
 * {@link PersistentIndex#VERSION_1}, all args are used to construct the {@link IndexValue} and for
 * {@link PersistentIndex#VERSION_0} unnecessary args are dropped to construct the {@link IndexValue}
 */
class IndexValueUtils {

  /**
   * Constructs IndexValue based on the args passed and for the given version
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param operationTimeInSecs operation time of the entry
   * @param serviceId the serviceId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, long operationTimeInSecs, short serviceId,
      short containerId, short version) {
    return getIndexValue(size, offset, Utils.Infinite_Time, operationTimeInSecs, serviceId, containerId, version);
  }

  /**
   * Constructs IndexValue based on the args passed and for the given version
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expirationTimeInMs the expiration time in ms at which the blob expires
   * @param operationTimeInSecs operation time of the entry
   * @param serviceId the serviceId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, long expirationTimeInMs, long operationTimeInSecs,
      short serviceId, short containerId, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset, expirationTimeInMs);
    } else {
      return new IndexValue(size, offset, (byte) 0, expirationTimeInMs, operationTimeInSecs, serviceId, containerId);
    }
  }

  /**
   * Constructs IndexValue based on the args passed and for the given version
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param operationTimeInSecs operation time of the entry
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, long operationTimeInSecs, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset);
    } else {
      return new IndexValue(size, offset, (byte) 0, Utils.Infinite_Time, operationTimeInSecs);
    }
  }

  /**
   * Constructs IndexValue based on another {@link IndexValue}
   * @param value the {@link IndexValue} using which to create another {@link IndexValue}
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(IndexValue value, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs());
    } else {
      return new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
          value.getOperationTimeInSecs(), value.getServiceId(), value.getContainerId());
    }
  }

  // Constructors of {@link IndexValue} in version {@link PersistentIndex#VERSION_0}

  /**
   * Constructs IndexValue based on the size and {@link Offset} for {@link PersistentIndex#VERSION_0}
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @return the {@link IndexValue} thus constructed
   */
  private static IndexValue getIndexValue(long size, Offset offset) {
    return getIndexValue(size, offset, (byte) 0, Utils.Infinite_Time, offset.getOffset());
  }

  /**
   * Constructs IndexValue based on the args passed in version {@link PersistentIndex#VERSION_0}
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param originalMessageOffset the original message offset where the Put record pertaining to a delete record exists
   *                              in the same log segment. Set to -1 otherwise.
   * @return the {@link IndexValue} thus constructed
   */
  private static IndexValue getIndexValue(long size, Offset offset, byte flags, long expiresAtMs,
      long originalMessageOffset) {
    int Blob_Size_In_Bytes = 8;
    int Offset_Size_In_Bytes = 8;
    int Flag_Size_In_Bytes = 1;
    int Expires_At_Ms_Size_In_Bytes = 8;
    int Original_Message_Offset_Size_In_Bytes = 8;
    int Index_Value_Size_In_Bytes =
        Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes + Expires_At_Ms_Size_In_Bytes
            + Original_Message_Offset_Size_In_Bytes;
    ByteBuffer value = ByteBuffer.allocate(Index_Value_Size_In_Bytes);
    value.putLong(size);
    value.putLong(offset.getOffset());
    value.put(flags);
    value.putLong(expiresAtMs);
    value.putLong(originalMessageOffset);
    value.position(0);
    return new IndexValue(offset.getName(), value, (short) PersistentIndex.VERSION_0);
  }

  /**
   * Constructs IndexValue based on the args passed in the version {@link PersistentIndex#VERSION_0}
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param flags the flags to be set in the {@link IndexValue}
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @return the {@link IndexValue} thus constructed
   */
  private static IndexValue getIndexValue(long size, Offset offset, byte flags, long expiresAtMs) {
    return getIndexValue(size, offset, flags, expiresAtMs, offset.getOffset());
  }

  /**
   * Constructs IndexValue based on the args passed in the version {@link PersistentIndex#VERSION_0}
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @return the {@link IndexValue} thus constructed
   */
  private static IndexValue getIndexValue(long size, Offset offset, long expiresAtMs) {
    return getIndexValue(size, offset, (byte) 0, expiresAtMs, offset.getOffset());
  }
}
