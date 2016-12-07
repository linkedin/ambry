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
package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * Refers to the Store descriptor of a {@link BlobStore} having some metadata information about each store.
 * As of now, store descriptor has storeId and incarnation Id. IncarnationId is a unique identifier for every new
 * incarnation of the store. The contents of store descriptor is expected to evolve.
 */
class StoreDescriptor {
  String storeId;
  UUID incarnationId;
  public static final int VERSION = 0;
  public static final int VERSION_SIZE = 2;
  public static final int STORE_ID_SIZE = 4;
  public static final int INCARNATION_ID_SIZE = 4;

  StoreDescriptor(String storeId, UUID incarnationId) {
    this.storeId = storeId;
    this.incarnationId = incarnationId;
  }

  /**
   * Deserialize the {@link StoreDescriptor} from the stream
   * @param stream the stream containing the store descriptor
   * @return the deserialized {@link StoreDescriptor}
   * @throws IOException
   */
  static StoreDescriptor fromBytes(DataInputStream stream) throws IOException {
    StoreDescriptor storeDescriptor = null;
    short version = stream.readShort();
    switch (version) {
      case 0:
        String storeId = Utils.readIntString(stream);
        UUID incarnationIdUUID = null;
        // read incarnationId
        String incarnationId = Utils.readIntString(stream);
        if (!incarnationId.isEmpty()) {
          incarnationIdUUID = UUID.fromString(incarnationId);
        }
        storeDescriptor = new StoreDescriptor(storeId, incarnationIdUUID);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version in StoreDescriptor: " + version);
    }
    return storeDescriptor;
  }

  /**
   * Returns the serialized form of this {@link StoreDescriptor}
   * @return the serialized form of this {@link StoreDescriptor}
   */
  byte[] toBytes() {
    int size = VERSION_SIZE + STORE_ID_SIZE + storeId.length() + INCARNATION_ID_SIZE +
        ((incarnationId == null) ? 0 : incarnationId.toString().getBytes().length);
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort((short) VERSION);
    bufWrap.putInt(storeId.length());
    bufWrap.put(storeId.getBytes());
    // add incarnationId
    byte[] incarnationIdInBytes = incarnationId == null ? null : incarnationId.toString().getBytes();
    bufWrap.putInt(incarnationIdInBytes == null ? 0 : incarnationIdInBytes.length);
    if (incarnationIdInBytes != null) {
      bufWrap.put(incarnationIdInBytes);
    }
    return buf;
  }
}