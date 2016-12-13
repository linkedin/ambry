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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * Refers to the Store descriptor of a {@link BlobStore} having some metadata information about each store.
 * As of now, store descriptor stores the incarnation Id which is a unique identifier for every new
 * incarnation of the store.
 */
class StoreDescriptor {
  private final UUID incarnationId;
  public static final short VERSION_0 = 0;
  public static final int VERSION_SIZE = 2;
  public static final int INCARNATION_ID_LENGTH_SIZE = 4;
  public static final String STORE_DESCRIPTOR = "StoreDescriptor";

  /**
   * Instantiates the {@link StoreDescriptor} for the store. If the respective file is present, reads the bytes
   * to understand the storeId and the incarnationId. If not, creates a new one with a random Unique identifier.
   * @param dataDir the directory path to locate the Store Descriptor file
   * @throws IOException
   */
  public StoreDescriptor(String dataDir) throws IOException {
    File storeDescriptorFile = new File(dataDir, StoreDescriptor.STORE_DESCRIPTOR);
    if (storeDescriptorFile.exists()) {
      DataInputStream dataInputStream = new DataInputStream(new FileInputStream(storeDescriptorFile));
      short version = dataInputStream.readShort();
      switch (version) {
        case VERSION_0:
          // read incarnationId
          String incarnationId = Utils.readIntString(dataInputStream);
          this.incarnationId = UUID.fromString(incarnationId);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized version in StoreDescriptor: " + version);
      }
    } else {
      this.incarnationId = UUID.randomUUID();
      storeDescriptorFile.createNewFile();
      DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(storeDescriptorFile));
      dataOutputStream.write(toBytes());
      dataOutputStream.close();
    }
  }

  public UUID getIncarnationId() {
    return incarnationId;
  }

  /**
   * Returns the serialized form of this {@link StoreDescriptor}
   * @return the serialized form of this {@link StoreDescriptor}
   */
  private byte[] toBytes() {
    byte[] incarnationIdBytes = incarnationId.toString().getBytes();
    int size = VERSION_SIZE + INCARNATION_ID_LENGTH_SIZE +
        incarnationIdBytes.length;
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort(VERSION_0);
    // add incarnationId
    bufWrap.putInt(incarnationIdBytes.length);
    bufWrap.put(incarnationIdBytes);
    return buf;
  }
}
