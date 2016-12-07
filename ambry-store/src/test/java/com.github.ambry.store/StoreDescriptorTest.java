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

import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Tests {@link StoreDescriptor}
 */
public class StoreDescriptorTest {

  @Test
  public void testStoreDescriptor() throws IOException {
    String storeId = "demoId";
    UUID incarnationId = UUID.randomUUID();
    StoreDescriptor storeDescriptor = new StoreDescriptor(storeId, incarnationId);

    byte[] toBytes = storeDescriptor.toBytes();
    storeDescriptor =
        StoreDescriptor.fromBytes(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(toBytes))));
    Assert.assertEquals("StoreId mismatch ", storeId, storeDescriptor.storeId);
    Assert.assertEquals("IncarnationId mismatch ", incarnationId, storeDescriptor.incarnationId);

    incarnationId = null;
    storeDescriptor = new StoreDescriptor(storeId, incarnationId);
    Assert.assertEquals("StoreId mismatch ", storeId, storeDescriptor.storeId);
    Assert.assertEquals("IncarnationId mismatch ", incarnationId, storeDescriptor.incarnationId);

    toBytes = storeDescriptor.toBytes();
    storeDescriptor =
        StoreDescriptor.fromBytes(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(toBytes))));
    Assert.assertEquals("StoreId mismatch ", storeId, storeDescriptor.storeId);
    Assert.assertEquals("IncarnationId mismatch ", incarnationId, storeDescriptor.incarnationId);

    // version mismatch
    incarnationId = UUID.randomUUID();
    int size = StoreDescriptor.VERSION_SIZE + StoreDescriptor.STORE_ID_SIZE + storeId.toString().getBytes().length +
        StoreDescriptor.INCARNATION_ID_SIZE + incarnationId.toString().getBytes().length;
    toBytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes);
    byteBuffer.putShort((short) 2);
    byteBuffer.putInt(storeId.toString().getBytes().length);
    byteBuffer.put(storeId.toString().getBytes());
    byteBuffer.putInt(incarnationId.toString().getBytes().length);
    byteBuffer.put(incarnationId.toString().getBytes());
    byteBuffer.flip();

    try {
      StoreDescriptor.fromBytes(new DataInputStream(new ByteBufferInputStream(byteBuffer)));
    } catch (IllegalArgumentException e) {

    }
  }
}
