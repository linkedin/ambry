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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Tests {@link StoreDescriptor}
 */
public class StoreDescriptorTest {

  /**
   * Tests {@link StoreDescriptor} for unit tests for instantiation and converting bytes into StoreDescriptor
   * @throws IOException
   */
  @Test
  public void testStoreDescriptor() throws IOException {
    String tempDirPath = "/tmp/";
    String storeId = "demoStoreId";
    StoreDescriptor storeDescriptor = new StoreDescriptor(tempDirPath, storeId);
    // store descriptor file should have been created.
    StoreDescriptor newStoreDescriptor = new StoreDescriptor(tempDirPath, storeId);
    Assert.assertEquals("StoreId mismatch ", storeDescriptor.getStoreId(), newStoreDescriptor.getStoreId());
    Assert.assertEquals("IncarnationId mismatch ", storeDescriptor.getIncarnationId(),
        newStoreDescriptor.getIncarnationId());

    // read the file to fetch storeId and incarnationId
    newStoreDescriptor = StoreDescriptor.fromBytes(
        new DataInputStream(new FileInputStream(new File(tempDirPath, StoreDescriptor.STORE_DESCRIPTOR))));
    Assert.assertEquals("StoreId mismatch ", storeDescriptor.getStoreId(), newStoreDescriptor.getStoreId());
    Assert.assertEquals("IncarnationId mismatch ", storeDescriptor.getIncarnationId(),
        newStoreDescriptor.getIncarnationId());

    // Create StoreDescriptor file with explicit store Id and incarnationId
    storeId = "demoStoreId2";
    UUID incarnationId = UUID.randomUUID();
    int size =
        StoreDescriptor.VERSION_SIZE + StoreDescriptor.STORE_ID_LENGTH_SIZE + storeId.toString().getBytes().length +
            StoreDescriptor.INCARNATION_ID_LENGTH_SIZE + incarnationId.toString().getBytes().length;
    byte[] toBytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes);
    byteBuffer.putShort(StoreDescriptor.VERSION_0);
    byteBuffer.putInt(storeId.toString().getBytes().length);
    byteBuffer.put(storeId.toString().getBytes());
    byteBuffer.putInt(incarnationId.toString().getBytes().length);
    byteBuffer.put(incarnationId.toString().getBytes());
    byteBuffer.flip();
    storeDescriptor = StoreDescriptor.fromBytes(new DataInputStream(new ByteBufferInputStream(byteBuffer)));
    Assert.assertEquals("StoreId mismatch ", storeId, storeDescriptor.getStoreId());
    Assert.assertEquals("IncarnationId mismatch ", incarnationId, storeDescriptor.getIncarnationId());

    // version mismatch
    size = StoreDescriptor.VERSION_SIZE + StoreDescriptor.STORE_ID_LENGTH_SIZE + storeId.toString().getBytes().length +
        StoreDescriptor.INCARNATION_ID_LENGTH_SIZE + incarnationId.toString().getBytes().length;
    toBytes = new byte[size];
    byteBuffer = ByteBuffer.wrap(toBytes);
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
