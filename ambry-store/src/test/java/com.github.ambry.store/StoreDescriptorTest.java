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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
    File storeDescriptorFile = new File(tempDirPath + "/" + StoreDescriptor.STORE_DESCRIPTOR);
    storeDescriptorFile.delete();

    StoreDescriptor storeDescriptor = new StoreDescriptor(tempDirPath);
    // store descriptor file should have been created.
    StoreDescriptor newStoreDescriptor = new StoreDescriptor(tempDirPath);
    Assert.assertEquals("IncarnationId mismatch ", storeDescriptor.getIncarnationId(),
        newStoreDescriptor.getIncarnationId());

   /* // read the file to fetch storeId and incarnationId
    DataInputStream stream = new DataInputStream(new FileInputStream(new File(tempDirPath, StoreDescriptor.STORE_DESCRIPTOR)));
    short version = stream.readShort();
    Assert.assertEquals("Version mismatch ", StoreDescriptor.VERSION_0, version);
    // read incarnationId
    String incarnationId = Utils.readIntString(stream);
    UUID incarnationIdUUID = UUID.fromString(incarnationId);
    Assert.assertEquals("IncarnationId mismatch", newStoreDescriptor.getIncarnationId(), incarnationIdUUID);
*/

    // Create StoreDescriptor file with new incarnationId
    UUID incarnationIdUUID = UUID.randomUUID();
    int size = StoreDescriptor.VERSION_SIZE +
        StoreDescriptor.INCARNATION_ID_LENGTH_SIZE + incarnationIdUUID.toString().getBytes().length;
    byte[] toBytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes);
    byteBuffer.putShort(StoreDescriptor.VERSION_0);
    byteBuffer.putInt(incarnationIdUUID.toString().getBytes().length);
    byteBuffer.put(incarnationIdUUID.toString().getBytes());
    byteBuffer.flip();

    storeDescriptorFile = new File(tempDirPath + "/" + StoreDescriptor.STORE_DESCRIPTOR);
    storeDescriptorFile.createNewFile();
    DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(storeDescriptorFile));
    dataOutputStream.write(toBytes);
    dataOutputStream.close();

    storeDescriptor = new StoreDescriptor(tempDirPath);
    Assert.assertEquals("IncarnationId mismatch ", incarnationIdUUID, storeDescriptor.getIncarnationId());

    // wrong version
    size = StoreDescriptor.VERSION_SIZE +
        StoreDescriptor.INCARNATION_ID_LENGTH_SIZE + incarnationIdUUID.toString().getBytes().length;
    toBytes = new byte[size];
    byteBuffer = ByteBuffer.wrap(toBytes);
    byteBuffer.putShort((short) 1);
    byteBuffer.putInt(incarnationIdUUID.toString().getBytes().length);
    byteBuffer.put(incarnationIdUUID.toString().getBytes());
    byteBuffer.flip();

    storeDescriptorFile = new File(tempDirPath + "/" + StoreDescriptor.STORE_DESCRIPTOR);
    storeDescriptorFile.createNewFile();
    dataOutputStream = new DataOutputStream(new FileOutputStream(storeDescriptorFile));
    dataOutputStream.write(toBytes);
    dataOutputStream.close();

    try {
      storeDescriptor = new StoreDescriptor(tempDirPath);
      Assert.fail("Wrong version should have thrown exception ");
    } catch (IllegalArgumentException e) {

    }
  }
}
