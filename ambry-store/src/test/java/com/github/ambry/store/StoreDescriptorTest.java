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

import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Utils;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.*;


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
    StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
    File tempDir = StoreTestUtils.createTempDirectory("storeDir");
    File storeDescriptorFile = new File(tempDir.getAbsolutePath(), StoreDescriptor.STORE_DESCRIPTOR_FILENAME);
    StoreDescriptor storeDescriptor = new StoreDescriptor(tempDir.getAbsolutePath(), config);
    // store descriptor file should have been created.
    StoreDescriptor newStoreDescriptor = new StoreDescriptor(tempDir.getAbsolutePath(), config);
    assertEquals("IncarnationId mismatch ", storeDescriptor.getIncarnationId(), newStoreDescriptor.getIncarnationId());

    assertTrue("Store descriptor file could not be deleted", storeDescriptorFile.delete());
    // Create StoreDescriptor file with new incarnationId
    UUID incarnationIdUUID = UUID.randomUUID();
    byte[] toBytes = getBytesForStoreDescriptor(StoreDescriptor.VERSION_0, incarnationIdUUID);

    storeDescriptorFile = new File(tempDir.getAbsolutePath(), StoreDescriptor.STORE_DESCRIPTOR_FILENAME);
    assertTrue("Store descriptor file could not be created", storeDescriptorFile.createNewFile());
    createStoreFile(storeDescriptorFile, toBytes);

    storeDescriptor = new StoreDescriptor(tempDir.getAbsolutePath(), config);
    assertEquals("IncarnationId mismatch ", incarnationIdUUID, storeDescriptor.getIncarnationId());

    // check for wrong version
    assertTrue("Store descriptor file could not be deleted", storeDescriptorFile.delete());
    toBytes = getBytesForStoreDescriptor((short) 1, incarnationIdUUID);
    assertTrue("Store descriptor file could not be created", storeDescriptorFile.createNewFile());
    createStoreFile(storeDescriptorFile, toBytes);
    try {
      new StoreDescriptor(tempDir.getAbsolutePath(), config);
      fail("Wrong version should have thrown IllegalArgumentException ");
    } catch (IllegalArgumentException e) {
    }

    // check for wrong Crc
    assertTrue("Store descriptor file could not be deleted", storeDescriptorFile.delete());
    assertTrue("Store descriptor file could not be created", storeDescriptorFile.createNewFile());
    toBytes = getBytesForStoreDescriptor(StoreDescriptor.VERSION_0, incarnationIdUUID);
    CrcOutputStream crcOutputStream = new CrcOutputStream(new FileOutputStream(storeDescriptorFile));
    DataOutputStream dataOutputStream = new DataOutputStream(crcOutputStream);
    dataOutputStream.write(toBytes);
    dataOutputStream.writeLong(crcOutputStream.getValue() + 1);
    dataOutputStream.close();
    try {
      new StoreDescriptor(tempDir.getAbsolutePath(), config);
      fail("Wrong CRC should have thrown IllegalStateException ");
    } catch (IllegalStateException e) {
    }
  }

  /**
   * Generates the byte array value for a given version and incarnationId
   * @param version the version to be used while writing
   * @param incarnationIdUUID the incarnationId of the store
   * @return the byte array representation of the store descriptor (excluding the crc)
   */
  private byte[] getBytesForStoreDescriptor(short version, UUID incarnationIdUUID) {
    int size = StoreDescriptor.VERSION_SIZE + StoreDescriptor.INCARNATION_ID_LENGTH_SIZE + incarnationIdUUID.toString()
        .getBytes().length;
    byte[] toBytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes);
    byteBuffer.putShort(version);
    Utils.serializeString(byteBuffer, incarnationIdUUID.toString(), Charset.defaultCharset());
    byteBuffer.flip();
    return byteBuffer.array();
  }

  /**
   * Creates a new StoreDescriptor file with the given byte array as content
   * @param storeDescriptorFile the store descriptor file
   * @param toBytes content that needs to go into the file
   * @throws IOException
   */
  private void createStoreFile(File storeDescriptorFile, byte[] toBytes) throws IOException {
    CrcOutputStream crcOutputStream = new CrcOutputStream(new FileOutputStream(storeDescriptorFile));
    DataOutputStream dataOutputStream = new DataOutputStream(crcOutputStream);
    dataOutputStream.write(toBytes);
    dataOutputStream.writeLong(crcOutputStream.getValue());
    dataOutputStream.close();
  }
}
