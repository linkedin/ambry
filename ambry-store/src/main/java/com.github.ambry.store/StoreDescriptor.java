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
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;


/**
 * Refers to the Store descriptor of a {@link BlobStore} having some metadata information about each store.
 * As of now, store descriptor stores the incarnation Id which is a unique identifier for every new
 * incarnation of the store.
 */
class StoreDescriptor {
  private final UUID incarnationId;
  static final short VERSION_0 = 0;
  static final int VERSION_SIZE = 2;
  static final int INCARNATION_ID_LENGTH_SIZE = 4;
  static final String STORE_DESCRIPTOR_FILENAME = "StoreDescriptor";

  /**
   * Instantiates the {@link StoreDescriptor} for the store. If the respective file is present, reads the bytes
   * to understand the incarnationId. If not, creates a new one with a random Unique identifier as the new incarnationId
   * @param dataDir the directory path to locate the Store Descriptor file
   * @param config the store config to use in this {@link StoreDescriptor}
   * @throws IOException when file creation or read or write to file fails
   */
  StoreDescriptor(String dataDir, StoreConfig config) throws IOException {
    File storeDescriptorFile = new File(dataDir, STORE_DESCRIPTOR_FILENAME);
    if (storeDescriptorFile.exists()) {
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(storeDescriptorFile));
      DataInputStream stream = new DataInputStream(crcStream);
      short version = stream.readShort();
      switch (version) {
        case VERSION_0:
          // read incarnationId
          String incarnationIdStr = Utils.readIntString(stream);
          incarnationId = UUID.fromString(incarnationIdStr);
          long crc = crcStream.getValue();
          long crcValueInFile = stream.readLong();
          if (crc != crcValueInFile) {
            throw new IllegalStateException(
                "CRC mismatch for StoreDescriptor. CRC of the stream " + crc + ", CRC from file " + crcValueInFile);
          }
          break;
        default:
          throw new IllegalArgumentException("Unrecognized version in StoreDescriptor: " + version);
      }
      if (config.storeSetFilePermissionEnabled) {
        Files.setPosixFilePermissions(storeDescriptorFile.toPath(), config.storeOperationFilePermission);
      }
    } else {
      incarnationId = UUID.randomUUID();
      File tempFile = new File(dataDir, STORE_DESCRIPTOR_FILENAME + ".tmp");
      File actual = new File(dataDir, STORE_DESCRIPTOR_FILENAME);
      if (tempFile.createNewFile()) {
        FileOutputStream fileStream = new FileOutputStream(tempFile);
        CrcOutputStream crc = new CrcOutputStream(fileStream);
        DataOutputStream writer = new DataOutputStream(crc);
        writer.write(toBytes());
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);
        fileStream.getChannel().force(true);
        if (!tempFile.renameTo(actual)) {
          throw new IllegalStateException(
              "File " + tempFile.getAbsolutePath() + " renaming to " + actual.getAbsolutePath() + " failed ");
        }
        if (config.storeSetFilePermissionEnabled) {
          Files.setPosixFilePermissions(actual.toPath(), config.storeOperationFilePermission);
        }
      } else {
        throw new IllegalStateException("File " + tempFile.getAbsolutePath() + " creation failed ");
      }
    }
  }

  /**
   * @return the incarnationId of the store
   */
  UUID getIncarnationId() {
    return incarnationId;
  }

  /**
   * Returns the serialized form of this {@link StoreDescriptor}
   * @return the serialized form of this {@link StoreDescriptor}
   */
  private byte[] toBytes() {
    byte[] incarnationIdBytes = incarnationId.toString().getBytes();
    int size = VERSION_SIZE + INCARNATION_ID_LENGTH_SIZE + incarnationIdBytes.length;
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
