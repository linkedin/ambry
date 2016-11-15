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
package com.github.ambry.commons;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import static org.junit.Assert.*;


public class BlobIdTest {
  @Test
  public void basicTest() throws Exception {
    final long id = 99;
    final long replicaCapacityInBytes = 1024 * 1024 * 1024;
    PartitionId partitionId = new Partition(id, PartitionState.READ_WRITE, replicaCapacityInBytes);
    BlobId blobId = new BlobId(partitionId);

    assertEquals(blobId.getPartition(), partitionId);
    System.out.println("Blob Id toString: " + blobId);
    System.out.println("Blob id sizeInBytes: " + blobId.toString().length());
  }

  /**
   * Test the parsing of a valid blob ID from a string and stream.
   * @throws Exception
   */
  @Test
  public void goodIdTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    short goodVersion = 1;
    PartitionId goodPartitionId = clusterMap.getWritablePartitionIds().get(0);
    String goodUUID = UUID.randomUUID().toString();
    String goodId = buildBlobIdLike(goodVersion, goodPartitionId, goodUUID.length(), goodUUID);
    List<BlobId> blobIds = new ArrayList<>();
    blobIds.add(new BlobId(goodId, clusterMap));
    blobIds.add(new BlobId(getStreamFromBase64(goodId), clusterMap));
    blobIds.add(new BlobId(getStreamFromBase64(goodId + "EXTRA"), clusterMap));
    for (BlobId blobId : blobIds) {
      assertEquals("Wrong partition ID in blob ID: " + blobId, goodPartitionId, blobId.getPartition());
      assertEquals("Wrong base-64 ID in blob ID: " + blobId, goodId, blobId.getID());
    }
  }

  /**
   * Test various invalid blob IDs
   * @throws Exception
   */
  @Test
  public void badIdTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    List<String> blobIdLikes = new ArrayList<>();

    short goodVersion = 1;
    PartitionId goodPartitionId = clusterMap.getWritablePartitionIds().get(0);
    PartitionId badPartitionId = new MockPartitionId(200000, Collections.EMPTY_LIST, 0);
    String goodUUID = UUID.randomUUID().toString();

    // Partition ID not in cluster map
    blobIdLikes.add(buildBlobIdLike(goodVersion, badPartitionId, goodUUID.length(), goodUUID));
    // UUID length too long
    blobIdLikes.add(buildBlobIdLike(goodVersion, goodPartitionId, goodUUID.length() + 1, goodUUID));
    // UUID length too short
    blobIdLikes.add(buildBlobIdLike(goodVersion, goodPartitionId, goodUUID.length() - 1, goodUUID));
    // UUID length is negative
    blobIdLikes.add(buildBlobIdLike(goodVersion, goodPartitionId, -1, goodUUID));
    // Extra characters after UUID
    blobIdLikes.add(buildBlobIdLike(goodVersion, goodPartitionId, goodUUID.length(), goodUUID + "EXTRA"));
    // Invalid version number
    blobIdLikes.add(buildBlobIdLike((short) (goodVersion + 1), goodPartitionId, goodUUID.length(), goodUUID));
    // Empty blob ID
    blobIdLikes.add("");
    // short Blob ID
    blobIdLikes.add("AA");

    for (String blobIdLike : blobIdLikes) {
      try {
        new BlobId(blobIdLike, clusterMap);
        fail("Expected blob ID creation to fail with blob ID string " + blobIdLike);
      } catch (Exception e) {
      }
    }
  }

  /**
   * Build a string that resembles a blob ID, but with possibly invalid sections.
   * @param version the version number to use in the blob ID
   * @param partitionId the partition ID to use in the blob ID
   * @param uuidLength the UUID length to use in the blob ID
   * @param uuidLike the UUID to use in the blob ID
   * @return a base-64 encoded {@link String} representing the blob ID
   */
  private String buildBlobIdLike(short version, PartitionId partitionId, int uuidLength, String uuidLike) {
    final int idLength = 2 + partitionId.getBytes().length + 4 + uuidLike.length();
    ByteBuffer idBuf = ByteBuffer.allocate(idLength);
    idBuf.putShort(version);
    idBuf.put(partitionId.getBytes());
    idBuf.putInt(uuidLength);
    idBuf.put(uuidLike.getBytes());
    return Base64.encodeBase64URLSafeString(idBuf.array());
  }

  /**
   * Convert a base-64 encoded string into a {@link DataInputStream}
   * @param base64String the base-64 encoded {@link String}
   * @return the {@link DataInputStream}
   */
  private DataInputStream getStreamFromBase64(String base64String) {
    return new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(base64String))));
  }
}
