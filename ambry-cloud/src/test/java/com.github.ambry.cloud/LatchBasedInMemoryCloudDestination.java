/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;


/**
 * An latch based in memory implementation of {@link CloudDestination}.
 */
public class LatchBasedInMemoryCloudDestination implements CloudDestination {

  private final Map<BlobId, Pair<CloudBlobMetadata, InputStream>> map = new HashMap<>();
  private final CountDownLatch latch;
  private final Set<BlobId> blobIds;
  private final Map<String, byte[]> tokenMap = new HashMap<>();

  /**
   * Instantiate {@link LatchBasedInMemoryCloudDestination}.
   * @param blobIds a list of blobs that {@link LatchBasedInMemoryCloudDestination} tracks.
   */
  public LatchBasedInMemoryCloudDestination(List<BlobId> blobIds) {
    this.blobIds = new HashSet<>(blobIds);
    this.latch = new CountDownLatch(blobIds.size());
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long blobSize, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) {
    if (blobIds.contains(blobId)) {
      map.put(blobId, new Pair<>(cloudBlobMetadata, blobInputStream));
      latch.countDown();
    }
    return true;
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime) throws CloudStorageException {
    return true;
  }

  @Override
  public boolean updateBlobExpiration(BlobId blobId, long expirationTime) throws CloudStorageException {
    if (map.containsKey(blobId)) {
      map.get(blobId).getFirst().setExpirationTime(expirationTime);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    Map<String, CloudBlobMetadata> result = new HashMap<>();
    for (BlobId blobId : blobIds) {
      if (map.containsKey(blobId)) {
        result.put(blobId.toString(), map.get(blobId).getFirst());
      }
    }
    return result;
  }

  @Override
  public boolean doesBlobExist(BlobId blobId) throws CloudStorageException {
    return map.containsKey(blobId);
  }

  @Override
  public void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream)
      throws CloudStorageException {
    try {
      tokenMap.put(partitionPath + tokenFileName, IOUtils.toByteArray(inputStream));
    } catch (IOException e) {
      throw new CloudStorageException("Read input stream error", e);
    }
  }

  @Override
  public boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream)
      throws CloudStorageException {
    if (tokenMap.get(partitionPath + tokenFileName) == null) {
      return false;
    }
    try {
      outputStream.write(tokenMap.get(partitionPath + tokenFileName));
      return true;
    } catch (IOException e) {
      throw new CloudStorageException("Write to stream error", e);
    }
  }

  public boolean await(long duration, TimeUnit timeUnit) throws InterruptedException {
    return latch.await(duration, timeUnit);
  }
}

