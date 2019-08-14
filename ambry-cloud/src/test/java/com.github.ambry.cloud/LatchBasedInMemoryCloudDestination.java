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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;


/**
 * An latch based in memory implementation of {@link CloudDestination}.
 */
public class LatchBasedInMemoryCloudDestination implements CloudDestination {

  private final Map<BlobId, Pair<CloudBlobMetadata, byte[]>> map = new HashMap<>();
  private final CountDownLatch latch;
  private final Set<BlobId> blobIdsToTrack = ConcurrentHashMap.newKeySet();
  private final Map<String, byte[]> tokenMap = new ConcurrentHashMap<>();
  private final AtomicLong bytesUploadedCounter = new AtomicLong(0);
  private final AtomicInteger blobsUploadedCounter = new AtomicInteger(0);

  /**
   * Instantiate {@link LatchBasedInMemoryCloudDestination}.
   * @param blobIdsToTrack a list of blobs that {@link LatchBasedInMemoryCloudDestination} tracks.
   */
  public LatchBasedInMemoryCloudDestination(List<BlobId> blobIdsToTrack) {
    this.blobIdsToTrack.addAll(blobIdsToTrack);
    latch = new CountDownLatch(blobIdsToTrack.size());
  }

  @Override
  synchronized public boolean uploadBlob(BlobId blobId, long blobSize, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) {
    // Note: for encrypted data, blobSize may be -1, so ideally we shouldn't use it here.
    // However, till the time encrypted get is implemented, getting the buffer with metadata size helps with testing of unencrypted get.
    byte[] buffer = new byte[(int) cloudBlobMetadata.getSize()];
    int bytesRead = 0;
    try {
      do {
        bytesRead = blobInputStream.read(buffer);
        bytesUploadedCounter.addAndGet(Math.max(bytesRead, 0));
      } while (bytesRead > 0);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    map.put(blobId, new Pair<>(cloudBlobMetadata, buffer));
    blobsUploadedCounter.incrementAndGet();
    if (blobIdsToTrack.remove(blobId)) {
      latch.countDown();
    }
    return true;
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
    CloudMessageReadSet.BlobReadInfo blobReadInfo = null;
    try {
      if (!map.containsKey(blobId)) {
        throw new CloudStorageException("Blob with blobId " + blobId.getID() + " does not exist.");
      }
      byte[] blobData = map.get(blobId).getSecond();
      outputStream.write(blobData);
    } catch (IOException ex) {
      throw new CloudStorageException(
          "Could not download blob for blobid " + blobId.getID() + " due to " + ex.toString());
    }
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime) throws CloudStorageException {
    if (!map.containsKey(blobId)) {
      return false;
    }
    map.get(blobId).getFirst().setDeletionTime(deletionTime);
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
  public List<CloudBlobMetadata> getDeadBlobs(String partitionPath) {
    return Collections.emptyList();
  }

  @Override
  public List<CloudBlobMetadata> findEntriesSince(String partitionPath, CloudFindToken findToken,
      long maxTotalSizeOfEntries) {
    return Collections.emptyList();
  }

  @Override
  public boolean purgeBlob(CloudBlobMetadata blobMetadata) {
    return true;
  }

  @Override
  public int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) {
    return 0;
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

  @Override
  public long getBlobSize(BlobId blobId) {
    return map.get(blobId).getSecond().length;
  }

  public Map<String, byte[]> getTokenMap() {
    return tokenMap;
  }

  public int getBlobsUploaded() {
    return blobsUploadedCounter.get();
  }

  public long getBytesUploaded() {
    return bytesUploadedCounter.get();
  }

  public boolean await(long duration, TimeUnit timeUnit) throws InterruptedException {
    return latch.await(duration, timeUnit);
  }
}

