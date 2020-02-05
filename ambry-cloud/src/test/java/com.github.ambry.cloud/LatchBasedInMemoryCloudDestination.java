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

import com.github.ambry.cloud.azure.CosmosChangeFeedFindToken;
import com.github.ambry.commons.BlobId;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Pair;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An latch based in memory implementation of {@link CloudDestination}.
 */
public class LatchBasedInMemoryCloudDestination implements CloudDestination {

  /**
   * Class representing change feed for {@link LatchBasedInMemoryCloudDestination}
   */
  class ChangeFeed {
    private final Map<String, BlobId> continuationTokenToBlobIdMap = new HashMap<>();
    private final Map<BlobId, String> blobIdToContinuationTokenMap = new HashMap<>();
    private int continuationTokenCounter = -1;
    private final String reqUuid = UUID.randomUUID().toString();

    /**
     * Add a blobid to the change feed.
     * @param blobId {@link BlobId} to add.
     */
    void add(BlobId blobId) {
      if (blobIdToContinuationTokenMap.containsKey(blobId)) {
        continuationTokenToBlobIdMap.put(blobIdToContinuationTokenMap.get(blobId), null);
      }
      continuationTokenToBlobIdMap.put(Integer.toString(++continuationTokenCounter), blobId);
      blobIdToContinuationTokenMap.put(blobId, Integer.toString(continuationTokenCounter));
    }

    String getContinuationTokenForBlob(BlobId blobId) {
      return blobIdToContinuationTokenMap.get(blobId);
    }

    Map<String, BlobId> getContinuationTokenToBlobIdMap() {
      return continuationTokenToBlobIdMap;
    }

    int getContinuationTokenCounter() {
      return continuationTokenCounter;
    }

    String getReqUuid() {
      return reqUuid;
    }
  }

  private final Map<BlobId, Pair<CloudBlobMetadata, byte[]>> map = new HashMap<>();
  private final CountDownLatch uploadLatch;
  private final CountDownLatch downloadLatch;
  private final Set<BlobId> blobIdsToTrack = ConcurrentHashMap.newKeySet();
  private final Map<String, byte[]> tokenMap = new ConcurrentHashMap<>();
  private final AtomicLong bytesUploadedCounter = new AtomicLong(0);
  private final AtomicInteger blobsUploadedCounter = new AtomicInteger(0);
  private final ChangeFeed changeFeed = new ChangeFeed();
  private final static Logger logger = LoggerFactory.getLogger(LatchBasedInMemoryCloudDestination.class);

  /**
   * Instantiate {@link LatchBasedInMemoryCloudDestination}.
   * @param blobIdsToTrack a list of blobs that {@link LatchBasedInMemoryCloudDestination} tracks.
   */
  public LatchBasedInMemoryCloudDestination(List<BlobId> blobIdsToTrack) {
    logger.debug("Constructing LatchBasedInMemoryCloudDestination with {} tracked blobs", blobIdsToTrack.size());
    this.blobIdsToTrack.addAll(blobIdsToTrack);
    uploadLatch = new CountDownLatch(blobIdsToTrack.size());
    downloadLatch = new CountDownLatch(blobIdsToTrack.size());
  }

  @Override
  synchronized public boolean uploadBlob(BlobId blobId, long blobSize, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) {

    // Note: blobSize can be -1 when we dont know the actual blob size being uploaded.
    // So we have to do buffered reads to handle that case.
    int bufferSz = (blobSize == -1) ? 1024 : (int) blobSize;
    byte[] buffer = new byte[bufferSz];
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try {
      int bytesRead = blobInputStream.read(buffer);
      while (bytesRead > 0) {
        outputStream.write(buffer, 0, bytesRead);
        bytesUploadedCounter.addAndGet(Math.max(bytesRead, 0));
        bytesRead = blobInputStream.read(buffer);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    cloudBlobMetadata.setLastUpdateTime(System.currentTimeMillis());
    map.put(blobId, new Pair<>(cloudBlobMetadata, outputStream.toByteArray()));
    changeFeed.add(blobId);
    blobsUploadedCounter.incrementAndGet();
    if (blobIdsToTrack.remove(blobId)) {
      uploadLatch.countDown();
    }
    return true;
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
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
    downloadLatch.countDown();
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime) {
    if (!map.containsKey(blobId)) {
      return false;
    }
    map.get(blobId).getFirst().setDeletionTime(deletionTime);
    map.get(blobId).getFirst().setLastUpdateTime(System.currentTimeMillis());
    changeFeed.add(blobId);
    return true;
  }

  @Override
  public boolean updateBlobExpiration(BlobId blobId, long expirationTime) {
    if (map.containsKey(blobId)) {
      map.get(blobId).getFirst().setExpirationTime(expirationTime);
      map.get(blobId).getFirst().setLastUpdateTime(System.currentTimeMillis());
      changeFeed.add(blobId);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) {
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
  public FindToken findEntriesSince(String partitionPath, FindToken findToken, long maxTotalSizeOfEntries,
      List<CloudBlobMetadata> nextEntries) {
    String continuationToken = ((CosmosChangeFeedFindToken)findToken).getEndContinuationToken();
    List<BlobId> blobIds = new ArrayList<>();
    getFeed(continuationToken, maxTotalSizeOfEntries, blobIds);
    nextEntries.addAll(blobIds.stream().map(blobId -> map.get(blobId).getFirst()).collect(Collectors.toList()));
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken = (CosmosChangeFeedFindToken)findToken;
    if (blobIds.size() != 0) {
      long bytesToBeRead = nextEntries.stream().mapToLong(CloudBlobMetadata::getSize).sum();
      cosmosChangeFeedFindToken = new CosmosChangeFeedFindToken(bytesToBeRead, changeFeed.getContinuationTokenForBlob(blobIds.get(0)),
          createEndContinuationToken(blobIds), 0, blobIds.size(), changeFeed.getReqUuid(), cosmosChangeFeedFindToken.getVersion());
    }
    return cosmosChangeFeedFindToken;
  }

  private String createEndContinuationToken(List<BlobId> blobIds) {
    return Integer.toString(
        Integer.parseInt(changeFeed.getContinuationTokenForBlob(blobIds.get(blobIds.size() - 1))) + 1);
  }

  /**
   * Get the change feed starting from given continuation token upto {@code maxLimit} number of items.
   * @param continuationToken starting token for the change feed.
   * @param maxTotalSizeOfEntries max size of all the blobs returned in changefeed.
   * @param feed {@link List} of {@link BlobId}s to be populated with the change feed.
   */
  private void getFeed(String continuationToken, long maxTotalSizeOfEntries, List<BlobId> feed) {
    int continuationTokenCounter = changeFeed.getContinuationTokenCounter();
    if (continuationToken.equals("")) {
      continuationToken = "0";
    }
    // there are no changes since last continuation token or there is no change feed at all, then return
    if (Integer.parseInt(continuationToken) == continuationTokenCounter + 1 || continuationTokenCounter == -1) {
      return;
    }
    // check if its an invalid continuation token
    if (!changeFeed.getContinuationTokenToBlobIdMap().containsKey(continuationToken)) {
      throw new IllegalArgumentException("Invalid continuation token");
    }
    // iterate through change feed till it ends or maxLimit or maxTotalSizeofEntries is reached
    String continuationTokenCtr = continuationToken;
    long totalFeedSize = 0;
    while (changeFeed.getContinuationTokenToBlobIdMap().containsKey(continuationTokenCtr)
        && totalFeedSize < maxTotalSizeOfEntries) {
      if (changeFeed.getContinuationTokenToBlobIdMap().get(continuationTokenCtr) != null) {
        feed.add(changeFeed.getContinuationTokenToBlobIdMap().get(continuationTokenCtr));
        totalFeedSize +=
            map.get(changeFeed.getContinuationTokenToBlobIdMap().get(continuationTokenCtr)).getFirst().getSize();
      }
      continuationTokenCtr = Integer.toString(Integer.parseInt(continuationTokenCtr) + 1);
    }
  }

  @Override
  public int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) {
    return 0;
  }

  boolean doesBlobExist(BlobId blobId) {
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

  public Map<String, byte[]> getTokenMap() {
    return tokenMap;
  }

  int getBlobsUploaded() {
    return blobsUploadedCounter.get();
  }

  long getBytesUploaded() {
    return bytesUploadedCounter.get();
  }

  public boolean awaitUpload(long duration, TimeUnit timeUnit) throws InterruptedException {
    return uploadLatch.await(duration, timeUnit);
  }

  public boolean awaitDownload(long duration, TimeUnit timeUnit) throws InterruptedException {
    return downloadLatch.await(duration, timeUnit);
  }
}
