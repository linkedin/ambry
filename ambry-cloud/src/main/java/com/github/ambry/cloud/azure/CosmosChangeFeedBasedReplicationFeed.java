/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * The replication feed that provides next list of blobs to replicate from Azure and corresponding {@link FindToken}
 * using Cosmos change feed apis.
 */
public final class CosmosChangeFeedBasedReplicationFeed implements AzureReplicationFeed {

  /**
   * Class representing change feed cache for each partition.
   */
  static class ChangeFeedCacheEntry {
    private final String startContinuationToken;
    private final String endContinuationToken;
    private final String cacheSessionId;
    private final List<CloudBlobMetadata> fetchedEntries;
    private final String partitionId;
    private final long creationTimestamp;

    /**
     * Constructor for {@link ChangeFeedCacheEntry}.
     * @param startContinuationToken start continuation token from where the cached entries are stored.
     * @param endContinuationToken end continuation token after all the cached items are consumed.
     * @param cacheSessionId a random UUID which uniquely identifies each cached info.
     * @param fetchedEntries {@link List} of cached {@link CloudBlobMetadata} objects.
     */
    ChangeFeedCacheEntry(String startContinuationToken, String endContinuationToken, String cacheSessionId,
        List<CloudBlobMetadata> fetchedEntries, String partitionId) {
      this.startContinuationToken = startContinuationToken;
      this.endContinuationToken = endContinuationToken;
      this.cacheSessionId = cacheSessionId;
      this.fetchedEntries = fetchedEntries;
      this.partitionId = partitionId;
      this.creationTimestamp = System.currentTimeMillis();
    }

    /**
     * Shallow copy Constructor for {@link ChangeFeedCacheEntry}, which copies all fields except creationTimestamp.
     * @param old old {@link ChangeFeedCacheEntry} object.
     */
    ChangeFeedCacheEntry(ChangeFeedCacheEntry old) {
      this(old.getStartContinuationToken(), old.getEndContinuationToken(), old.getCacheSessionId(),
          old.getFetchedEntries(), old.getPartitionId());
    }

    /**
     * Return start continuation token.
     * @return start continuation token.
     */
    String getStartContinuationToken() {
      return startContinuationToken;
    }

    /**
     * Return the end continuation token.
     * @return end continuation token.
     */
    String getEndContinuationToken() {
      return endContinuationToken;
    }

    /**
     * Return the Azure request id.
     * @return Azure request id.
     */
    String getCacheSessionId() {
      return cacheSessionId;
    }

    /**
     * Return the fetch entries list.
     * @return {@link List} of {@link CloudBlobMetadata} entries.
     */
    List<CloudBlobMetadata> getFetchedEntries() {
      return fetchedEntries;
    }

    /**
     * Return the {@code partitionId}
     * @return {@code partitionId}
     */
    public String getPartitionId() {
      return partitionId;
    }

    /**
     * Check if is this entry is expired. The condition for expiry uses {@code creationTimestamp}. This is good enough as
     * this means that the cached set of fetches entries hasn't been consumed within the invalidation duration.
     * @return true if this entry is expired. false otherwise.
     */
    boolean isExpired() {
      return creationTimestamp < System.currentTimeMillis() - CACHE_VALID_DURATION_IN_MS;
    }
  }

  // change feed cache by cache session id
  private final ConcurrentHashMap<String, ChangeFeedCacheEntry> changeFeedCache;
  private final int defaultCacheSize;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final AzureMetrics azureMetrics;
  private final ScheduledExecutorService scheduler;
  private final static long CACHE_VALID_DURATION_IN_MS = TimeUnit.HOURS.toMillis(1); //1 hour

  /**
   * Constructor to create a {@link CosmosChangeFeedBasedReplicationFeed} object.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object.
   * @param azureMetrics{@link {@link AzureMetrics} object.
   * @param changeFeedBatchSize batch size for each change feed request.
   */
  public CosmosChangeFeedBasedReplicationFeed(CosmosDataAccessor cosmosDataAccessor, AzureMetrics azureMetrics,
      int changeFeedBatchSize) {
    this.defaultCacheSize = changeFeedBatchSize;
    changeFeedCache = new ConcurrentHashMap<>();
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.azureMetrics = azureMetrics;
    // schedule periodic invalidation of cache
    scheduler = Utils.newScheduler(1, false);
    scheduler.scheduleAtFixedRate(() -> changeFeedCache.entrySet().removeIf(entry -> entry.getValue().isExpired()),
        CACHE_VALID_DURATION_IN_MS, CACHE_VALID_DURATION_IN_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Get next set of change feed entries for the specified partition, after the {@code curFindToken}.
   * The number of entries is capped by maxEntriesSize.
   * This method creates a cache for change feed entries. If the {@code curFindToken} is not valid,
   * or if all the items in the cache are consumed, then it queries Cosmos for new entries.
   * @param curFindToken {@link FindToken} after which the next entries have to be returned.
   * @param maxTotalSizeOfEntries maximum size of all the blobs returned.
   * @param partitionPath Partition for which change feed entries have to be returned.
   * @return {@link FindResult} instance that contains updated {@link FindToken} object which can act as a bookmark for
   * subsequent requests, and {@link List} of {@link CloudBlobMetadata} entries.
   * @throws DocumentClientException if any cosmos query encounters error.
   */
  @Override
  public FindResult getNextEntriesAndUpdatedToken(FindToken curFindToken, long maxTotalSizeOfEntries,
      String partitionPath) throws DocumentClientException {
    Timer.Context operationTimer = azureMetrics.replicationFeedQueryTime.time();
    try {
      List<CloudBlobMetadata> nextEntries = new ArrayList<>();
      CosmosChangeFeedFindToken cosmosChangeFeedFindToken = (CosmosChangeFeedFindToken) curFindToken;
      int index = cosmosChangeFeedFindToken.getIndex();
      ChangeFeedCacheEntry changeFeedCacheEntry = changeFeedCache.get(cosmosChangeFeedFindToken.getCacheSessionId());
      boolean cacheHit = true;
      if (changeFeedCacheEntry == null || !isCacheValid(partitionPath, cosmosChangeFeedFindToken,
          changeFeedCacheEntry)) {
        // the cache may not be valid. So we cannot use session id
        azureMetrics.changeFeedCacheMissRate.mark();
        cacheHit = false;
        changeFeedCacheEntry = getNextChangeFeed(partitionPath, cosmosChangeFeedFindToken.getStartContinuationToken());
        // invalidate the previous token's cache
        changeFeedCache.remove(cosmosChangeFeedFindToken.getCacheSessionId());
        index = 0;
      }

      long resultSize = 0;

      List<CloudBlobMetadata> fetchedEntries = changeFeedCacheEntry.getFetchedEntries();
      while (true) {
        if (index < fetchedEntries.size()) {
          if (cacheHit) {
            azureMetrics.changeFeedCacheHitRate.mark();
            cacheHit = false;
          }
          if (resultSize + fetchedEntries.get(index).getSize() < maxTotalSizeOfEntries || resultSize == 0) {
            nextEntries.add(fetchedEntries.get(index));
            resultSize = resultSize + fetchedEntries.get(index).getSize();
            index++;
          } else {
            break;
          }
        } else {
          // we can reuse the session id in this case, because we know that the cache ran out of new items.
          changeFeedCacheEntry = getNextChangeFeed(partitionPath, changeFeedCacheEntry.getEndContinuationToken(),
              changeFeedCacheEntry.getCacheSessionId());
          fetchedEntries = changeFeedCacheEntry.getFetchedEntries();
          if (fetchedEntries.isEmpty()) {
            // This means that either there are no new changes, or change feed made progress in continuation token
            // but no change feed results were returned (probably because this cosmos shard's change feed has other
            // other partition feed too, and they were filtered out). In either case its appropriate to break here and
            // return updated token. The source replication logic will retry replication with updated token.
            break;
          } else {
            azureMetrics.changeFeedCacheRefreshRate.mark();
          }
          index = 0;
        }
      }

      FindToken updatedToken = new CosmosChangeFeedFindToken(cosmosChangeFeedFindToken.getBytesRead() + resultSize,
          changeFeedCacheEntry.getStartContinuationToken(), changeFeedCacheEntry.getEndContinuationToken(), index,
          changeFeedCacheEntry.getFetchedEntries().size(), changeFeedCacheEntry.getCacheSessionId(),
          cosmosChangeFeedFindToken.getVersion());
      changeFeedCache.put(changeFeedCacheEntry.getCacheSessionId(), new ChangeFeedCacheEntry(changeFeedCacheEntry));
      return new FindResult(nextEntries, updatedToken);
    } finally {
      operationTimer.stop();
    }
  }

  @Override
  public void close() {
    Utils.shutDownExecutorService(scheduler, 5, TimeUnit.MINUTES);
  }

  /**
   * Check is the cache is valid for the {@code cosmosChangeFeedFindToken} provided.
   * @param partitionId partition of the {@code cosmosChangeFeedFindToken}.
   * @param cosmosChangeFeedFindToken {@link CosmosChangeFeedFindToken} object.
   * @return true is cache is valid. false otherwise.
   */
  private boolean isCacheValid(String partitionId, CosmosChangeFeedFindToken cosmosChangeFeedFindToken,
      ChangeFeedCacheEntry changeFeedCacheEntry) {
    return Objects.equals(cosmosChangeFeedFindToken.getCacheSessionId(), changeFeedCacheEntry.getCacheSessionId())
        && Objects.equals(cosmosChangeFeedFindToken.getStartContinuationToken(),
        changeFeedCacheEntry.getStartContinuationToken()) && Objects.equals(
        cosmosChangeFeedFindToken.getEndContinuationToken(), changeFeedCacheEntry.getEndContinuationToken())
        && cosmosChangeFeedFindToken.getTotalItems() == changeFeedCacheEntry.getFetchedEntries().size()
        && Objects.equals(partitionId, changeFeedCacheEntry.getPartitionId());
  }

  /**
   * Populate change feed cache by querying Cosmos for the next set of change feed entries after the specified request
   * continuation token. Also generate a new session id for the cache.
   * @param partitionId Partition for which the change feed cache needs to be populated.
   * @param startRequestContinuationToken request continuation token from which the change feed query needs to be made.
   * @return {@link ChangeFeedCacheEntry} object representing new cache entry.
   */
  private ChangeFeedCacheEntry getNextChangeFeed(String partitionId, String startRequestContinuationToken)
      throws DocumentClientException {
    return getNextChangeFeed(partitionId, startRequestContinuationToken, UUID.randomUUID().toString());
  }

  /**
   * Populate change feed cache by querying Cosmos for the next set of change feed entries after the specified request continuation token.
   * @param partitionId Partition for which the change feed cache needs to be populated.
   * @param startRequestContinuationToken request continuation token from which the change feed query needs to be made.
   * @param cacheSessionId cacheSessionId to use in the cache.
   * @return {@link ChangeFeedCacheEntry} object representing new cache entry.
   */
  private ChangeFeedCacheEntry getNextChangeFeed(String partitionId, String startRequestContinuationToken,
      String cacheSessionId) throws DocumentClientException {
    List<CloudBlobMetadata> changeFeedEntries = new ArrayList<>(defaultCacheSize);
    String newRequestContinuationToken =
        cosmosDataAccessor.queryChangeFeed(startRequestContinuationToken, defaultCacheSize, changeFeedEntries,
            partitionId, azureMetrics.changeFeedQueryTime);
    return new ChangeFeedCacheEntry(startRequestContinuationToken, newRequestContinuationToken, cacheSessionId,
        changeFeedEntries, partitionId);
  }
}
