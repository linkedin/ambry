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

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class to handle and cache cosmos change feed.
 */
public class CosmosChangeFeedCache {

  /**
   * Class representing change feed cache for each partition.
   */
  class ChangeFeedCacheEntry {
    private final String startContinuationToken;
    private final String endContinuationToken;
    private final String azureRequestId;
    private final List<CloudBlobMetadata> fetchedEntries;

    /**
     * Constructor for {@link ChangeFeedCacheEntry}.
     * @param startContinuationToken start continuation token from where the cached entries are stored.
     * @param endContinuationToken end continuation token after all the cached items are consumed.
     * @param azureRequestId a random UUID which uniquely identifies each cached info.
     * @param fetchedEntries {@link List} of cached {@link CloudBlobMetadata} objects.
     */
    ChangeFeedCacheEntry(String startContinuationToken, String endContinuationToken, String azureRequestId,
        List<CloudBlobMetadata> fetchedEntries) {
      this.startContinuationToken = startContinuationToken;
      this.endContinuationToken = endContinuationToken;
      this.azureRequestId = azureRequestId;
      this.fetchedEntries = fetchedEntries;
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
     * Return the azure request id.
     * @return azure request id.
     */
    String getAzureRequestId() {
      return azureRequestId;
    }

    /**
     * Return the fetch entries list.
     * @return {@link List} of {@link CloudBlobMetadata} entries.
     */
    List<CloudBlobMetadata> getFetchedEntries() {
      return fetchedEntries;
    }
  }

  private final ConcurrentHashMap<String, ChangeFeedCacheEntry> changeFeedCache;
  private final int defaultCacheSize;
  private final CosmosDataAccessor cosmosDataAccessor;

  /**
   * Constructor to create a {@link CosmosChangeFeedCache} object.
   * @param cacheSize default number of cachedEntries for each partition.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object.
   */
  public CosmosChangeFeedCache(int cacheSize, CosmosDataAccessor cosmosDataAccessor) {
    this.defaultCacheSize = cacheSize;
    changeFeedCache = new ConcurrentHashMap<>();
    this.cosmosDataAccessor = cosmosDataAccessor;
  }

  /**
   * Get next set of change feed entries for the specified partition, after the {@code cosmosChangeFeedFindToken}.
   * The number of entries is capped by maxEntriesSize.
   * This method creates a cache for change feed entries. If the {@code cosmosChangeFeedFindToken} is not valid,
   * or if all the items in the cache are consumed, then it queries cosmos for new entries.
   * @param cosmosChangeFeedFindToken {@link CosmosChangeFeedFindToken} after which the next entries have to be returned.
   * @param results {@link List} of {@link CloudBlobMetadata} objects which will be populated by new entries.
   * @param maxEntriesSize maximum size of all the blobs returned in {@code results}
   * @param partitionId Partition for which change feed entries have to be returned.
   * @return updated {@link CosmosChangeFeedFindToken} after processing the next set of entries.
   */
  public CosmosChangeFeedFindToken getNextEntriesAndToken(CosmosChangeFeedFindToken cosmosChangeFeedFindToken,
      List<CloudBlobMetadata> results, long maxEntriesSize, String partitionId) throws DocumentClientException {
    int index = cosmosChangeFeedFindToken.getIndex();
    if (!changeFeedCache.containsKey(partitionId) || !isCacheValid(partitionId, cosmosChangeFeedFindToken)) {
      populateChangeFeedCache(partitionId, cosmosChangeFeedFindToken.getStartContinuationToken());
      index = 0;
    }

    long resultSize = 0;
    while (true) {
      if (index < changeFeedCache.get(partitionId).getFetchedEntries().size()) {
        if (resultSize + changeFeedCache.get(partitionId).getFetchedEntries().get(index).getSize() < maxEntriesSize) {
          results.add(changeFeedCache.get(partitionId).getFetchedEntries().get(index));
          resultSize = resultSize + changeFeedCache.get(partitionId).getFetchedEntries().get(index).getSize();
          index++;
        } else {
          if (resultSize == 0) {
            results.add(changeFeedCache.get(partitionId).getFetchedEntries().get(index));
            resultSize = changeFeedCache.get(partitionId).getFetchedEntries().get(index).getSize();
            index++;
          }
          break;
        }
      } else {
        populateChangeFeedCache(partitionId, cosmosChangeFeedFindToken.getEndContinuationToken());
        if (cacheEmpty(partitionId)) {
          // this means that there are no new changes
          break;
        }
        index = 0;
      }
    }

    return new CosmosChangeFeedFindToken(resultSize, changeFeedCache.get(partitionId).getStartContinuationToken(),
        changeFeedCache.get(partitionId).getEndContinuationToken(), index,
        changeFeedCache.get(partitionId).getFetchedEntries().size(),
        changeFeedCache.get(partitionId).getAzureRequestId(), cosmosChangeFeedFindToken.getVersion());
  }

  /**
   * Check is the cache is valid for the {@code cosmosChangeFeedFindToken} provided.
   * @param partitionId partition of the {@code cosmosChangeFeedFindToken}.
   * @param cosmosChangeFeedFindToken {@link CosmosChangeFeedFindToken} object.
   * @return true is cache is valid. false otherwise.
   */
  private boolean isCacheValid(String partitionId, CosmosChangeFeedFindToken cosmosChangeFeedFindToken) {
    ChangeFeedCacheEntry changeFeedCacheEntry = changeFeedCache.get(partitionId);
    return Utils.checkNullableStringEquals(cosmosChangeFeedFindToken.getAzureTokenRequestId(),
        changeFeedCacheEntry.getAzureRequestId()) && Utils.checkNullableStringEquals(
        cosmosChangeFeedFindToken.getStartContinuationToken(), changeFeedCacheEntry.getStartContinuationToken())
        && Utils.checkNullableStringEquals(cosmosChangeFeedFindToken.getEndContinuationToken(),
        changeFeedCacheEntry.getEndContinuationToken())
        && cosmosChangeFeedFindToken.getTotalItems() == changeFeedCacheEntry.getFetchedEntries().size();
  }

  private boolean cacheEmpty(String partitionId) {
    return changeFeedCache.get(partitionId).getFetchedEntries().size() == 0;
  }

  /**
   * Populate change feed cache by querying cosmos for the next set of change feed entries after the specified request continuation token.
   * @param partitionId Partition for which the change feed cache needs to be populated.
   * @param startRequestContinuationToken request continuation token from which the change feed query needs to be made.
   */
  private void populateChangeFeedCache(String partitionId, String startRequestContinuationToken)
      throws DocumentClientException {
    List<CloudBlobMetadata> changeFeedEntries = new ArrayList<>(defaultCacheSize);
    String newRequestContinuationToken =
        cosmosDataAccessor.queryChangeFeed(startRequestContinuationToken, defaultCacheSize, changeFeedEntries,
            partitionId);
    ChangeFeedCacheEntry changeFeedCacheEntry =
        new ChangeFeedCacheEntry(startRequestContinuationToken, newRequestContinuationToken,
            UUID.randomUUID().toString(), changeFeedEntries);
    changeFeedCache.put(partitionId, changeFeedCacheEntry);
  }
}
