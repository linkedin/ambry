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
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;


public class MockChangeFeedQuery extends CosmosDataAccessor {
  private final Map<String, String> continuationTokenToBlobIdMap = new HashMap<>();
  private final Map<String, String> blobIdToContinuationTokenMap = new HashMap<>();
  private final Map<String, CloudBlobMetadata> blobIdToMetadataMap = new HashMap<>();
  private int continuationTokenCounter = -1;

  MockChangeFeedQuery() {
    super(mock(AsyncDocumentClient.class), "", mock(AzureMetrics.class));
  }

  /**
   * Add a blobid to the change feed.
   * @param cloudBlobMetadata {@link CloudBlobMetadata} to add.
   */
  void add(CloudBlobMetadata cloudBlobMetadata) {
    String blobId = cloudBlobMetadata.getId();
    blobIdToMetadataMap.put(blobId, cloudBlobMetadata);
    if (blobIdToContinuationTokenMap.containsKey(blobId)) {
      continuationTokenToBlobIdMap.put(blobIdToContinuationTokenMap.get(blobId), null);
    }
    continuationTokenToBlobIdMap.put(Integer.toString(++continuationTokenCounter), blobId);
    blobIdToContinuationTokenMap.put(blobId, Integer.toString(continuationTokenCounter));
  }

  @Override
  public String queryChangeFeed(String requestContinuationToken, int maxFeedSize, List<CloudBlobMetadata> changeFeed,
      String partitionPath, Timer timer) {
    if (requestContinuationToken.equals("")) {
      requestContinuationToken = "0";
    }
    // there are no changes since last continuation token or there is no change feed at all, then return
    if ((requestContinuationToken != null && Integer.parseInt(requestContinuationToken) == continuationTokenCounter + 1)
        || continuationTokenCounter == -1) {
      return requestContinuationToken;
    }
    // check if its an invalid continuation token
    if (requestContinuationToken != null && !continuationTokenToBlobIdMap.containsKey(requestContinuationToken)) {
      throw new IllegalArgumentException("Invalid continuation token");
    }

    if (requestContinuationToken == null) {
      requestContinuationToken = "0";
    }

    // iterate through change feed till it ends or maxLimit or maxTotalSizeofEntries is reached
    String continuationTokenCtr = requestContinuationToken;
    while (continuationTokenToBlobIdMap.containsKey(continuationTokenCtr) && changeFeed.size() < maxFeedSize) {
      if (continuationTokenToBlobIdMap.get(continuationTokenCtr) != null) {
        changeFeed.add(blobIdToMetadataMap.get(continuationTokenToBlobIdMap.get(continuationTokenCtr)));
      }
      continuationTokenCtr = Integer.toString(Integer.parseInt(continuationTokenCtr) + 1);
    }
    return continuationTokenCtr;
  }
}

