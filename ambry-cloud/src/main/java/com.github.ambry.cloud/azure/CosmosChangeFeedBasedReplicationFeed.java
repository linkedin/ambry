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
import com.github.ambry.replication.FindToken;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import java.util.List;


public class CosmosChangeFeedBasedReplicationFeed implements AzureReplicationFeed {
  private final CosmosChangeFeedCache cosmosChangeFeedCache;

  /**
   * Constructor for {@link CosmosChangeFeedBasedReplicationFeed} object.
   * @param cosmosDataAccessor
   */
  public CosmosChangeFeedBasedReplicationFeed(CosmosDataAccessor cosmosDataAccessor, AzureMetrics azureMetrics) {
    cosmosChangeFeedCache =
        new CosmosChangeFeedCache(AzureCloudDestination.getFindSinceQueryLimit(), cosmosDataAccessor, azureMetrics);
  }

  @Override
  public CosmosChangeFeedFindToken getNextEntriesAndUpdatedToken(FindToken curfindToken,
      List<CloudBlobMetadata> nextEntries, long maxTotalSizeOfEntries, String partitionPath)
      throws DocumentClientException {
    return cosmosChangeFeedCache.getNextEntriesAndToken((CosmosChangeFeedFindToken) curfindToken, nextEntries,
        maxTotalSizeOfEntries, partitionPath);
  }
}
