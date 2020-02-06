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
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;


/**
 * The replication feed that provides next list of blobs to replicate from azure and corresponding {@link FindToken}
 * using cosmos update time field.
 */
public class CosmosUpdateTimeBasedReplicationFeed implements AzureReplicationFeed {

  private static final String LIMIT_PARAM = "@limit";
  private static final String TIME_SINCE_PARAM = "@timesince";
  // Note: ideally would like to order by uploadTime and id, but Cosmos doesn't allow without composite index.
  // It is unlikely (but not impossible) for two blobs in same partition to have the same uploadTime (would have to
  // be multiple VCR's uploading same partition).  We track the lastBlobId in the CloudFindToken and skip it if
  // is returned in successive queries.
  private static final String ENTRIES_SINCE_QUERY_TEMPLATE =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " >= "
          + TIME_SINCE_PARAM + " ORDER BY c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " ASC";
  private final CosmosDataAccessor cosmosDataAccessor;
  private final AzureMetrics azureMetrics;

  /**
   * Constructor for {@link CosmosUpdateTimeBasedReplicationFeed} object.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object to run cosmos change feed queries.
   * @param azureMetrics {@link AzureMetrics} object.
   */
  public CosmosUpdateTimeBasedReplicationFeed(CosmosDataAccessor cosmosDataAccessor, AzureMetrics azureMetrics) {
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.azureMetrics = azureMetrics;
  }

  @Override
  public CosmosUpdateTimeFindToken getNextEntriesAndUpdatedToken(FindToken curfindToken,
      List<CloudBlobMetadata> nextEntries, long maxTotalSizeOfEntries, String partitionPath)
      throws DocumentClientException {
    CosmosUpdateTimeFindToken findToken = (CosmosUpdateTimeFindToken) curfindToken;
    SqlQuerySpec entriesSinceQuery = new SqlQuerySpec(ENTRIES_SINCE_QUERY_TEMPLATE,
        new SqlParameterCollection(new SqlParameter(LIMIT_PARAM, AzureCloudDestination.getFindSinceQueryLimit()),
            new SqlParameter(TIME_SINCE_PARAM, findToken.getLastUpdateTime())));
    List<CloudBlobMetadata> queryResults =
        cosmosDataAccessor.queryMetadata(partitionPath, entriesSinceQuery, azureMetrics.findSinceQueryTime);
    if (queryResults.isEmpty()) {
      return findToken;
    }
    if (queryResults.get(0).getLastUpdateTime() == findToken.getLastUpdateTime()) {
      filterOutLastReadBlobs(queryResults, findToken.getLastUpdateTimeReadBlobIds(), findToken.getLastUpdateTime());
    }
    nextEntries.addAll(CloudBlobMetadata.capMetadataListBySize(queryResults, maxTotalSizeOfEntries));
    return CosmosUpdateTimeFindToken.getUpdatedToken(findToken, queryResults);
  }

  /**
   * Filter out {@link CloudBlobMetadata} objects from lastUpdateTime ordered {@code cloudBlobMetadataList} whose
   * lastUpdateTime is {@code lastUpdateTime} and id is in {@code lastReadBlobIds}.
   * @param cloudBlobMetadataList list of {@link CloudBlobMetadata} objects to filter out from.
   * @param lastReadBlobIds set if blobIds which need to be filtered out.
   * @param lastUpdateTime lastUpdateTime of the blobIds to filter out.
   */
  private void filterOutLastReadBlobs(List<CloudBlobMetadata> cloudBlobMetadataList, Set<String> lastReadBlobIds,
      long lastUpdateTime) {
    ListIterator<CloudBlobMetadata> iterator = cloudBlobMetadataList.listIterator();
    int numRemovedBlobs = 0;
    while (iterator.hasNext()) {
      CloudBlobMetadata cloudBlobMetadata = iterator.next();
      if (numRemovedBlobs == lastReadBlobIds.size() || cloudBlobMetadata.getLastUpdateTime() > lastUpdateTime) {
        break;
      }
      if (lastReadBlobIds.contains(cloudBlobMetadata.getId())) {
        iterator.remove();
        numRemovedBlobs++;
      }
    }
  }
}
