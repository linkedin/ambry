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
import com.github.ambry.cloud.FindResult;
import com.github.ambry.replication.FindToken;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import java.io.Closeable;
import java.util.List;


/**
 * The replication feed that provides next list of blobs to replicate from Azure, and a bookmark in form of {@link FindToken}.
 */
public interface AzureReplicationFeed extends Closeable {

  enum FeedType {
    /** Replication feed is obtained from cosmos change feed api */
    COSMOS_CHANGE_FEED,

    /** Replication feed is obtained from cosmos queries ordered by update time */
    COSMOS_UPDATE_TIME
  }

  /**
   * Returns an ordered sequenced list of blobs within the specified partition and updated
   * {@link com.github.ambry.replication.FindToken}, such that total size of all blobs in the list are less or equal to
   * {@code maxTotalSizeOfEntries}. This method should return at least one blob, if exists, after {@code curfindToken},
   * irrespective of {@code maxTotalSizeOfEntries} requirement.
   * @param curfindToken current {@link FindToken} object that acts as a bookmark to return blobs after.
   * @param maxTotalSizeOfEntries max total size of all the {@link CloudBlobMetadata} objects returned.
   * @param partitionPath partition of the blobs.
   * @return {@link FindResult} instance that contains updated {@link FindToken} object which can act as a bookmark for
   * subsequent requests, and {@link List} of {@link CloudBlobMetadata} entries.
   * @throws {@link DocumentClientException}.
   */
  FindResult getNextEntriesAndUpdatedToken(FindToken curfindToken, long maxTotalSizeOfEntries, String partitionPath)
      throws DocumentClientException;
}
