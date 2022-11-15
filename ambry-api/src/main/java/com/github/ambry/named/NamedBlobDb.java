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
 *
 */
package com.github.ambry.named;

import com.github.ambry.frontend.Page;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import java.util.concurrent.CompletableFuture;


/**
 * A layer for interacting with a metadata store that holds mappings between blob names and blob IDs.
 */
public interface NamedBlobDb {

  /**
   * Look up a {@link NamedBlobRecord} by name.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param blobName the name of the blob.
   * @param option The {@link GetOption} for this get method.
   * @return a {@link CompletableFuture} that will eventually contain either the {@link NamedBlobRecord} for the named
   *         blob or an exception if an error occurred.
   */
  CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName, GetOption option);

  /**
   * Look up a {@link NamedBlobRecord} by name.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param blobName the name of the blob.
   * @return a {@link CompletableFuture} that will eventually contain either the {@link NamedBlobRecord} for the named
   *         blob or an exception if an error occurred.
   */
  default CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName) {
    return get(accountName, containerName, blobName, GetOption.None);
  }

  /**
   * List blobs that start with a provided prefix in a container. This returns paginated results. If there are
   * additional pages to read, {@link Page#getNextPageToken()} will be non null.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param blobNamePrefix the name prefix to search for.
   * @param pageToken if {@code null}, return the first page of {@link NamedBlobRecord}s that start with
   *                  {@code blobNamePrefix}. If set, use this as a token to resume reading additional pages
   *                  of records that start with the prefix.
   * @return a {@link CompletableFuture} that will eventually contain a {@link Page} of {@link NamedBlobRecord}s
   *         starting with the specified prefix or an exception if an error occurred.
   */
  CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken);

  /**
   * Persist a {@link NamedBlobRecord} in the database.
   * @param record the {@link NamedBlobRecord}
   * @param state the {@link NamedBlobState}
   * @param isUpsert the {@link Boolean}
   * @return a {@link CompletableFuture} that will eventually contain a {@link PutResult} or an exception if an error
   *         occurred.
   */
  CompletableFuture<PutResult> put(NamedBlobRecord record, NamedBlobState state, Boolean isUpsert);

  /**
   * Persist a {@link NamedBlobRecord} in the database.
   * @param record the {@link NamedBlobRecord}
   * @return a {@link CompletableFuture} that will eventually contain a {@link PutResult} or an exception if an error
   *         occurred.
   */
  default CompletableFuture<PutResult> put(NamedBlobRecord record) {
    return put(record, NamedBlobState.READY, false);
  }

  /**
   * Delete a record for a blob in the database.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param blobName the name of the blob.
   * @return a {@link CompletableFuture} that will eventually contain a {@link DeleteResult} or an exception if an error
   *         occurred.
   */
  CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName);
}
