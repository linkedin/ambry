/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.commons.FutureUtils;
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;


public class TestNamedBlobDb implements NamedBlobDb {
  private Map<String, Map<String, TreeMap<String, Pair<NamedBlobRecord, Long>>>> allRecords = new HashMap<>();
  private Exception exception;
  private final Time time;
  private final int listMaxResults;
  private static final Set<GetOption> includeDeletedOptions =
      new HashSet<>(Arrays.asList(GetOption.Include_All, GetOption.Include_Deleted_Blobs));
  private static final Set<GetOption> includeExpiredOptions =
      new HashSet<>(Arrays.asList(GetOption.Include_All, GetOption.Include_Expired_Blobs));

  public TestNamedBlobDb(Time time, int listMaxResults) {
    this.time = time;
    this.listMaxResults = listMaxResults;
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName,
      GetOption option) {
    CompletableFuture<NamedBlobRecord> future = new CompletableFuture<>();
    if (exception != null) {
      future.completeExceptionally(exception);
      return future;
    }
    Pair<NamedBlobRecord, Long> recordWithDelete = getInternal(accountName, containerName, blobName);
    if (recordWithDelete == null) {
      future.completeExceptionally(new RestServiceException("NotFound", RestServiceErrorCode.NotFound));
    } else if (recordWithDelete.getSecond() != 0 && recordWithDelete.getSecond() < time.milliseconds()
        && !includeDeletedOptions.contains(option)) {
      future.completeExceptionally(new RestServiceException("Deleted", RestServiceErrorCode.Deleted));
    } else if (recordWithDelete.getFirst().getExpirationTimeMs() != 0
        && recordWithDelete.getFirst().getExpirationTimeMs() < time.milliseconds() && !includeExpiredOptions.contains(
        option)) {
      future.completeExceptionally(new RestServiceException("Deleted", RestServiceErrorCode.Deleted));
    } else {
      future.complete(recordWithDelete.getFirst());
    }
    return future;
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken) {
    if (exception != null) {
      return FutureUtils.completedExceptionally(exception);
    }
    CompletableFuture<Page<NamedBlobRecord>> future = new CompletableFuture<>();
    List<NamedBlobRecord> entries = new ArrayList<>();
    String nextContinuationToken = null;
    if (!allRecords.containsKey(accountName) || !allRecords.get(accountName).containsKey(containerName)) {
      future.complete(new Page<>(entries, nextContinuationToken));
      return future;
    }

    TreeMap<String, Pair<NamedBlobRecord, Long>> allNamedBlobsInContainer =
        allRecords.get(accountName).get(containerName);
    NavigableMap<String, Pair<NamedBlobRecord, Long>> nextMap;
    if (pageToken == null) {
      nextMap = allNamedBlobsInContainer.tailMap(blobNamePrefix, true);
    } else {
      nextMap = allNamedBlobsInContainer.tailMap(pageToken, true);
    }
    int numRecords = 0;
    for (Map.Entry<String, Pair<NamedBlobRecord, Long>> entry : nextMap.entrySet()) {
      if (!entry.getKey().startsWith(blobNamePrefix)) {
        break;
      }

      NamedBlobRecord record = entry.getValue().getFirst();
      long deleteTs = entry.getValue().getSecond();
      if (numRecords++ == listMaxResults) {
        nextContinuationToken = record.getBlobName();
        break;
      }
      if (deleteTs != 0 && deleteTs < time.milliseconds()) {
        // ignore
      } else if (record.getExpirationTimeMs() != 0 && record.getExpirationTimeMs() < time.milliseconds()) {
        // ignore
      } else {
        entries.add(record);
      }
    }
    future.complete(new Page<>(entries, nextContinuationToken));
    return future;
  }

  @Override
  public CompletableFuture<PutResult> put(NamedBlobRecord record, NamedBlobState state, Boolean isUpsert) {
    CompletableFuture<PutResult> future = new CompletableFuture<>();
    if (exception != null) {
      future.completeExceptionally(exception);
      return future;
    }
    putInternal(record, 0L);
    future.complete(new PutResult(record));
    return future;
  }

  @Override
  public CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName) {
    CompletableFuture<DeleteResult> future = new CompletableFuture<>();
    if (exception != null) {
      future.completeExceptionally(exception);
      return future;
    }
    Pair<NamedBlobRecord, Long> recordWithDelete = getInternal(accountName, containerName, blobName);
    if (recordWithDelete == null) {
      future.completeExceptionally(new RestServiceException("NotFound", RestServiceErrorCode.NotFound));
    }
    long deleteTs = recordWithDelete.getSecond();
    boolean alreadyDeleted = true;
    if (deleteTs == 0 || deleteTs >= time.milliseconds()) {
      alreadyDeleted = false;
      putInternal(recordWithDelete.getFirst(), time.milliseconds());
    }
    future.complete(new DeleteResult(recordWithDelete.getFirst().getBlobId(), alreadyDeleted));
    return future;
  }

  private Pair<NamedBlobRecord, Long> getInternal(String accountName, String containerName, String blobName) {
    if (allRecords.containsKey(accountName)) {
      if (allRecords.get(accountName).containsKey(containerName)) {
        if (allRecords.get(accountName).get(containerName).containsKey(blobName)) {
          return allRecords.get(accountName).get(containerName).get(blobName);
        }
      }
    }
    return null;
  }

  private void putInternal(NamedBlobRecord record, long deleteTs) {
    allRecords.computeIfAbsent(record.getAccountName(), k -> new HashMap<>())
        .computeIfAbsent(record.getContainerName(), k -> new TreeMap<>())
        .put(record.getBlobName(), new Pair<>(record, deleteTs));
  }

  private void setException(Exception exception) {
    this.exception = exception;
  }
}
