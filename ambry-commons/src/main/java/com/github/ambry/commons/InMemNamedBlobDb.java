/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.github.ambry.account.Container;
import com.github.ambry.frontend.Page;
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.named.StaleNamedBlob;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


public class InMemNamedBlobDb implements NamedBlobDb {

  private static final Set<GetOption> includeDeletedOrExpiredOptions = new HashSet<>(
      Arrays.asList(GetOption.Include_All, GetOption.Include_Deleted_Blobs, GetOption.Include_Expired_Blobs));
  private final Map<String, Map<String, TreeMap<String, List<NamedBlobRow>>>> allRecords = new HashMap<>();
  private final Time time;
  private final int listMaxResults;
  private final boolean enableHardDelete;
  private Exception exception;

  public InMemNamedBlobDb(Time time, int listMaxResults, boolean enableHardDelete) {
    this.time = time;
    this.listMaxResults = listMaxResults;
    this.enableHardDelete = enableHardDelete;
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName,
      GetOption option, boolean localGet) {
    CompletableFuture<NamedBlobRecord> future = new CompletableFuture<>();
    if (exception != null) {
      future.completeExceptionally(exception);
      return future;
    }
    List<NamedBlobRow> rows = getInternal(accountName, containerName, blobName, NamedBlobState.READY);
    if (rows.isEmpty()) {
      future.completeExceptionally(new RestServiceException("NotFound", RestServiceErrorCode.NotFound));
    }

    NamedBlobRecord recordWithDelete = rows.get(rows.size() - 1).getRecord();
    if (recordWithDelete.getExpirationTimeMs() != Utils.Infinite_Time
        && recordWithDelete.getExpirationTimeMs() < time.milliseconds() && !includeDeletedOrExpiredOptions.contains(
        option)) {
      future.completeExceptionally(new RestServiceException("Deleted", RestServiceErrorCode.Deleted));
    } else {
      future.complete(recordWithDelete);
    }
    return future;
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken, Integer maxKey) {
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

    TreeMap<String, List<NamedBlobRow>> allNamedBlobsInContainer = allRecords.get(accountName).get(containerName);
    NavigableMap<String, List<NamedBlobRow>> nextMap;
    if (pageToken == null) {
      nextMap = allNamedBlobsInContainer.tailMap(blobNamePrefix, true);
    } else {
      nextMap = allNamedBlobsInContainer.tailMap(pageToken, true);
    }
    int numRecords = 0;
    for (Map.Entry<String, List<NamedBlobRow>> entry : nextMap.entrySet()) {
      if (!entry.getKey().startsWith(blobNamePrefix)) {
        break;
      }

      List<NamedBlobRow> recordList = entry.getValue();
      NamedBlobRecord record = recordList.get(recordList.size() - 1).getRecord();
      int maxKeysValue = maxKey == null ? listMaxResults : maxKey;

      if (record.getExpirationTimeMs() != Utils.Infinite_Time && record.getExpirationTimeMs() < time.milliseconds()) {
        continue;
      }
      if (numRecords++ == maxKeysValue) {
        nextContinuationToken = record.getBlobName();
        break;
      }
      entries.add(record);
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
    if (!isUpsert) {
      NamedBlobRecord currentRecord = null;
      try {
        currentRecord =
            get(record.getAccountName(), record.getContainerName(), record.getBlobName(), GetOption.None, false).get();
      } catch (Exception e) {
        // We are expecting a failure here. If upsert is not enabled, then we expect to catch a NOT_FOUND or DELETED exception
        // before we can create a new record.
      }
      if (currentRecord != null) {
        future.completeExceptionally(new RestServiceException("PUT: Blob still alive", RestServiceErrorCode.Conflict));
        return future;
      }
    }
    if (record.getVersion() == NamedBlobRecord.UNINITIALIZED_VERSION) {
      record = new NamedBlobRecord(record.getAccountName(), record.getContainerName(), record.getBlobName(),
          record.getBlobId(), record.getExpirationTimeMs(), time.milliseconds(), record.getBlobSize(),
          record.getModifiedTimeMs(), record.isDirectory());
    }
    putInternal(record, state);
    future.complete(new PutResult(record));
    return future;
  }

  @Override
  public CompletableFuture<PutResult> updateBlobTtlAndStateToReady(NamedBlobRecord record) {
    CompletableFuture<PutResult> future = new CompletableFuture<>();
    if (exception != null) {
      future.completeExceptionally(exception);
      return future;
    }
    List<NamedBlobRow> rows = getInternal(record.getAccountName(), record.getContainerName(), record.getBlobName());
    if (rows.isEmpty()) {
      future.completeExceptionally(new RestServiceException("NotFound", RestServiceErrorCode.NotFound));
      return future;
    }
    NamedBlobRow row =
        rows.stream().filter(r -> r.getRecord().getVersion() == record.getVersion()).findFirst().orElse(null);
    if (row == null) {
      future.completeExceptionally(new RestServiceException("NotFound", RestServiceErrorCode.NotFound));
      return future;
    }

    row.setBlobState(NamedBlobState.READY);
    updateExpirationTimeMs(row, Utils.Infinite_Time);
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
    List<NamedBlobRow> rows = getInternal(accountName, containerName, blobName);
    if (rows.isEmpty()) {
      future.completeExceptionally(new RestServiceException("NotFound", RestServiceErrorCode.NotFound));
    } else {
      List<DeleteResult.BlobVersion> blobVersions = new ArrayList<>();
      for (NamedBlobRow row : rows) {
        boolean alreadyDeleted = row.getRecord().getExpirationTimeMs() != Utils.Infinite_Time
            && row.getRecord().getExpirationTimeMs() < time.milliseconds();
        DeleteResult.BlobVersion blobVersion =
            new DeleteResult.BlobVersion(row.getRecord().getBlobId(), row.getRecord().getVersion(), alreadyDeleted);
        if (!alreadyDeleted) {
          updateExpirationTimeMs(row, time.milliseconds());
        }
        blobVersions.add(blobVersion);
      }
      if (enableHardDelete) {
        allRecords.get(accountName).get(containerName).remove(blobName); // remove the blob from the db
      }
      future.complete(new DeleteResult(blobVersions));
    }
    return future;
  }

  @Override
  public CompletableFuture<StaleBlobsWithLatestBlobName> pullStaleBlobs(Container container, String latestBlob) {
    CompletableFuture<StaleBlobsWithLatestBlobName> future = new CompletableFuture<>();
    List<StaleNamedBlob> resultList = new ArrayList<>();
    String containerName = container.getName();

    for (String accountName : allRecords.keySet()) {
      Map<String, TreeMap<String, List<NamedBlobRow>>> rowsPerAccount = allRecords.get(accountName);
      TreeMap<String, List<NamedBlobRow>> rowsPerContainer = rowsPerAccount.get(containerName);
      if (rowsPerContainer == null) {
        // No blobs for this container in this account, skip
        continue;
      }

      for (String blobName : rowsPerContainer.keySet()) {
        List<NamedBlobRow> rows = rowsPerContainer.get(blobName);
        int latestReadyIndex = -1;
        for (int i = rows.size() - 1; i >= 0; i--) {
          NamedBlobRow row = rows.get(i);
          long deletedTs = row.getRecord().getExpirationTimeMs();
          boolean isDeleted = deletedTs != Utils.Infinite_Time && deletedTs < time.milliseconds();
          if (latestReadyIndex != -1 && !isDeleted) {
            // when the last ready version is found, then all the prior versions that are not deleted are stale.
            StaleNamedBlob result = new StaleNamedBlob(
                (short) accountName.hashCode(),
                (short) containerName.hashCode(),
                blobName,
                row.getRecord().getBlobId(),
                row.getRecord().getVersion(),
                new Timestamp(time.milliseconds()),
                row.blobState,
                new Timestamp(row.getRecord().getModifiedTimeMs())
            );
            resultList.add(result);
          }
          if (!isDeleted && row.getBlobState() == NamedBlobState.READY) {
            latestReadyIndex = i;
          }
        }
      }
    }
    StaleBlobsWithLatestBlobName resultObj = new StaleBlobsWithLatestBlobName(resultList, "");
    future.complete(resultObj);
    return future;
  }

  @Override
  public CompletableFuture<Integer> cleanupStaleData(List<StaleNamedBlob> staleRecords) {
    CompletableFuture<Integer> future = new CompletableFuture<>();

    Set<String> staleBlobIds = staleRecords.stream().map(StaleNamedBlob::getBlobId).collect(Collectors.toSet());

    for (String accountName : allRecords.keySet()) {
      Map<String, TreeMap<String, List<NamedBlobRow>>> rowsPerAccount = allRecords.get(accountName);
      for (String containerName : rowsPerAccount.keySet()) {
        TreeMap<String, List<NamedBlobRow>> rowsPerContainer = rowsPerAccount.get(containerName);
        for (String blobName : rowsPerContainer.keySet()) {
          List<NamedBlobRow> rows = rowsPerContainer.get(blobName);
          List<NamedBlobRow> newRowList =
              rows.stream().filter(r -> !staleBlobIds.contains(r.getRecord().getBlobId())).collect(Collectors.toList());
          rowsPerContainer.put(blobName, newRowList);
        }
      }
    }
    future.complete(staleRecords.size());
    return future;
  }

  private List<NamedBlobRow> getInternal(String accountName, String containerName, String blobName,
      NamedBlobState... eligibleStates) {
    EnumSet<NamedBlobState> eligibleStatesSet = eligibleStates.length == 0 ? EnumSet.allOf(NamedBlobState.class)
        : EnumSet.copyOf(Arrays.asList(eligibleStates));
    if (allRecords.containsKey(accountName)) {
      if (allRecords.get(accountName).containsKey(containerName)) {
        if (allRecords.get(accountName).get(containerName).containsKey(blobName)) {
          return allRecords.get(accountName)
              .get(containerName).get(blobName).stream().filter(e -> eligibleStatesSet.contains(e.getBlobState()))
              .collect(Collectors.toList());
        }
      }
    }
    return new ArrayList<>();
  }

  private void putInternal(NamedBlobRecord record, NamedBlobState state) {
    record.setModifiedTimeMs(time.milliseconds());
    NamedBlobRow row = new NamedBlobRow(record, state);
    allRecords.computeIfAbsent(record.getAccountName(), k -> new HashMap<>())
        .computeIfAbsent(record.getContainerName(), k -> new TreeMap<>())
        .computeIfAbsent(record.getBlobName(), k -> new ArrayList<>())
        .add(row);
  }

  private void setException(Exception exception) {
    this.exception = exception;
  }

  private void updateExpirationTimeMs(NamedBlobRow row, long expirationTimeMs) {
    NamedBlobRecord currRecord = row.getRecord();
    // Change the expiration time (deleted ts)
    row.setRecord(
        new NamedBlobRecord(currRecord.getAccountName(), currRecord.getContainerName(), currRecord.getBlobName(),
            currRecord.getBlobId(), expirationTimeMs, currRecord.getVersion(), currRecord.getBlobSize(),
            currRecord.getModifiedTimeMs(), currRecord.isDirectory()));
  }

  @Override
  public void close() throws IOException {
  }

  private static class NamedBlobRow {
    private NamedBlobRecord record;
    private NamedBlobState blobState;

    public NamedBlobRow(NamedBlobRecord record, NamedBlobState blobState) {
      this.record = record;
      this.blobState = blobState;
    }

    public NamedBlobState getBlobState() {
      return blobState;
    }

    public void setBlobState(NamedBlobState blobState) {
      this.blobState = blobState;
    }

    public NamedBlobRecord getRecord() {
      return record;
    }

    public void setRecord(NamedBlobRecord record) {
      this.record = record;
    }
  }
}
