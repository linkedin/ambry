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
import com.github.ambry.named.StaleNamedBlob;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.sql.Timestamp;
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
import java.util.stream.Collectors;


public class TestNamedBlobDb implements NamedBlobDb {
  private static final Set<GetOption> includeDeletedOptions =
      new HashSet<>(Arrays.asList(GetOption.Include_All, GetOption.Include_Deleted_Blobs));
  private static final Set<GetOption> includeExpiredOptions =
      new HashSet<>(Arrays.asList(GetOption.Include_All, GetOption.Include_Expired_Blobs));
  private final Map<String, Map<String, TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>>>>
      allRecords = new HashMap<>();
  private final Time time;
  private final int listMaxResults;
  private Exception exception;

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

    TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>> allNamedBlobsInContainer =
        allRecords.get(accountName).get(containerName);
    NavigableMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>> nextMap;
    if (pageToken == null) {
      nextMap = allNamedBlobsInContainer.tailMap(blobNamePrefix, true);
    } else {
      nextMap = allNamedBlobsInContainer.tailMap(pageToken, true);
    }
    int numRecords = 0;
    for (Map.Entry<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>> entry : nextMap.entrySet()) {
      if (!entry.getKey().startsWith(blobNamePrefix)) {
        break;
      }

      List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> recordList = entry.getValue();

      NamedBlobRecord record = recordList.get(recordList.size() - 1).getFirst();
      long deleteTs = recordList.get(recordList.size() - 1).getSecond().getSecond();
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
    putInternal(record, state, 0L);
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
      putInternal(recordWithDelete.getFirst(), NamedBlobState.READY, time.milliseconds());
    }
    future.complete(new DeleteResult(recordWithDelete.getFirst().getBlobId(), alreadyDeleted));
    return future;
  }

  @Override
  public CompletableFuture<List<StaleNamedBlob>> pullStaleBlobs() {
    CompletableFuture<List<StaleNamedBlob>> future = new CompletableFuture<>();
    List<StaleNamedBlob> resultList = new ArrayList<>();
    for (String accountName : allRecords.keySet()) {
      Map<String, TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>>> recordsPerAccount =
          allRecords.get(accountName);
      for (String containerName : recordsPerAccount.keySet()) {
        TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>> recordsPerContainer =
            recordsPerAccount.get(containerName);
        for (String blobName : recordsPerContainer.keySet()) {
          List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> recordList = recordsPerContainer.get(blobName);
          int latestReadyIndex = -1;
          for (int i = recordList.size() - 1; i >= 0; i--) {
            Pair<NamedBlobRecord, Pair<NamedBlobState, Long>> record = recordList.get(i);
            long deleteTs = record.getSecond().getSecond();
            boolean isDeleted = deleteTs != 0 && deleteTs < time.milliseconds();
            if (!isDeleted && record.getSecond().getFirst() == NamedBlobState.READY) {
              latestReadyIndex = i;
              break;
            }
          }
          if (latestReadyIndex != -1) {
            List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> recordStaleList =
                recordList.subList(latestReadyIndex + 1, recordList.size());
            recordList = recordList.subList(0, latestReadyIndex);
            recordList.addAll(recordStaleList);
          }
          for (Pair<NamedBlobRecord, Pair<NamedBlobState, Long>> r : recordList) {
            StaleNamedBlob result =
                new StaleNamedBlob((short) accountName.hashCode(), (short) containerName.hashCode(), blobName,
                    r.getFirst().getBlobId(), r.getFirst().getVersion(),
                    new Timestamp(r.getFirst().getExpirationTimeMs()));
            resultList.add(result);
          }
        }
      }
    }
    future.complete(resultList);
    return future;
  }

  @Override
  public CompletableFuture<Integer> cleanupStaleData(List<StaleNamedBlob> staleRecords) {
    CompletableFuture<Integer> future = new CompletableFuture<>();

    Set<String> staleBlobIds = staleRecords.stream().map(StaleNamedBlob::getBlobId).collect(Collectors.toSet());

    for (String accountName : allRecords.keySet()) {
      Map<String, TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>>> recordsPerAccount =
          allRecords.get(accountName);
      for (String containerName : recordsPerAccount.keySet()) {
        TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>> recordsPerContainer =
            recordsPerAccount.get(containerName);
        for (String blobName : recordsPerContainer.keySet()) {
          List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> recordList = recordsPerContainer.get(blobName);

          List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> newRecordList = recordList.stream()
              .filter(r -> !staleBlobIds.contains(r.getFirst().getBlobId()))
              .collect(Collectors.toList());
          recordsPerContainer.put(blobName, newRecordList);
        }
      }
    }
    future.complete(staleRecords.size());
    return future;
  }

  @Override
  public CompletableFuture<Set<String>> pullValidBlobIds() {
    CompletableFuture<Set<String>> future = new CompletableFuture<>();

    Set<String> blobIds = new HashSet<>();
    for (String accountName : allRecords.keySet()) {
      Map<String, TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>>> recordsPerAccount =
          allRecords.get(accountName);
      for (String containerName : recordsPerAccount.keySet()) {
        TreeMap<String, List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>>> recordsPerContainer =
            recordsPerAccount.get(containerName);
        for (String blobName : recordsPerContainer.keySet()) {
          List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> recordList = recordsPerContainer.get(blobName);
          long maxVersion = recordList.stream().filter(s -> s.getSecond().getFirst().equals(NamedBlobState.READY))
              .mapToLong(s -> s.getFirst().getVersion()).max().getAsLong();

          blobIds.addAll(
              recordList.stream()
                  .filter(
                      s -> s.getFirst().getVersion() == maxVersion && s.getSecond().getSecond() < time.milliseconds()
                  )
                  .map(s -> s.getFirst().getBlobId()).collect(Collectors.toSet()));
        }
      }
    }
    future.complete(blobIds);
    return future;
  }

  private Pair<NamedBlobRecord, Long> getInternal(String accountName, String containerName, String blobName) {
    if (allRecords.containsKey(accountName)) {
      if (allRecords.get(accountName).containsKey(containerName)) {
        if (allRecords.get(accountName).get(containerName).containsKey(blobName)) {
          List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> readyRecords = allRecords.get(accountName)
              .get(containerName)
              .get(blobName)
              .stream()
              .filter(e -> e.getSecond().getFirst() == NamedBlobState.READY)
              .collect(Collectors.toList());
          Pair<NamedBlobRecord, Pair<NamedBlobState, Long>> latestRecord = readyRecords.get(readyRecords.size() - 1);
          return new Pair<>(latestRecord.getFirst(), latestRecord.getSecond().getSecond());
        }
      }
    }
    return null;
  }

  private void putInternal(NamedBlobRecord record, NamedBlobState state, long deleteTs) {
    Pair<NamedBlobState, Long> stateAndDeleteTs = new Pair<>(state, deleteTs);
    Pair<NamedBlobRecord, Pair<NamedBlobState, Long>> newRecord = new Pair<>(record, stateAndDeleteTs);
    List<Pair<NamedBlobRecord, Pair<NamedBlobState, Long>>> recordList =
        allRecords.computeIfAbsent(record.getAccountName(), k -> new HashMap<>())
            .computeIfAbsent(record.getContainerName(), k -> new TreeMap<>())
            .getOrDefault(record.getBlobName(), new ArrayList<>());
    recordList.add(newRecord);
    allRecords.computeIfAbsent(record.getAccountName(), k -> new HashMap<>())
        .computeIfAbsent(record.getContainerName(), k -> new TreeMap<>())
        .put(record.getBlobName(), recordList);
  }

  private void setException(Exception exception) {
    this.exception = exception;
  }
}
