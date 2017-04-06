/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Holds status of a blob from the perspective of an Index. If multiple index entries are found for the same blob,
 * everything is captured in a single instance of this class
 */
class BlobStatus {
  private final Set<String> available = new HashSet<>();
  private final Set<String> deletedOrExpired = new HashSet<>();
  private final Set<String> unavailable = new HashSet<>();
  private long earliestPutTimeMs = Utils.Infinite_Time;
  private long earliestDeleteTimeMs = Utils.Infinite_Time;
  private boolean isDeletedOrExpired;
  private AtomicBoolean belongsToRecentIndexSegment = new AtomicBoolean(false);

  BlobStatus(List<String> replicaList) {
    if (replicaList != null && replicaList.size() > 0) {
      unavailable.addAll(replicaList);
    }
  }

  Set<String> getAvailable() {
    return available;
  }

  void addAvailable(String replica, long opTimeMs) {
    this.available.add(replica);
    this.unavailable.remove(replica);
    if (earliestPutTimeMs == Utils.Infinite_Time || opTimeMs < earliestPutTimeMs) {
      earliestPutTimeMs = opTimeMs;
    }
  }

  Set<String> getDeletedOrExpired() {
    return deletedOrExpired;
  }

  Set<String> getUnavailableList() {
    return unavailable;
  }

  boolean belongsToRecentIndexSegment() {
    return belongsToRecentIndexSegment.get();
  }

  void setBelongsToRecentIndexSegment(boolean belongsToRecentIndexSegment) {
    this.belongsToRecentIndexSegment.compareAndSet(false, belongsToRecentIndexSegment);
  }

  void addDeletedOrExpired(String replica, long opTimeMs) {
    this.deletedOrExpired.add(replica);
    this.isDeletedOrExpired = true;
    this.unavailable.remove(replica);
    this.available.remove(replica);
    if (earliestDeleteTimeMs == Utils.Infinite_Time || opTimeMs < earliestDeleteTimeMs) {
      earliestDeleteTimeMs = opTimeMs;
    }
  }

  boolean isDeletedOrExpired() {
    return isDeletedOrExpired;
  }

  long getOpTime() {
    return Math.max(earliestPutTimeMs, earliestDeleteTimeMs);
  }

  public String toString() {
    int totalReplicas = available.size() + deletedOrExpired.size() + unavailable.size();
    return "Available size: " + available.size() + ", Available :: " + available + "\nDeleted/Expired size: "
        + deletedOrExpired.size() + " Deleted/Expired :: " + deletedOrExpired + "\nUnavailable size: "
        + unavailable.size() + " Unavailable :: " + unavailable + "\nTotal Replica count: " + totalReplicas;
  }
}