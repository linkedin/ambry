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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


class BlobStatus {
  private final Set<String> available;
  private final Set<String> deletedOrExpired;
  private final Set<String> unavailable;
  private boolean isDeletedOrExpired;

  BlobStatus(String replica, boolean isDeletedOrExpired, ArrayList<String> replicaList) {
    available = new HashSet<>();
    deletedOrExpired = new HashSet<>();
    unavailable = new HashSet<>();
    this.isDeletedOrExpired = isDeletedOrExpired;
    if (!isDeletedOrExpired) {
      available.add(replica);
    } else {
      deletedOrExpired.add(replica);
    }
    if (replicaList != null && replicaList.size() > 0) {
      unavailable.addAll(replicaList);
      unavailable.remove(replica);
    }
  }

  Set<String> getAvailable() {
    return available;
  }

  void addAvailable(String replica) {
    this.available.add(replica);
    this.unavailable.remove(replica);
  }

  Set<String> getDeletedOrExpired() {
    return deletedOrExpired;
  }

  Set<String> getUnavailableList() {
    return unavailable;
  }

  void addDeletedOrExpired(String replica) {
    this.deletedOrExpired.add(replica);
    this.isDeletedOrExpired = true;
    this.unavailable.remove(replica);
    this.available.remove(replica);
  }

  boolean getIsDeletedOrExpired() {
    return isDeletedOrExpired;
  }

  public String toString() {
    int totalReplicas = available.size() + deletedOrExpired.size() + unavailable.size();
    String msg = "Available size: " + available.size() + " Available :: " + available + "\nDeleted/Expired size: "
        + deletedOrExpired.size() + " Deleted/Expired :: " + deletedOrExpired + "\nUnavailable size: "
        + unavailable.size() + " Unavailable :: " + unavailable + "\nTotal Replica count: " + totalReplicas;
    return msg;
  }
}