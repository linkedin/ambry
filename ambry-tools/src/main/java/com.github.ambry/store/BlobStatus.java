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


public class BlobStatus {
  ArrayList<String> available;
  ArrayList<String> deletedOrExpired;
  ArrayList<String> unavailable;
  boolean isDeletedOrExpired;

  public BlobStatus(String replica, boolean isDeletedOrExpired, ArrayList<String> replicaList) {
    available = new ArrayList<String>();
    deletedOrExpired = new ArrayList<String>();
    unavailable = new ArrayList<String>();
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

  ArrayList<String> getAvailable() {
    return available;
  }

  void addAvailable(String replica) {
    this.available.add(replica);
    this.unavailable.remove(replica);
  }

  ArrayList<String> getDeletedOrExpired() {
    return deletedOrExpired;
  }

  ArrayList<String> getUnavailableList() {
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