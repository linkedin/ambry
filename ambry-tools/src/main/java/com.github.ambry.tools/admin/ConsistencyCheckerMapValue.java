package com.github.ambry.tools.admin;

import java.util.ArrayList;


public class ConsistencyCheckerMapValue {
  ArrayList<String> available;
  ArrayList<String> deletedOrExpired;
  ArrayList<String> unavailable;
  boolean isDeletedOrExpired;

  public ConsistencyCheckerMapValue(String replica, boolean isDeletedOrExpired, ArrayList<String> replicaList) {
    available = new ArrayList<String>();
    deletedOrExpired = new ArrayList<String>();
    unavailable = new ArrayList<String>();
    this.isDeletedOrExpired = isDeletedOrExpired;
    if (!isDeletedOrExpired) {
      available.add(replica);
    } else {
      deletedOrExpired.add(replica);
    }
    unavailable.addAll(replicaList);
    unavailable.remove(replica);
  }

  ArrayList<String> getAvailable() {
    return available;
  }

  void addAvailable(String replica) {
    if (!deletedOrExpired.contains(replica)) {
      this.available.add(replica);
      this.unavailable.remove(replica);
    }
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
  }

  boolean getIsDeletedOrExpired() {
    return isDeletedOrExpired;
  }

  public String toString() {
    String msg =
        "Available :: " + available + "\nDeleted/Expired :: " + deletedOrExpired + "\nUnavailable :: " + unavailable
            + "\n";
    return msg;
  }
}