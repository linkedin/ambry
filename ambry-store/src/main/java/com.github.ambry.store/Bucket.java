package com.github.ambry.store;

import java.util.HashMap;


public class Bucket {
  private final long endTime;
  private final HashMap<String, Long> entries;

  public Bucket(long endTime) {
    this.endTime = endTime;
    this.entries = new HashMap<>();
  }

  public void updateValue(String key, Long value) {
    Long currValue = entries.get(key);
    if (currValue == null) {
      currValue = value;
    } else {
      currValue += value;
    }
    entries.put(key, currValue);
  }

  public Long getSize(String key) {
    return entries.get(key);
  }

  public long getEndTime() { return endTime; }

  public int getEntryCount() { return entries.size(); }

  public boolean contains(String key) { return entries.containsKey(key); }
}
