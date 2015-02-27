package com.github.ambry.store;

import java.util.ArrayList;
import java.util.List;


public class MockJournal extends InMemoryJournal {
  private List<Long> savedOffsets;
  private List<StoreKey> savedKeys;
  boolean paused;

  public MockJournal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn) {
    super(dataDir, maxEntriesToJournal, maxEntriesToReturn);
    savedOffsets = new ArrayList<Long>();
    savedKeys = new ArrayList<StoreKey>();
    paused = false;
  }

  public void pause() {
    paused = true;
  }

  public void resume() {
    for (int i = 0; i < savedOffsets.size(); i++) {
      super.addEntry(savedOffsets.get(i), savedKeys.get(i));
    }
    paused = false;
  }

  public void addEntry(long offset, StoreKey key) {
    if (paused) {
      savedOffsets.add(offset);
      savedKeys.add(key);
    } else {
      super.addEntry(offset, key);
    }
  }
}
