package com.github.ambry.store;

import java.util.ArrayList;
import java.util.List;


/**
 * A mock journal that makes use of InMemoryJournal in the background, and provides support for pausing and
 * resuming the addition of entries to the backing InMemoryJournal. This can be used to simulate the race
 * condition of an entry getting added to the index but not yet to the journal.
 */
class MockJournal extends InMemoryJournal {
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

  @Override
  public void addEntry(long offset, StoreKey key) {
    if (paused) {
      savedOffsets.add(offset);
      savedKeys.add(key);
    } else {
      super.addEntry(offset, key);
    }
  }
}
