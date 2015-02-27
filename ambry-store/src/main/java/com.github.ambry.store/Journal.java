package com.github.ambry.store;

import java.util.List;


public interface Journal {
  public void addEntry(long offset, StoreKey key);
  public List<JournalEntry> getEntriesSince(long offset, boolean inclusive);
  public long getFirstOffset();
  public long getLastOffset();
}
