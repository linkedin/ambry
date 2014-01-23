package com.github.ambry.store;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.List;

class JournalEntry {
  private long offset;
  private StoreKey key;

  public JournalEntry(long offset, StoreKey key) {
    this.offset = offset;
    this.key = key;
  }

  public long getOffset() {
    return offset;
  }

  public StoreKey getKey() {
    return key;
  }
}
/**
 * An in memory journal used to track the most recent blobs for a store.
 */
public class BlobJournal {
  private final ConcurrentSkipListMap<Long, StoreKey> journal;
  private final int maxEntriesToJournal;
  private final int maxEntriesToReturn;
  private int currentNumberOfEntries;

  /**
   * The journal that holds the most recent entries in a store sorted by offset of the blob on disk
   * @param maxEntriesToJournal The max number of entries to journal. The oldest entry will be removed from
   *                            the journal after the size is reached.
   * @param maxEntriesToReturn The max number of entries to return from the journal when queried for entries.
   */
  public BlobJournal(int maxEntriesToJournal, int maxEntriesToReturn) {
    journal = new ConcurrentSkipListMap<Long, StoreKey>();
    this.maxEntriesToJournal = maxEntriesToJournal;
    this.maxEntriesToReturn = maxEntriesToReturn;
    this.currentNumberOfEntries = 0;
  }

  /**
   * The entry that needs to be added to the journal.
   * @param offset The offset that the key pertains to. The journal verifies that the provided offset is monotonically
   *               increasing.
   * @param key The key that the entry in the journal refers to.
   */
  public void addEntry(long offset, StoreKey key) {
    if (key == null || offset < 0)
      throw new IllegalArgumentException("Invalid arguments passed to add to the journal");

    if (journal.size() > 0 && journal.lastEntry().getKey() >= offset)
      throw new IllegalArgumentException("Offsets for the journal need to be monotonically increasing. " +
              "                           Input offset does not satisfy constraint " + offset);
    if (currentNumberOfEntries == maxEntriesToJournal) {
      journal.remove(journal.firstKey());
      currentNumberOfEntries--;
    }
    journal.put(offset, key);
    currentNumberOfEntries++;
  }

  /**
   * Gets all the entries from the journal starting at the provided offset and till the maxEntriesToReturn or the
   * end of the journal is reached.
   * @param offset The offset (inclusive) from where the journal needs to return entries.
   * @return The entries in the journal starting from offset. If the offset is outside the range of the journal,
   *         it returns null.
   */
  public List<JournalEntry> getEntriesSince(long offset) {
    // To prevent synchronizing the addEntry method, we first get all the entries from the journal that are greater
    // than offset. Once we have all the required entries, we finally check if the offset is actually present
    // in the journal. If the offset is not present we return null, else we return the entries we got in the first step.
    ConcurrentNavigableMap<Long, StoreKey> subsetMap = journal.tailMap(offset, true);
    int entriesToReturn = Math.min(subsetMap.size(), maxEntriesToReturn);
    List<JournalEntry> journalEntries = new ArrayList<JournalEntry>(entriesToReturn);
    int entriesAdded = 0;
    for (Map.Entry<Long, StoreKey> entries : subsetMap.entrySet()) {
      journalEntries.add(new JournalEntry(entries.getKey(), entries.getValue()));
      entriesAdded++;
      if (entriesAdded == entriesToReturn)
        break;
    }
    if (!journal.containsKey(offset))
      return null;
    return journalEntries;
  }
}
