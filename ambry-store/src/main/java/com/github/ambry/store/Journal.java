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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class JournalEntry {
  private final Offset offset;
  private final StoreKey key;

  JournalEntry(Offset offset, StoreKey key) {
    this.offset = offset;
    this.key = key;
  }

  Offset getOffset() {
    return offset;
  }

  StoreKey getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JournalEntry entry = (JournalEntry) o;
    return this.offset == entry.offset && this.key == entry.key;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, key);
  }
}

/**
 * An in memory journal used to track the most recent blobs for a store.
 */
class Journal {

  private final ConcurrentSkipListMap<Offset, StoreKey> journal;
  private final ConcurrentHashMap<StoreKey, Long> recentCrcs;
  private final int maxEntriesToJournal;
  private final int maxEntriesToReturn;
  private final AtomicInteger currentNumberOfEntries;
  private final String dataDir;
  private static final Logger logger = LoggerFactory.getLogger(Journal.class);
  private boolean inBootstrapMode = false;

  /**
   * The journal that holds the most recent entries in a store sorted by offset of the blob on disk
   * @param maxEntriesToJournal The max number of entries to journal. The oldest entry will be removed from
   *                            the journal after the size is reached.
   * @param maxEntriesToReturn The max number of entries to return from the journal when queried for entries.
   */
  Journal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn) {
    journal = new ConcurrentSkipListMap<>();
    recentCrcs = new ConcurrentHashMap<>();
    this.maxEntriesToJournal = maxEntriesToJournal;
    this.maxEntriesToReturn = maxEntriesToReturn;
    this.currentNumberOfEntries = new AtomicInteger(0);
    this.dataDir = dataDir;
  }

  /**
   * Adds an entry into the journal with the given {@link Offset}, {@link StoreKey}, and crc.
   * @param offset The {@link Offset} that the key pertains to.
   * @param key The key that the entry in the journal refers to.
   * @param crc The crc of the object. This may be null if crc is not available.
   */
  synchronized void addEntry(Offset offset, StoreKey key, Long crc) {
    if (key == null || offset == null) {
      throw new IllegalArgumentException("Invalid arguments passed to add to the journal");
    }
    if (maxEntriesToJournal > 0) {
      if (!inBootstrapMode && currentNumberOfEntries.get() >= maxEntriesToJournal) {
        Map.Entry<Offset, StoreKey> earliestEntry = journal.firstEntry();
        journal.remove(earliestEntry.getKey());
        recentCrcs.remove(earliestEntry.getValue());
        currentNumberOfEntries.decrementAndGet();
      }
      journal.put(offset, key);
      if (crc != null) {
        recentCrcs.put(key, crc);
      }
      logger.trace("Journal : {} offset {} key {}", dataDir, offset, key);
      currentNumberOfEntries.incrementAndGet();
      logger.trace("Journal : {} number of entries {}", dataDir, currentNumberOfEntries.get());
    }
  }

  /**
   * This method should only be called when replica gets sealed. Remove all entries belongs to the auto closed log segment.
   * It will delay the addEntry method for sealed replica, so the replication performance will be hold while running this method.
   * @param activeLogSegmentName journal should remove all entries not belongs to active log segment.
   */
  synchronized void cleanUpJournal(LogSegmentName activeLogSegmentName) {
    Map.Entry<Offset, StoreKey> earliestEntry = journal.firstEntry();
    while (!inBootstrapMode && earliestEntry != null
        && earliestEntry.getKey().getName().compareTo(activeLogSegmentName) < 0) {
      journal.remove(earliestEntry.getKey());
      recentCrcs.remove(earliestEntry.getValue());
      currentNumberOfEntries.decrementAndGet();
      earliestEntry = journal.firstEntry();
    }
  }

  /**
   * This method is used to test the impact of performance between release lock after journal finish clean up and release lock each time entry gets removed.
   * @param activeLogSegmentName journal should remove all entries not belongs to active log segment.
   */
  void cleanUpJournalWithSynchronizeInside(LogSegmentName activeLogSegmentName) {
    Map.Entry<Offset, StoreKey> earliestEntry = journal.firstEntry();
    while (!inBootstrapMode && earliestEntry != null
        && earliestEntry.getKey().getName().compareTo(activeLogSegmentName) < 0) {
      synchronized (this) {
        journal.remove(earliestEntry.getKey());
        recentCrcs.remove(earliestEntry.getValue());
        currentNumberOfEntries.decrementAndGet();
        earliestEntry = journal.firstEntry();
      }
    }
  }

  /**
   * Adds an entry into the journal with the given {@link Offset}, {@link StoreKey}, and a null crc.
   * @param offset The {@link Offset} that the key pertains to.
   * @param key The key that the entry in the journal refers to.
   */
  void addEntry(Offset offset, StoreKey key) {
    addEntry(offset, key, null);
  }

  /**
   * Gets all the entries from the journal starting at the provided offset and till the maxEntriesToReturn or the
   * end of the journal is reached.
   * @param offset The {@link Offset} from where the journal needs to return entries.
   * @param inclusive if {@code true}, the returned entries (if not {@code null}), contain the entry at {@code offset}.
   * @return The entries in the journal starting from offset. If the offset is outside the range of the journal,
   *         it returns null.
   */
  List<JournalEntry> getEntriesSince(Offset offset, boolean inclusive) {
    // To prevent synchronizing the addEntry method, we first get all the entries from the journal that are greater
    // than offset. Once we have all the required entries, we finally check if the offset is actually present
    // in the journal. If the offset is not present we return null, else we return the entries we got in the first step.
    // The offset may not be present in the journal as it could be removed.

    Map.Entry<Offset, StoreKey> first = journal.firstEntry();
    Map.Entry<Offset, StoreKey> last = journal.lastEntry();

    // check if the journal contains the offset.
    if (first == null || offset.compareTo(first.getKey()) < 0 || last == null || offset.compareTo(last.getKey()) > 0
        || !journal.containsKey(offset)) {
      return null;
    }

    ConcurrentNavigableMap<Offset, StoreKey> subsetMap = journal.tailMap(offset, true);
    int entriesToReturn = Math.min(subsetMap.size(), maxEntriesToReturn);
    List<JournalEntry> journalEntries = new ArrayList<JournalEntry>(entriesToReturn);
    int entriesAdded = 0;
    for (Map.Entry<Offset, StoreKey> entry : subsetMap.entrySet()) {
      if (inclusive || !entry.getKey().equals(offset)) {
        journalEntries.add(new JournalEntry(entry.getKey(), entry.getValue()));
        entriesAdded++;
        if (entriesAdded == entriesToReturn) {
          break;
        }
      }
    }

    // Ensure that the offset was not pushed out of the journal.
    first = journal.firstEntry();
    if (first == null || offset.compareTo(first.getKey()) < 0) {
      return null;
    }

    logger.trace("Journal : {} entries returned {}", dataDir, journalEntries.size());
    return journalEntries;
  }

  /**
   * @return all journal entries at the time when this method is invoked. (Return empty list if journal is empty)
   */
  List<JournalEntry> getAllEntries() {
    List<JournalEntry> result = new ArrayList<>();
    // get current last Offset
    Offset lastOffset = getLastOffset();
    journal.entrySet();
    if (lastOffset != null) {
      // get portion view of journal whose key is less than or equal to lastOffset
      NavigableMap<Offset, StoreKey> journalView = journal.headMap(lastOffset, true);
      for (Map.Entry<Offset, StoreKey> entry : journalView.entrySet()) {
        result.add(new JournalEntry(entry.getKey(), entry.getValue()));
      }
    }
    return result;
  }

  /**
   * @return the first/smallest offset in the journal or {@code null} if no such entry exists.
   */
  Offset getFirstOffset() {
    Map.Entry<Offset, StoreKey> first = journal.firstEntry();
    return first == null ? null : first.getKey();
  }

  /**
   * @return the last/greatest offset in the journal or {@code null} if no such entry exists.
   */
  Offset getLastOffset() {
    Map.Entry<Offset, StoreKey> last = journal.lastEntry();
    return last == null ? null : last.getKey();
  }

  /**
   * Returns the crc associated with this key in the journal if there is one; else returns null.
   * @param key the key for which the crc is to be obtained.
   * @return the crc associated with this key in the journal if there is one; else returns null.
   */
  Long getCrcOfKey(StoreKey key) {
    return recentCrcs.get(key);
  }

  /**
   * @param offset the offset of the record whose key is needed
   * @return the {@link StoreKey} of the record at {@code offset}. {@code null} if the journal is not tracking that
   * offset
   */
  StoreKey getKeyAtOffset(Offset offset) {
    return journal.get(offset);
  }

  /**
   * Puts the {@link Journal} into bootstrap mode to ignore the {@code maxEntriesToJournal} constraint temporarily.
   */
  void startBootstrap() {
    inBootstrapMode = true;
  }

  /**
   * Signals the {@link Journal} is done bootstrapping and will start to honor {@code maxEntriesToJournal}.
   */
  void finishBootstrap() {
    inBootstrapMode = false;
  }

  boolean isInBootstrapMode() {
    return inBootstrapMode;
  }

  int getMaxEntriesToJournal() {
    return maxEntriesToJournal;
  }

  /**
   * @return the number of entries that is currently in the {@link Journal}.
   */
  int getCurrentNumberOfEntries() {
    return currentNumberOfEntries.get();
  }
}
