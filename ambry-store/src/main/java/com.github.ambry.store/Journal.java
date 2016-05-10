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

import java.util.List;


/**
 * Represents the journal used by the index. The journal supports methods to add <offset, StoreKey> pair
 * and to get entries starting from an offset.
 */
interface Journal {
  /**
   * Adds an <offset, key> pair to the journal.
   * @param offset The log offset to add to the journal
   * @param key The key at this offset in the log
   */
  public void addEntry(long offset, StoreKey key);

  /**
   * Gets entries since an offset in the journal
   * @param offset The offset representing the starting point of the range of entries to return
   * @param inclusive true, if this offset is to be included in the returned entries.
   * @return A list of JournalEntry for entries starting at this offset
   */
  public List<JournalEntry> getEntriesSince(long offset, boolean inclusive);

  /**
   * Returns the first/smallest offset in the journal
   * @return Return the first/smallest offset in the journal or -1 if no such entry exists.
   */
  public long getFirstOffset();

  /**
   * Returns the last/greatest offset in the journal
   * @return Return the last/greatest offset in the journal or -1 if no such entry exists.
   */
  public long getLastOffset();
}
