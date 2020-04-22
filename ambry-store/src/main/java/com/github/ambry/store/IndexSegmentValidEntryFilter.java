/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 * Interface to return valid entries from a index segment. Those valid entries will be copied to a new segment
 * in compaction process.
 */
public interface IndexSegmentValidEntryFilter {
  /**
   * Return the valid {@link IndexEntry} from a given {@link IndexSegment}.
   * @param indexSegment The {@link IndexSegment} in which to return all valid {@link IndexEntry}.
   * @param duplicateSearchSpan The {@link FileSpan} to search for duplication.
   * @param checkAlreadyCopied {@code true} if a check for existence in the swap spaces has to be executed (due to
   *                                       crash/shutdown), {@code false} otherwise.
   * @return the list of valid {@link IndexEntry} sorted by their offset.
   */
  List<IndexEntry> getValidEntry(IndexSegment indexSegment, FileSpan duplicateSearchSpan, boolean checkAlreadyCopied)
      throws StoreException;

  /**
   * Checks if a record already exists in {@code index}.
   * @param index the {@link PersistentIndex} to search in
   * @param searchSpan the {@link FileSpan} to search
   * @param key the {@link StoreKey} of the record.
   * @param srcValue the {@link IndexValue} whose existence needs to be checked.
   * @return {@code true} if the record already exists in {@code idx}, {@code false} otherwise.
   * @throws StoreException if there is any problem with using the index.
   */
  boolean alreadyExists(PersistentIndex index, FileSpan searchSpan, StoreKey key, IndexValue srcValue)
      throws StoreException;
}
