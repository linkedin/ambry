/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
 * Represents information about a log segment and its associated metadata files.
 * Contains details about:
 * - The sealed log segment
 * - Associated index segments
 * - Bloom filters for efficient lookups
 */
public class LogInfo {
  // The sealed log segment containing the actual data
  FileInfo sealedSegment;

  // List of index segments associated with this log
  // Each index segment provides lookup capabilities for a portion of the log
  List<FileInfo> indexSegments;

  // List of bloom filters for efficient key lookups
  // Each bloom filter corresponds to an index segment
  List<FileInfo> bloomFilters;

  /**
   * Creates a new LogInfo instance with the specified components.
   *
   * @param sealedSegment The sealed log segment file information
   * @param indexSegments List of index segment file information
   * @param bloomFilters List of bloom filter file information
   */
  public LogInfo(FileInfo sealedSegment, List<FileInfo> indexSegments, List<FileInfo> bloomFilters) {
    this.sealedSegment = sealedSegment;
    this.indexSegments = indexSegments;
    this.bloomFilters = bloomFilters;
  }

  /**
   * Gets the sealed log segment information.
   *
   * @return FileInfo for the sealed log segment
   */
  public FileInfo getSealedSegment() {
    return sealedSegment;
  }

  /**
   * Sets the sealed log segment information.
   *
   * @param sealedSegments New sealed segment information
   */
  public void setSealedSegments(FileInfo sealedSegments) {
    this.sealedSegment = sealedSegments;
  }

  /**
   * Gets the list of index segment information.
   *
   * @return List of FileInfo objects for index segments
   */
  public List<FileInfo> getIndexSegments() {
    return indexSegments;
  }

  /**
   * Sets the list of index segment information.
   *
   * @param indexSegments New list of index segment information
   */
  public void setIndexSegments(List<FileInfo> indexSegments) {
    this.indexSegments = indexSegments;
  }

  /**
   * Gets the list of bloom filter information.
   *
   * @return List of FileInfo objects for bloom filters
   */
  public List<FileInfo> getBloomFilters() {
    return bloomFilters;
  }

  /**
   * Sets the list of bloom filter information.
   *
   * @param bloomFilters New list of bloom filter information
   */
  public void setBloomFilters(List<FileInfo> bloomFilters) {
    this.bloomFilters = bloomFilters;
  }

  /**
   * Returns a string representation of the LogInfo object.
   * Useful for logging and debugging.
   *
   * @return String containing details of sealed segment, index segments, and bloom filters
   */
  @Override
  public String toString() {
    return "LogInfo{" +
        "sealedSegment=" + sealedSegment +
        ", indexSegments=" + indexSegments +
        ", bloomFilters=" + bloomFilters +
        '}';
  }
}
