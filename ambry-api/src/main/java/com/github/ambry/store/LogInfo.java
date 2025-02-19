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

import java.util.Collections;
import java.util.List;


/**
 * Contains information about a log segment,
 * FileInfo info for the log segment,
 * FileInfo info for the linked index segments and
 * FileInfo info for the linked bloom filters.
 */
public class LogInfo {
  /**
   * FileInfo for the log segment.
   */
  private final FileInfo logSegment;

  /**
   * FileInfo for the linked index segments.
   */
  private final List<FileInfo> indexSegments;

  /**
   * FileInfo for the linked bloom filters.
   */
  private final List<FileInfo> bloomFilters;

  /**
   * Constructs a LogInfo with the given log segment, index segments and bloom filters.
   * @param logSegment FileInfo for the log segment.
   * @param indexSegments FileInfo for the linked index segments.
   * @param bloomFilters FileInfo for the linked bloom filters.
   */
  public LogInfo(
      FileInfo logSegment,
      List<FileInfo> indexSegments,
      List<FileInfo> bloomFilters) {
    this.logSegment = logSegment;
    this.indexSegments = indexSegments;
    this.bloomFilters = bloomFilters;
  }

  // TODO: Add isSealed prop
  // private final boolean isSealed;

  /**
   * @return FileInfo for the log segment.
   */
  public FileInfo getLogSegment() {
    return logSegment;
  }

  /**
   * @return FileInfo for the linked index segments.
   */
  public List<FileInfo> getIndexSegments() {
    return Collections.unmodifiableList(indexSegments);
  }

  /**
   * @return FileInfo for the linked bloom filters.
   */
  public List<FileInfo> getBloomFilters() {
    return Collections.unmodifiableList(bloomFilters);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LogInfo{")
        .append("logSegment=").append(logSegment)
        .append(", indexSegments=").append(indexSegments)
        .append(", bloomFilters=").append(bloomFilters)
        .append('}');
    return sb.toString();
  }
}
