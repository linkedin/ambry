/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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


public class LogInfo {
  private final FileInfo logSegment;
  private final List<FileInfo> indexSegments;
  private final List<FileInfo> bloomFilters;

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

  public FileInfo getLogSegment() {
    return logSegment;
  }

  public List<FileInfo> getIndexSegments() {
    return Collections.unmodifiableList(indexSegments);
  }

  public List<FileInfo> getBloomFilters() {
    return Collections.unmodifiableList(bloomFilters);
  }

  @Override
  public String toString() {
    return "LogInfo{" +
        "logSegment=" + logSegment +
        ", indexSegments=" + indexSegments +
        ", bloomFilters=" + bloomFilters +
        '}';
  }
}
