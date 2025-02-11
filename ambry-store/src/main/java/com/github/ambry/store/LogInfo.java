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


public class LogInfo {
  private FileInfo logSegment;
  private List<FileInfo> indexSegments;
  private List<FileInfo> bloomFilters;

  public LogInfo(FileInfo logSegment, List<FileInfo> indexSegments, List<FileInfo> bloomFilters) {
    this.logSegment = logSegment;
    this.indexSegments = indexSegments;
    this.bloomFilters = bloomFilters;
  }

  public FileInfo getLogSegment() {
    return logSegment;
  }

  public void setLogSegment(FileInfo logSegment) {
    this.logSegment = logSegment;
  }

  public List<FileInfo> getIndexSegments() {
    return Collections.unmodifiableList(indexSegments);
  }

  public void setIndexSegments(List<FileInfo> indexSegments) {
    this.indexSegments = indexSegments;
  }

  public List<FileInfo> getBloomFilters() {
    return Collections.unmodifiableList(bloomFilters);
  }

  public void setBloomFilters(List<FileInfo> bloomFilters) {
    this.bloomFilters = bloomFilters;
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