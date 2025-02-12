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


public class LogInfo {
  FileInfo sealedSegment;
  List<FileInfo> indexSegments;
  List<FileInfo> bloomFilters;

  public LogInfo(FileInfo sealedSegment, List<FileInfo> indexSegments, List<FileInfo> bloomFilters) {
    this.sealedSegment = sealedSegment;
    this.indexSegments = indexSegments;
    this.bloomFilters = bloomFilters;
  }

  public FileInfo getSealedSegment() {
    return sealedSegment;
  }

  public void setSealedSegments(FileInfo sealedSegments) {
    this.sealedSegment = sealedSegments;
  }

  public List<FileInfo> getIndexSegments() {
    return indexSegments;
  }

  public void setIndexSegments(List<FileInfo> indexSegments) {
    this.indexSegments = indexSegments;
  }

  public List<FileInfo> getBloomFilters() {
    return bloomFilters;
  }

  public void setBloomFilters(List<FileInfo> bloomFilters) {
    this.bloomFilters = bloomFilters;
  }

  @Override
  public String toString() {
    return "LogInfo{" +
        "sealedSegment=" + sealedSegment +
        ", indexSegments=" + indexSegments +
        ", bloomFilters=" + bloomFilters +
        '}';
  }
}