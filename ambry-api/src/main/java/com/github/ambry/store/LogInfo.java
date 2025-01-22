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