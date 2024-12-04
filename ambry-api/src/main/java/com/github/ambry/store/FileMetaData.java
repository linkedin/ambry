package com.github.ambry.store;

import java.util.List;


public class FileMetaData {
  SealedFileInfo sealedSegments;
  List<SealedFileInfo> indexSegments;
  List<SealedFileInfo> bloomFilters;

  public SealedFileInfo getSealedSegments() {
    return sealedSegments;
  }

  public void setSealedSegments(SealedFileInfo sealedSegments) {
    this.sealedSegments = sealedSegments;
  }

  public List<SealedFileInfo> getIndexSegments() {
    return indexSegments;
  }

  public void setIndexSegments(List<SealedFileInfo> indexSegments) {
    this.indexSegments = indexSegments;
  }

  public List<SealedFileInfo> getBloomFilters() {
    return bloomFilters;
  }

  public void setBloomFilters(List<SealedFileInfo> bloomFilters) {
    this.bloomFilters = bloomFilters;
  }
}
