package com.github.ambry.store;

public class SealedFileInfo {
  private String fileName;
  private final long fileSize;

  public SealedFileInfo(String fileName, Long fileSize) {
    this.fileName = fileName;
    this.fileSize = fileSize;
  }
  public String getFileName() {
    return fileName;
  }

  public Long getFileSize() {
    return fileSize;
  }

}
