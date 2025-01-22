
package com.github.ambry.store;

public class FileInfo {
  private String fileName;
  private final long fileSize;

  public FileInfo(String fileName, Long fileSize) {
    this.fileName = fileName;
    this.fileSize = fileSize;
  }
  public String getFileName() {
    return fileName;
  }

  public Long getFileSize() {
    return fileSize;
  }

  @Override
  public String toString() {
    return "FileInfo{" +
        "fileName='" + fileName + '\'' +
        ", fileSize=" + fileSize +
        '}';
  }
}