package com.github.ambry.store;

public interface FileInfo {
  public String getFilename();

  public String totalFileSize();

  public long getSize();
}
