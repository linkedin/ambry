package com.github.ambry.clustermap;

public class FileStoreException extends RuntimeException{

  private static final long serialVersionUID = 1L;
  private final FileStoreErrorCode error;

  public FileStoreException(String s, FileStoreErrorCode error) {
    super(s);
    this.error = error;
  }

  public FileStoreException(String s, FileStoreErrorCode error, Throwable throwable) {
    super(s, throwable);
    this.error = error;
  }

  public enum FileStoreErrorCode{
    FileStoreRunningFailure
  }
}
