package com.github.ambry.clustermap;

public class FileStoreException extends RuntimeException{
  private static final long serialVersionUID = 1L;
  private final FileStoreErrorCode error;

  public FileStoreException(String s, FileStoreErrorCode error){
    super(s);
    this.error = error;
  }

  public FileStoreException(String s, FileStoreErrorCode error, Throwable throwable) {
    super(s, throwable);
    this.error = error;
  }


  public StateTransitionException.TransitionErrorCode getErrorCode() {
    return error;
  }
  public enum FileStoreErrorCode{
    FileStoreError,

    BufferNotFound,

    FileNotFound,

    FileOffsetError,
    FileWriteError
  }
}
