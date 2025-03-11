package com.github.ambry.store;

public class FileCopySchedulerException extends Exception{
  public FileCopySchedulerException(String message) {
    super(message);
  }

  public FileCopySchedulerException(String message, Throwable cause) {
    super(message, cause);
  }
}
