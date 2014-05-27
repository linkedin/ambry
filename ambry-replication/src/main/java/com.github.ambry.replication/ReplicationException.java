package com.github.ambry.replication;

public class ReplicationException extends Exception {
  private static final long serialVersionUID = 1;

  public ReplicationException(String message) {
    super(message);
  }

  public ReplicationException(String message, Throwable e) {
    super(message, e);
  }

  public ReplicationException(Throwable e) {
    super(e);
  }
}