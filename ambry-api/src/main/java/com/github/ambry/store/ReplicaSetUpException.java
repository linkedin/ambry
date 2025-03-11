package com.github.ambry.store;

public class ReplicaSetUpException extends Exception{
  public ReplicaSetUpException(String message) {
    super(message);
  }

  public ReplicaSetUpException(String message, Throwable cause) {
    super(message, cause);
  }
}
