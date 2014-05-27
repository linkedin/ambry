package com.github.ambry.shared;

/**
 * Exception used by the connection pool to indicate that the operation timedout
 */
public class ConnectionPoolTimeoutException extends Exception {
  private static final long serialVersionUID = 1;

  public ConnectionPoolTimeoutException(String message) {
    super(message);
  }

  public ConnectionPoolTimeoutException(String message, Throwable e) {
    super(message, e);
  }

  public ConnectionPoolTimeoutException(Throwable e) {
    super(e);
  }
}
