package com.github.ambry.router;

/**
 * TODO: write description
 */
public class RouterException extends Exception {
  private final RouterErrorCode errorCode;

  public RouterException(String message, RouterErrorCode errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public RouterException(String message, Throwable e, RouterErrorCode errorCode) {
    super(message, e);
    this.errorCode = errorCode;
  }

  public RouterException(Throwable e, RouterErrorCode errorCode) {
    super(e);
    this.errorCode = errorCode;
  }

  public RouterErrorCode getErrorCode() {
    return this.errorCode;
  }
}
