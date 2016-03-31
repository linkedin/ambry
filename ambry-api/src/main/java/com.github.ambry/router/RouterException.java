package com.github.ambry.router;

/**
 * Exceptions thrown by a {@link Router}. All exceptions are accompanied by a {@link RouterErrorCode}.
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RouterException that = (RouterException) o;
    return this.errorCode == that.errorCode && this.getCause() == that.getCause();
  }
}
