package com.github.ambry.coordinator;

/**
 * Exception thrown by Coordinator with {@link CoordinatorError}
 */
public class CoordinatorException extends Exception {
  private static final long serialVersionUID = 1;
  private final CoordinatorError error;

  public CoordinatorException(String message, CoordinatorError error) {
    super(message);
    this.error = error;
  }

  public CoordinatorException(String message, Throwable e, CoordinatorError error) {
    super(message, e);
    this.error = error;
  }

  public CoordinatorException(Throwable e, CoordinatorError error) {
    super(e);
    this.error = error;
  }

  public CoordinatorError getErrorCode() {
    return this.error;
  }
}
