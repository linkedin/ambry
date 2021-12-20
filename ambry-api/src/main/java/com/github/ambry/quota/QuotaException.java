package com.github.ambry.quota;


/**
 * Exception thrown by quota logic.
 */
public class QuotaException extends Exception {
  private final boolean isRetryable;

  /**
   * @param message the exception message.
   * @param isRetryable {@code true} if the quota operation can be tried again. {@code false} otherwise.
   */
  public QuotaException(String message, boolean isRetryable) {
    super(message);
    this.isRetryable = isRetryable;
  }

  /**
   * @return {@code isRetryable}.
   */
  public boolean isRetryable() {
    return isRetryable;
  }
}
