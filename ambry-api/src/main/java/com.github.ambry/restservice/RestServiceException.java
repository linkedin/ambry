package com.github.ambry.restservice;

/**
 * Exceptions thrown by different layers of the RESTful frontend. All exceptions are accompanied by a
 * {@link RestServiceErrorCode}.
 */
public class RestServiceException extends Exception {
  private final RestServiceErrorCode error;

  public RestServiceException(String message, RestServiceErrorCode error) {
    super(message);
    this.error = error;
  }

  public RestServiceException(String message, Throwable e, RestServiceErrorCode error) {
    super(message, e);
    this.error = error;
  }

  public RestServiceException(Throwable e, RestServiceErrorCode error) {
    super(e);
    this.error = error;
  }

  public RestServiceErrorCode getErrorCode() {
    return this.error;
  }
}
