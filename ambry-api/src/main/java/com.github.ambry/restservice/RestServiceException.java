package com.github.ambry.restservice;

/**
 * High level exception used to exchange error information b/w different layers and implementations.
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
