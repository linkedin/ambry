package com.github.ambry.rest;

/**
 * High level exception used to exchange error information b/w different layers and implementations.
 */
public class RestException extends Exception {
  private final RestErrorCode error;

  public RestException(String message, RestErrorCode error) {
    super(message);
    this.error = error;
  }

  public RestException(String message, Throwable e, RestErrorCode error) {
    super(message, e);
    this.error = error;
  }

  public RestException(Throwable e, RestErrorCode error) {
    super(e);
    this.error = error;
  }

  public RestErrorCode getErrorCode() {
    return this.error;
  }
}
