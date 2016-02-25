package com.github.ambry.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Logs requests and responses to public access log.
 */
public class PublicAccessLogger {

  private Logger publicAccessLogger = LoggerFactory.getLogger("PublicAccessLogger");

  private final String[] requestHeaders;
  private final String[] responseHeaders;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public PublicAccessLogger(String[] requestHeaders, String[] responseHeaders) {
    this.requestHeaders = requestHeaders;
    this.responseHeaders = responseHeaders;
    logger.trace("Created PublicAccessLogger for log " + publicAccessLogger.getName());
  }

  public String[] getRequestHeaders() {
    return requestHeaders;
  }

  public String[] getResponseHeaders() {
    return responseHeaders;
  }

  public void logError(String message) {
    publicAccessLogger.error(message);
  }

  public void logInfo(String message) {
    publicAccessLogger.info(message);
  }
}
