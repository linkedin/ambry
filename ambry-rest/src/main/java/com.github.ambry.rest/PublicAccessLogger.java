/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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

  /**
   * @param requestHeaders the request headers to log.
   * @param responseHeaders the response headers to log.
   */
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
