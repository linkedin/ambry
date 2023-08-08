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

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.StringMapMessage;


/**
 * Logs requests and responses to public access log.
 */
public class PublicAccessLogger {

  //private static final Logger publicAccessLogger = LoggerFactory.getLogger("PublicAccessLogger");
  private static final Logger publicAccessLogger = LogManager.getLogger("PublicAccessLogger");

  private final String[] requestHeaders;
  private final String[] responseHeaders;
  private boolean enableStructuredLogging;

  /**
   * @param requestHeaders the request headers to log.
   * @param responseHeaders the response headers to log.
   * @param enableStructuredLogging {@code true} if we want to use structured logging (for Kusto), {@code false} otherwise
   */
  public PublicAccessLogger(String[] requestHeaders, String[] responseHeaders, boolean enableStructuredLogging) {
    this.requestHeaders = requestHeaders;
    this.responseHeaders = responseHeaders;
    this.enableStructuredLogging = enableStructuredLogging;
  }

  public String[] getRequestHeaders() {
    return requestHeaders;
  }

  public String[] getResponseHeaders() {
    return responseHeaders;
  }

  public boolean structuredLoggingEnabled() {
    return enableStructuredLogging;
  }

  public void logError(String message) {
    publicAccessLogger.error(message);
  }

  public void logError(StringMapMessage message) {
    publicAccessLogger.error(message);
  }

  public void logInfo(String message) {
    publicAccessLogger.info(message);
  }

  public void logInfo(StringMapMessage message) {
    publicAccessLogger.info(message);
  }
}
