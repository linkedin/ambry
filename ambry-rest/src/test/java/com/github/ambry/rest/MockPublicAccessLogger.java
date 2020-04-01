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

public class MockPublicAccessLogger extends PublicAccessLogger {

  private StringBuilder publicAccessLogger = new StringBuilder();
  private String lastPublicAccessLogEntry = new String();

  public MockPublicAccessLogger(String[] requestHeaders, String[] responseHeaders) {
    super(requestHeaders, responseHeaders);
  }

  @Override
  public void logError(String message) {
    lastPublicAccessLogEntry = "Error:" + message;
    publicAccessLogger.append(lastPublicAccessLogEntry);
  }

  @Override
  public void logInfo(String message) {
    lastPublicAccessLogEntry = "Info:" + message;
    publicAccessLogger.append(lastPublicAccessLogEntry);
  }

  public String getLastPublicAccessLogEntry() {
    return lastPublicAccessLogEntry;
  }
}
