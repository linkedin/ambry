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
