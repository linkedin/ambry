package com.github.ambry.rest;

public class MockPublicAccessLogger extends PublicAccessLogger {

  private StringBuilder publicAccessLogger = new StringBuilder();
  private StringBuilder lastPublicAccessLogEntry = new StringBuilder();

  public MockPublicAccessLogger(String[] requestHeaders, String[] responseHeaders) {
    super(requestHeaders, responseHeaders);
  }

  @Override
  public void logError(StringBuilder message) {
    publicAccessLogger.append("Error:"+ message);
    lastPublicAccessLogEntry = new StringBuilder("Error:"+message);
  }

  @Override
  public void logInfo(StringBuilder message) {
    publicAccessLogger.append("Info:" + message);
    lastPublicAccessLogEntry = new StringBuilder("Info:"+message);
  }

  public StringBuilder getLastPublicAccessLogEntry(){
    return lastPublicAccessLogEntry;
  }
}
