package com.github.ambry.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.Assert;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PublicAccessLoggerTest {
  private Logger publicAccessLogger = LoggerFactory.getLogger("PublicAccessLogger");
  private final String REQUEST_HEADER_PREFIX = "requestHeader";
  private final String RESPONSE_HEADER_PREFIX = "responseHeader";

  @Test
  public void testPublicAccessLoggerHeaders() {
    String[] requestHeaders = new String[]{REQUEST_HEADER_PREFIX + "1", REQUEST_HEADER_PREFIX + "2"};
    String[] responseHeaders = new String[]{RESPONSE_HEADER_PREFIX + "1", RESPONSE_HEADER_PREFIX + "2"};
    PublicAccessLogger publicAccessLogger = new PublicAccessLogger(requestHeaders, responseHeaders);
    Assert.assertTrue("Request Headers mismatch ",
        Arrays.deepEquals(publicAccessLogger.getRequestHeaders(), requestHeaders));
    Assert.assertTrue("Response Headers mismatch ",
        Arrays.deepEquals(publicAccessLogger.getResponseHeaders(), responseHeaders));
  }

  @Test
  public void testPublicAccessLogger() {
    Logger publicAccessLogger = LoggerFactory.getLogger("PublicAccessLogger");
  }

  @Test
  public void testSomething() {
    MockAppender mockAppender = new MockAppender();
    /*Logger.getRootLogger().addAppender(mockAppender);

    // Check INFO is set (from log4j.xml).
    level = (String) mbserver.getAttribute(objectName, "Level");
    assertEquals("INFO", level);*/
  }

  public static final class MockAppender extends AppenderSkeleton {
    private final List<LoggingEvent> logLines;

    public MockAppender() {
      logLines = new ArrayList<LoggingEvent>();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(LoggingEvent event) {
      logLines.add(event);
    }

    public List<LoggingEvent> getLogLines() {
      return logLines;
    }
  }
}
