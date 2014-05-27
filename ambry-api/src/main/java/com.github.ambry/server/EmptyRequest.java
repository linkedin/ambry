package com.github.ambry.server;

import com.github.ambry.network.Request;
import com.github.ambry.utils.SystemTime;

import java.io.InputStream;


/**
 * The request class used to identify the end of the network communication
 */
public class EmptyRequest implements Request {
  private final long startTimeInMs;
  private static EmptyRequest ourInstance = new EmptyRequest();

  public static EmptyRequest getInstance() {
    return ourInstance;
  }

  private EmptyRequest() {
    startTimeInMs = SystemTime.getInstance().milliseconds();
  }

  @Override
  public InputStream getInputStream() {
    return null;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }
}
