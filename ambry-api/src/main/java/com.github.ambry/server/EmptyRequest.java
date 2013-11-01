package com.github.ambry.server;

import com.github.ambry.network.Request;

import java.io.InputStream;

/**
 * The request class used to identify the end of the network communication
 */
public class EmptyRequest implements Request {
  private static EmptyRequest ourInstance = new EmptyRequest();

  public static EmptyRequest getInstance() {
    return ourInstance;
  }

  private EmptyRequest() {
  }

  @Override
  public InputStream getInputStream() {
    return null;
  }
}
