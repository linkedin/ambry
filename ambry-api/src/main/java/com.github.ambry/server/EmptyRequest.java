package com.github.ambry.server;

import com.github.ambry.network.Request;

import java.io.InputStream;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/28/13
 * Time: 2:41 AM
 * To change this template use File | Settings | File Templates.
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
