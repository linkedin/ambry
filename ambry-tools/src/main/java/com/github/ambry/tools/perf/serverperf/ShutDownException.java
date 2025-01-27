package com.github.ambry.tools.perf.serverperf;

public class ShutDownException extends Exception {

  public ShutDownException() {

  }

  public ShutDownException(String message) {
    super(message);
  }
}
