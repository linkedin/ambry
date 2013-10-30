package com.github.ambry.network;

/**
 * Simple response
 */
public interface Response {
  Send getOutput();

  Request getRequest();
}
