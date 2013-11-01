package com.github.ambry.network;

/**
 * Simple response
 */
public interface Response {

  /**
   * Provides the send object that can be sent over the network
   * @return The send object that is part of this response
   */
  Send getPayload();

  /**
   * The original request object that this response maps to
   * @return The request object that maps to this response
   */
  Request getRequest();
}
