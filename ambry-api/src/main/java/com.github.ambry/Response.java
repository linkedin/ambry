package com.github.ambry;

import java.io.OutputStream;

/**
 * Simple response
 */
public interface Response {
  Send getOutput();

  Request getRequest();
}
