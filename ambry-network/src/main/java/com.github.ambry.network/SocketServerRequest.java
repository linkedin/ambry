package com.github.ambry.network;

import com.github.ambry.utils.SystemTime;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The request at the network layer
 */
class SocketServerRequest implements Request {
  private static final Logger logger = LoggerFactory.getLogger(SocketServerRequest.class);
  private final int processor;
  private final String connectionId;
  private final InputStream input;
  private final long startTimeInMs;

  SocketServerRequest(int processor, String connectionId, InputStream input) {
    this.processor = processor;
    this.connectionId = connectionId;
    this.input = input;
    this.startTimeInMs = SystemTime.getInstance().milliseconds();
    logger.trace("Processor {} received request : {}", processor, connectionId);
  }

  @Override
  public InputStream getInputStream() {
    return input;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  int getProcessor() {
    return processor;
  }

  String getConnectionId() {
    return connectionId;
  }
}
