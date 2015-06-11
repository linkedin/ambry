package com.github.ambry.rest;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration that is specific to admin functions
 */
public class RestServerConfig {
  public static String MESSAGE_HANDLER_COUNT_KEY = "rest.message.handler.count";

  /**
   * The number of handlers in the admin that process queued messages
   */
  @Config("messageHandlerCount")
  @Default("5")
  private final int messageHandlerCount;

  public int getMessageHandlerCount() {
    return messageHandlerCount;
  }

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    messageHandlerCount = verifiableProperties.getInt(MESSAGE_HANDLER_COUNT_KEY, 5);
  }
}
