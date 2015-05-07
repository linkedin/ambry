package com.github.ambry.admin;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration that is specific to admin functions
 */
public class AdminConfig {
  public static String HANDLER_COUNT_KEY = "ambry.admin.handler.count";

  /**
   *  The number of handlers in the admin that process queued messages
   */
  @Config("handlerCount")
  @Default("5")
  private final int handlerCount;

  public int getHandlerCount() {
    return handlerCount;
  }

  public AdminConfig(VerifiableProperties verifiableProperties) {
    handlerCount = verifiableProperties.getInt(HANDLER_COUNT_KEY, 5);
  }
}
