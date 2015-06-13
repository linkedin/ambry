package com.github.ambry.rest;

import com.github.ambry.restservice.MessageInfo;


/**
 * Show stopper for a RestMessageHandler instance. When this message is processed, the instance shuts down.
 */
public class PoisonInfo extends MessageInfo {
  public PoisonInfo() {
    super(null, null, null);
  }
}
