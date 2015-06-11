package com.github.ambry.rest;

import com.github.ambry.restservice.MessageInfo;


/**
 * Show stopper for the messageInfoQueue of all message handlers
 */
public class PoisonInfo extends MessageInfo {
  public PoisonInfo() {
    super(null, null, null);
  }
}
