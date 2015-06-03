package com.github.ambry.rest;

/**
 * TODO: write description
 */
public interface HandleMessageEventListener {

  public void onMessageHandleSuccess(MessageInfo messageInfo);
  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e);
}
