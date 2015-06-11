package com.github.ambry.restservice;

/**
 * TODO: write description
 */
public interface HandleMessageEventListener {

  public void onMessageHandleSuccess(MessageInfo messageInfo);

  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e);
}
