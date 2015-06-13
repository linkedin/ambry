package com.github.ambry.restservice;

/**
 * Interface that can be implemented to listen to handling results for a particular message info.
 */
public interface HandleMessageResultListener {

  public void onMessageHandleSuccess(MessageInfo messageInfo);

  public void onMessageHandleFailure(MessageInfo messageInfo, Exception e);
}
