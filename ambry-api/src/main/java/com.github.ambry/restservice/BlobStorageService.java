package com.github.ambry.restservice;

/**
 * TODO: write description
 */
public interface BlobStorageService {

  public void start()
      throws InstantiationException;

  public void shutdown()
      throws Exception;

  public void handleMessage(MessageInfo messageInfo)
      throws RestServiceException;
}
