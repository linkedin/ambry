package com.github.ambry.restservice;

/**
 * Interface for any BlobStorage service that needs to be used with a rest server
 */
public interface BlobStorageService {

  /**
   * Do startup tasks for the BlobStorageService. Return when startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Do shutdown tasks for the BlobStorageService. Return when shutdown is FULLY complete.
   * @throws Exception
   */
  public void shutdown()
      throws Exception;

  /**
   * Handle a message given message information. Any underlying functionality has to sit behind this call
   * and message information should be enough to know what functionality is required.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handleMessage(MessageInfo messageInfo)
      throws RestServiceException;
}
