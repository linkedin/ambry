package com.github.ambry.restservice;

/**
 * Interface for any BlobStorage service that needs to be used with a rest server
 */
public interface BlobStorageService {

  /**
   * Do startup tasks for the BlobStorageService. Returns when startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Do shutdown tasks for the BlobStorageService. Returns when shutdown is FULLY complete.
   */
  public void shutdown();

  /**
   * Handle a GET operation.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handleGet(MessageInfo messageInfo)
      throws RestServiceException;

  /**
   * Handle a POST operation.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handlePost(MessageInfo messageInfo)
      throws RestServiceException;

  /**
   * Handle a DELETE operation.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handleDelete(MessageInfo messageInfo)
      throws RestServiceException;

  /**
   * Handle a HEAD operation.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handleHead(MessageInfo messageInfo)
      throws RestServiceException;
}
