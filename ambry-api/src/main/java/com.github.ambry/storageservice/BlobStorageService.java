package com.github.ambry.storageservice;

import java.util.concurrent.TimeUnit;


/**
 * TODO: write description
 */
public interface BlobStorageService {

  public void start() throws InstantiationException;

  public void shutdown() throws Exception;

  /**
   * Await shutdown of the service for a specified amount of time.
   * @param timeout
   * @param timeUnit
   * @return true if service exited within timeout. false otherwise
   * @throws InterruptedException
   */
  public boolean awaitShutdown(long timeout, TimeUnit timeUnit) throws InterruptedException;

  /**
   * To know whether the service is up. If shutdown was called, isUp is false. To check if a service has completed
   * shutting down, use isTerminated()
   * @return
   */
  public boolean isUp();

  /**
   * returns true if and only if the service was started and was subsequently shutdown and the shutdown is complete
   * @return
   */
  public boolean isTerminated();

  public String putBlob()
      throws BlobStorageServiceException;

  public Object getBlob()
      throws BlobStorageServiceException;

  public boolean deleteBlob()
      throws BlobStorageServiceException;

  /**
   * Execute a custom operation. The data must define the type of operation
   * @param data
   * @return
   * @throws BlobStorageServiceException
   */
  public ExecutionResult execute(ExecutionData data)
      throws BlobStorageServiceException;
}
