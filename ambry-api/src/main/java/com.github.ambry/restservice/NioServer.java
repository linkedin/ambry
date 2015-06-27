package com.github.ambry.restservice;

/**
 * Represents a NIO (non blocking I/O) server. Responsible for all network communication i.e. receiving requests
 * and sending responses and supports doing these operations in a non-blocking manner .
 * <p/>
 * A typical implementation of a NioServer will handle incoming connections from clients, decode the REST protocol
 * (usually HTTP), convert them to generic objects that the {@link BlobStorageService} can understand, instantiate a
 * NioServer specific implementation of {@link RestResponseHandler} to return responses to clients and push them down
 * the pipeline. The NioServer should be non-blocking while receiving requests and sending responses. It should also
 * provide methods that the downstream can use to control the flow of data towards the {@link BlobStorageService}.
 */
public interface NioServer {

  /**
   * Do startup tasks for the NioServer. When the function returns, startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException;

  /**
   * Do shutdown tasks for the NioServer. When the function returns, shutdown is FULLY complete.
   */
  public void shutdown();
}
