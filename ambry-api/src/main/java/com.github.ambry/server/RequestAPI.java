package com.github.ambry.server;

import com.github.ambry.network.Request;
import java.io.IOException;


/**
 * This defines the server request API. The commands below are the requests that can be issued against the server
 */
public interface RequestAPI {
  /**
   * This request puts a blob into the store. It accepts a blob property, user metadata and the blob
   * as a stream and stores them
   * @param request The request that contains the blob property, user metadata and blob as a stream
   * @throws IOException
   * @throws InterruptedException
   */
  void handlePutRequest(Request request)
      throws IOException, InterruptedException;

  /**
   * This request gets blob property, user metadata or the blob from the specified partition
   * @param request The request that contains the partition and id of the blob whose blob property, user metadata or
   *                blob needs to be returned
   * @throws IOException
   * @throws InterruptedException
   */
  void handleGetRequest(Request request)
      throws IOException, InterruptedException;

  /**
   * This request deletes the blob from the store
   * @param request The request that contains the partition and id of the blob that needs to be deleted
   * @throws IOException
   * @throws InterruptedException
   */
  void handleDeleteRequest(Request request)
      throws IOException, InterruptedException;

  /**
   * This request cancels the time to live (ttl) value set on a blob. Once the ttl is cancelled, the blob lives for ever
   * @param request The request that contains the partition and id of the blob whose ttl needs to be cancelled
   * @throws IOException
   * @throws InterruptedException
   */
  void handleTTLRequest(Request request)
      throws IOException, InterruptedException;
}
