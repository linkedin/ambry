/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

/**
 * BlobStorageService defines a service that forms the bridge b/w a RESTful frontend and a storage backend (or something
 * that communicates with a storage backend).
 * <p/>
 * Typically, a BlobStorageService is expected to receive requests from the RESTful frontend, handle them as required
 * and either send a response (if immediately available) or pass control to another component that does further handling
 * and generates a response. The information received from the scaling layer should be enough to perform these
 * functions.
 * <p/>
 * Most operations are performed async and responses are therefore queued asynchronously instead of being available at
 * the end of the function call.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface BlobStorageService {

  /**
   * Does startup tasks for the BlobStorageService. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if BlobStorageService is unable to start.
   */
  public void start() throws InstantiationException;

  /**
   * Does shutdown tasks for the BlobStorageService. When the function returns, shutdown is FULLY complete.
   */
  public void shutdown();

  /**
   * Handles a GET operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a POST operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a PUT operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a DELETE operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a HEAD operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles an OPTIONS request.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public void handleOptions(RestRequest restRequest, RestResponseChannel restResponseChannel);
}
