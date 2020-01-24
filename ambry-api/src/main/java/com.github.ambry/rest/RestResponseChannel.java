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

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;


/**
 * The RestResponseChannel is meant to provide a {@link NioServer} implementation independent way to return responses
 * to the client. It deals with data in terms of bytes only and is not concerned with different types of data that might
 * need to be returned from the {@link RestRequestService}.
 * <p/>
 * This functionality is mostly required by implementations of {@link RestRequestService} since they are agnostic to
 * both the REST protocol being used and the framework used for the implementation of {@link NioServer}.
 * <p/>
 * Typically, the RestResponseChannel wraps the underlying network channel and the APIs of the NIO framework to return
 * responses to clients.
 * <p/>
 * Implementations are expected to be thread-safe but use with care across different threads since there are neither
 * ordering guarantees nor operation success guarantees (e.g. if an external thread closes the channel while a write
 * attempt is in progress) - especially with concurrent writes.
 */
public interface RestResponseChannel extends AsyncWritableChannel {
  /**
   * Adds a sequence of bytes to the body of the response.
   * <p/>
   * If any write fails, all subsequent writes will fail.
   * {@inheritDoc}
   * @param src the data that needs to be written to the channel.
   * @param callback the {@link Callback} that will be invoked once the write succeeds/fails. This can be null.
   * @return a {@link Future} that will eventually contain the result of the write operation.
   */
  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback);

  /**
   * Closes the underlying network channel immediately. This should be used only if there is an error and the channel
   * needs to be closed immediately.
   * <p/>
   * Typically {@link #onResponseComplete(Exception)} will take care of closing the channel if required. It will
   * handle keep-alive headers and close the channel gracefully as opposed to this function which will simply close
   * the channel abruptly.
   * {@inheritDoc}
   * @throws IOException if there was a problem closing the channel.
   */
  @Override
  public void close() throws IOException;

  /**
   * Notifies that response handling for the request is complete (whether the request succeeded or not) and tasks that
   * need to be done after handling of a response is complete can proceed (e.g. cleanup code + closing of connection if
   * not keepalive).
   * <p/>
   * If {@code exception} is not null, then it indicates that there was an error while handling the request and
   * {@code exception} defines the error that occurred. The expectation is that an appropriate error response will be
   * constructed, returned to the client if possible and the connection closed (if required).
   * <p/>
   * It is possible that the connection might be closed/severed before this is called. Therefore this function needs to
   * always check if the channel of communication with the client is still open if it wants to send data.
   * <p/>
   * A response is considered to be complete either when a complete response has been sent to the client, an exception
   * occurred while handling the request or if the client timed out (or there was any other client side error).
   * <p/>
   * A response of OK is returned if {@code exception} is null and no response body was constructed (i.e if there were no
   * {@link #write(ByteBuffer, Callback)} calls).
   * <p/>
   * This is (has to be) called regardless of the request being concluded successfully or unsuccessfully
   * (e.g. connection interruption).
   * <p/>
   * This operation is idempotent.
   * @param exception if an error occurred, the cause of the error. Otherwise null.
   */
  public void onResponseComplete(Exception exception);

  /**
   * Sets the response status.
   * @param status the response status.
   * @throws RestServiceException if there is an error setting the header.
   */
  public void setStatus(ResponseStatus status) throws RestServiceException;

  /**
   * Gets the current {@link ResponseStatus}.
   * @return the response status.
   */
  public ResponseStatus getStatus();

  /**
   * Sets header {@code headerName} to {@code headerValue}.
   * @param headerName the name of the header to set to {@code headerValue}.
   * @param headerValue the value of the header with name {@code headerName}.
   */
  public void setHeader(String headerName, Object headerValue);

  /**
   * Gets the current value of the header with {@code headerName}.
   * @param headerName the name of the header whose value is required.
   * @return the value of the header with name {@code headerName} if it exists. {@code null} otherwise.
   */
  public Object getHeader(String headerName);
}
