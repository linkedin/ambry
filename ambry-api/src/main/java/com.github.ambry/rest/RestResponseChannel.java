package com.github.ambry.rest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * The RestResponseChannel is meant to provide a {@link NioServer} implementation independent way to return responses
 * to the client. It deals with data in terms of bytes only and is not concerned with different types of data that might
 * need to be returned from the {@link BlobStorageService}.
 * <p/>
 * This functionality is mostly required by implementations of {@link BlobStorageService} since they are agnostic to
 * both the REST protocol being used and the framework used for the implementation of {@link NioServer}.
 * <p/>
 * Typically, the RestResponseChannel wraps the APIs provided by the framework used for an implementation of
 * {@link NioServer} to return responses to clients.
 * <p/>
 * Implementations are expected to be thread-safe but use with care across different threads since there are neither
 * ordering guarantees nor operation success guarantees (e.g. if an external thread closes the channel while a write
 * attempt is in progress).
 */
public interface RestResponseChannel extends WritableByteChannel {
  /**
   * Adds a sequence of bytes to the body of the response. Requests a write to the underlying channel before returning.
   * <p/>
   * An attempt is made to add up to {@code src.remaining()} bytes to the response but the number requested to be
   * written to the underlying channel might be lesser depending on the free space in the channel's write buffer.
   * <p/>
   * This method might not have sent data to the wire upon return. The bytes in the {@code src} might still be in use.
   * <p/>
   * {@link #flush()} need not be called after every invocation of this function (and for performance reasons, not
   * advisable either) but be sure to call it if you want to reuse {@code src} or if you want to send all pending data
   * to actual transport.
   * <p/>
   * If the write fails sometime in the future, the channel may be closed.
   * <p/>
   * This method may be invoked at any time. However, if another thread has already initiated a write operation upon
   * this channel, then an invocation of this method will block until the first operation is complete.
   * @see WritableByteChannel#write(ByteBuffer) for details on how {@code src.position()},  {@code src.remaining()} and
   * {@code src.limit()} are used.
   * @param src the buffer from which bytes are to be retrieved.
   * @return the number of bytes that were requested to be written to the channel, possibly zero.
   * @throws java.nio.channels.ClosedChannelException if this channel is closed.
   * @throws IOException if some other I/O error occurs.
   */
  @Override
  public int write(ByteBuffer src)
      throws IOException;

  /**
   * Flushes all pending messages in the channel to transport.
   * @throws RestServiceException if there is an error while flushing to channel.
   */
  public void flush()
      throws RestServiceException;

  /**
   * Notifies that request handling is complete (successfully or unsuccessfully) and tasks that need to be done after
   * handling of a request is complete can proceed (e.g. cleanup code + closing of connection if not keepalive).
   * <p/>
   * If {@code cause} is not null, then it indicates that there was an error while handling the request and
   * {@code cause} defines the error that occurred. The expectation is that an appropriate error response will be
   * constructed, returned to the client if possible and the connection closed (if required).
   * <p/>
   * It is possible that the connection might be closed/severed before this is called. Therefore this function needs to
   * always check if the channel of communication with the client is still open if it wants to send data.
   * <p/>
   * A request is considered to be complete when all {@link RestRequestInfo}s associated with the same request have been
   * processed or if there was an error while handling the request or if the client timed out.
   * <p/>
   * A response of OK is returned if {@code cause} is null and no response body was constructed (i.e if there were no
   * {@link #write(ByteBuffer)} calls).
   * <p/>
   * This is (has to be) called regardless of the request being concluded successfully or unsuccessfully
   * (e.g. connection interruption).
   * <p/>
   * This operation is idempotent.
   * @param cause if an error occurred, the cause of the error. Otherwise null.
   * @param forceClose whether the connection needs to be forcibly closed.
   */
  public void onRequestComplete(Throwable cause, boolean forceClose);

  /**
   * Returns completion status of the request being handled by this RestResponseChannel.
   * @return {@code true} if there has been a call to {@link #onRequestComplete(Throwable, boolean)} and the request has
   * been marked complete. {@code false} otherwise.
   */
  public boolean isRequestComplete();

  // Header helper functions.
  //
  // We will add more as we discover uses for them.
  // 1. Haven't added one for http version because that is going to be 1.1 for now.
  // 2. Haven't added for status since status is OK unless there is an exception.
  //
  // For the exception case, we directly convert the exception error code to a response code.
  //
  // May need to add later - content length, keep alive
  // If we discover other use cases, can add here.

  /**
   * Sets the content-type of the response. Expected to be MIME types.
   * @param type the content-type of the data in the response.
   * @throws RestServiceException if there is an error while setting the content-type.
   */
  public void setContentType(String type)
      throws RestServiceException;
}
