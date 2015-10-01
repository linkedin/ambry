package com.github.ambry.rest;

import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;


/**
 * RestRequestContent represents piece of content in a request. It is meant to be a generic object that can be
 * understood by all the layers in a RESTful frontend. It will not contain any metadata about the request itself.
 * <p/>
 * It is possible that the underlying content is reference counted so it is important to retain content when processing
 * it async and to release it when done processing.
 * <p/>
 * Closing this channel also releases retained content.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestRequestContent extends ReadableStreamChannel {
  /**
   * Used to check if this is the last chunk of a particular request.
   * @return whether this is the last chunk.
   */
  public boolean isLast();

  /**
   * If the underlying content is reference counted, increase the reference count so that the it is not lost to
   * recycling before async processing is complete.
   */
  public void retain();

  /**
   * If the underlying content is reference counted, decrease the reference count so that it can be recycled, clean up
   * any resources and do work that needs to be done at the end of the lifecycle if required.
   */
  public void release();

  /**
   * Closes the content channel and releases all of the resources associated with it. The reference count of the
   * underlying content will be reset to the value it would have been if no {@link #retain()}s or {@link #release()}s
   * had been executed through this class.
   * <p/>
   * {@inheritDoc}
   * @throws IOException if there is an I/O error while closing the content channel.
   */
  @Override
  public void close()
      throws IOException;
}
