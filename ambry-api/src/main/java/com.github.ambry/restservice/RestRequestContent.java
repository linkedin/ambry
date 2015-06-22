package com.github.ambry.restservice;

/**
 * RestRequestContent represents piece of content in a request. It is meant to be a generic object that can be
 * understood by all the layers in a RESTful frontend. It will not contain any metadata about the request itself.
 * <p/>
 * It is possible that the underlying content is reference counted so it is important to retain content when processing
 * it async and to release it when done processing.
 * <p/>
 * Implementations are expected to be thread-safe (for {@link RestRequestContent#retain()} and
 * {@link RestRequestContent#release()}).
 */
public interface RestRequestContent {
  /**
   * Used to check if this is the last chunk of a particular request.
   * @return whether this is the last chunk.
   */
  public boolean isLast();

  /**
   * Get the underlying content as a byte array.
   * @return - the underlying content as a byte array.
   */
  public byte[] getBytes();

  /**
   * If the underlying content is reference counted, increase the reference count so that the it is not lost to
   * recycling before async processing is complete.
   */
  public void retain();

  /**
   * If the underlying content is reference counted, decrease the reference count so that it can be recycled, clean up
   * any resources and do work that needs to be done at the end of the lifecycle.
   */
  public void release();
}
