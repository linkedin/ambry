package com.github.ambry.rest;

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
   * Gets the size of the content.
   * @return size of content.
   */
  public int getContentSize();

  /**
   * Transfers the content of specified length in the form of bytes starting at the specified absolute srcIndex to the
   * destination starting at the specified absolute dstIndex.
   * <p/>
   * Will throw exceptions if srcIndex < 0, dstIndex < 0, length < 0, src does not have enough data or if dst does not
   * have enough space.
   * @param srcIndex
   * @param dst
   * @param dstIndex
   * @param length
   */
  public void getBytes(int srcIndex, byte[] dst, int dstIndex, int length);

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
