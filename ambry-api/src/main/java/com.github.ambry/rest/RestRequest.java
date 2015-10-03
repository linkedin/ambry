package com.github.ambry.rest;

import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * RestRequest represents a HTTP request as a generic object that can be understood by all the layers in a RESTful
 * frontend.
 * <p/>
 * It contains metadata about the request including the {@link RestMethod} desired, the original URI and associated
 * arguments (in the form of parameters or headers). It also contains all the content associated with the request
 * and this content can be streamed out through the read operations.
 * <p/>
 * It is possible that the underlying request object is reference counted so it is important to retain it when
 * processing it async and to release it when done processing.
 * <p/>
 * Closing this channel also releases retained metadata.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestRequest extends ReadableStreamChannel {
  /**
   * Gets the generic {@link RestMethod} that this request desires.
   * @return RestMethod the {@link RestMethod} defined by the request.
   */
  public RestMethod getRestMethod();

  /**
   * Return the path (the parts of the URI after the domain excluding query parameters).
   * @return path String that represents part of the uri excluding domain and query parameters.
   */
  public String getPath();

  /**
   * Return the request URI.
   * @return the URI defined by the request.
   */
  public String getUri();

  /**
   * Gets all the arguments passed as a part of the request.
   * <p/>
   * The implementation can decide what constitute as arguments. It can be specific parts of the URI, query parameters,
   * header values etc. or a combination of any of these.
   * @return the arguments and their values (if any) as a map.
   */
  public Map<String, List<String>> getArgs();

  /**
   * If the underlying request metadata is reference counted, increase the reference count so that the it is not lost to
   * recycling before async processing is complete.
   */
  public void retain();

  /**
   * If the underlying request metadata is reference counted, decrease the reference count so that it can be recycled,
   * clean up any resources and do work that needs to be done at the end of the lifecycle if required.
   */
  public void release();

  /**
   * Closes this request channel and releases all of the resources associated with it. The reference count of the
   * request metadata will be reset to the value it would have been if no {@link #retain()}s or {@link #release()}s had
   * been executed through this class.
   * <p/>
   * {@inheritDoc}
   * @throws IOException if there is an I/O error while closing the request channel.
   */
  @Override
  public void close()
      throws IOException;
}
