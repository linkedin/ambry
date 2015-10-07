package com.github.ambry.rest;

import java.util.List;
import java.util.Map;


/**
 * RestRequestMetadata represents metadata associated with a request. For example it contains information regarding the
 * {@link RestMethod} desired, the original URI and associated arguments (in the form of parameters or headers). It is
 * meant to be a generic object that can be understood by all the layers in a RESTful frontend. It may be required to
 * interpret the operation that needs to be performed either on receipt of just the metadata or on the receipt of each
 * (or all) {@link RestRequestContent} in a request.
 * <p/>
 * It is possible that the underlying request metadata is reference counted so it is important to retain it when
 * processing it async and to release it when done processing.
 * <p/>
 * Implementations are expected to be thread-safe (for {@link RestRequestContent#retain()} and
 * {@link RestRequestContent#release()}).
 */
public interface RestRequestMetadata {
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
   * clean up any resources and do work that needs to be done at the end of the lifecycle.
   */
  public void release();
}
