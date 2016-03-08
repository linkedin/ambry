package com.github.ambry.rest;

import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.Map;


/**
 * RestRequest represents a HTTP request as a generic object that can be understood by all the layers in a RESTful
 * frontend.
 * <p/>
 * It contains metadata about the request including the {@link RestMethod} desired, the original URI and associated
 * arguments (in the form of parameters or headers). It also contains all the content associated with the request
 * and this content can be streamed out through the read operations.
 * <p/>
 * Implementations are expected to be thread-safe.
 * <p/>
 * NOTE: Most of the operations performed in the REST front end are async. Therefore any reference counted objects
 * inside an implementation of this interface have to be retained for the life of the object. They can be released on
 * {@link #close()}.
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
  public Map<String, Object> getArgs();

  /**
   * Prepares the request for reading.
   * <p/>
   * Any CPU bound tasks (decoding, decryption) can be performed in this method as it is expected to be called in a CPU
   * bound thread. Calling this from an I/O bound thread will impact throughput.
   * @throws RestServiceException if request channel is closed or if the request could not be prepared for reading.
   */
  public void prepare()
      throws RestServiceException;

  /**
   * Closes this request channel and releases all of the resources associated with it. Also records some metrics via
   * the {@link RestRequestMetricsTracker} instance attached to this RestRequest.
   * <p/>
   * Expected to be called once the request is "completed" i.e. either a response is completely sent out or an error
   * state is reached from which a response cannot be sent.
   * <p/>
   * {@inheritDoc}
   * @throws IOException if there is an I/O error while closing the request channel.
   */
  @Override
  public void close()
      throws IOException;

  /**
   * Gets the {@link RestRequestMetricsTracker} instance attached to this RestRequest.
   * @return the {@link RestRequestMetricsTracker} instance attached to this RestRequest.
   */
  public RestRequestMetricsTracker getMetricsTracker();
}
