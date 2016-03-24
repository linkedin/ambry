package com.github.ambry.rest;

import com.github.ambry.router.Callback;
import java.io.Closeable;
import java.util.concurrent.Future;


/**
 * This is a service that can be used to convert IDs across different formats.
 * </p>
 * Typical usage will be to add extensions, tailor IDs for different use-cases, encrypt/obfuscate IDs or get name
 * mappings.
 */
public interface IdConverter extends Closeable {

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
   * @return a {@link Future} that will eventually contain the converted ID.
   */
  public Future<String> convert(RestRequest restRequest, String input, Callback<String> callback);
}
