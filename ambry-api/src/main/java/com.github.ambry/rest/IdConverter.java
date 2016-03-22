package com.github.ambry.rest;

import java.io.Closeable;


/**
 * This is a service that can be used to convert IDs across different formats.
 * </p>
 * Typical usage will be to add extensions, tailor IDs for different use-cases or encrypt/obfuscate IDs.
 */
public interface IdConverter extends Closeable {

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @return an ID that has been appropriately converted.
   */
  public String convert(RestRequest restRequest, String input);
}
