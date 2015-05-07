package com.github.ambry.rest;

import java.util.List;


/**
 * Interface for RestRequest
 */
public interface RestRequest extends RestMessage {
  /**
   * Return the generic RestMethod that this request desires.
   *
   * @return RestMethod
   */
  public RestMethod getRestMethod();

  /**
   * Return the request URI
   * @return request URI
   */
  public String getUri();

  /**
   * Return the values of a particular parameter in the URI
   * @param parameter
   * @return the values of the parameter in the URI
   */
  public List<String> getValuesOfParameterInURI(String parameter);
}
