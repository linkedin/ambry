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
   * Return the path (the parts of the url after the domain)
   *
   * @return path
   */
  public String getPath();

  /**
   * Returns the specified part in the path (path is separated by "/")
   *
   * @param part
   * @return
   */
  public String getPathPart(int part);

  /**
   * Return the request URI
   *
   * @return request URI
   */
  public String getUri();

  /**
   * Get the value of a certain header
   *
   * @param name
   * @return
   */
  public Object getValueOfHeader(String name);

  /**
   * Return the values of a particular parameter in the URI
   *
   * @param parameter
   * @return the values of the parameter in the URI
   */
  public List<String> getValuesOfParameterInURI(String parameter);
}
