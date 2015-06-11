package com.github.ambry.restservice;

import java.util.List;


/**
 * Interface for RestRequest
 */
public interface RestRequest extends RestObject {
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
