package com.github.ambry.restservice;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of RestRequest that can be used in tests.
 */
public class MockRestRequest implements RestRequest {
  public static String REST_METHOD_KEY = "restMethod";
  public static String URI_KEY = "uri";
  public static String HEADERS_KEY = "headers";

  /**
   * the underlying request data in the form of JSON
   * Contains: -
   * 1. restMethod - the http method required
   * 2. uri - the uri
   * 3. headers - all the headers as a json object
   */
  private final JSONObject data;
  private final URI uri;
  /**
   * map that stores all the query parameters as key value pairs
   */
  private Map<String, List<String>> parameterValues;

  public MockRestRequest(JSONObject data)
      throws URISyntaxException, JSONException {
    this.data = data;
    this.uri = new URI(data.getString(URI_KEY));
  }

  public RestMethod getRestMethod() {
    try {
      return RestMethod.valueOf(data.getString(REST_METHOD_KEY));
    } catch (JSONException e) {
      return null;
    }
  }

  public String getPath() {
    return uri.getPath();
  }

  public String getUri() {
    return uri.toString();
  }

  public Object getValueOfHeader(String name) {
    try {
      return data.getJSONObject(HEADERS_KEY).get(name);
    } catch (JSONException e) {
      return null;
    }
  }

  public List<String> getValuesOfParameterInURI(String parameter) {
    if (parameterValues == null) {
      try {
        generateParameterValues();
      } catch (UnsupportedEncodingException e) {
        return null;
      }
    }
    return parameterValues.get(parameter);
  }

  public void release() {
    //nothing to do
  }

  /**
   * Uses the uri and decodes all the query parameters into key value pairs. Handles multiple values for the
   * same parameter and parameters without values.
   * @throws UnsupportedEncodingException
   */
  private void generateParameterValues()
      throws UnsupportedEncodingException {
    parameterValues = new HashMap<String, List<String>>();
    for (String parameterValue : uri.getQuery().split("&")) {
      int idx = parameterValue.indexOf("=");
      String key = idx > 0 ? parameterValue.substring(0, idx) : parameterValue;
      key = URLDecoder.decode(key, "UTF-8");
      String value = idx > 0 ? URLDecoder.decode(parameterValue.substring(idx + 1), "UTF-8") : null;
      if (!parameterValues.containsKey(key)) {
        parameterValues.put(key, new LinkedList<String>());
      }
      parameterValues.get(key).add(value);
    }
  }
}
