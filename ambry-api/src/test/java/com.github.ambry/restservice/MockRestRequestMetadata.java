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
 * Implementation of {@link RestRequestMetadata} that can be used in tests.
 * <p/>
 * The underlying request metadata is in the form of a {@link JSONObject} that contains the following fields: -
 * 1. "restMethod" - {@link RestMethod} - the rest method required.
 * 2. "uri" - String - the uri.
 @ 3. "headers" - {@link JSONObject} - all the headers as key value pairs.
 */
public class MockRestRequestMetadata implements RestRequestMetadata {
  public static String REST_METHOD_KEY = "restMethod";
  public static String URI_KEY = "uri";
  public static String HEADERS_KEY = "headers";

  private final JSONObject data;
  private final URI uri;
  private final Map<String, List<String>> parameterValues;

  public MockRestRequestMetadata(JSONObject data)
      throws URISyntaxException, JSONException {
    this.data = data;
    this.uri = new URI(data.getString(URI_KEY));

    // decodes parameters in the URI. Handles parameters without values and multiple values for the same parameter.
    if (uri.getQuery() != null) {
      parameterValues = new HashMap<String, List<String>>();
      for (String parameterValue : uri.getQuery().split("&")) {
        try {
          int idx = parameterValue.indexOf("=");
          String key = idx > 0 ? parameterValue.substring(0, idx) : parameterValue;
          key = URLDecoder.decode(key, "UTF-8");
          String value = idx > 0 ? URLDecoder.decode(parameterValue.substring(idx + 1), "UTF-8") : null;
          if (!parameterValues.containsKey(key)) {
            parameterValues.put(key, new LinkedList<String>());
          }
          parameterValues.get(key).add(value);
        } catch (UnsupportedEncodingException e) {
          // continue.
        }
      }
    } else {
      parameterValues = null;
    }
  }

  @Override
  public RestMethod getRestMethod() {
    try {
      return RestMethod.valueOf(data.getString(REST_METHOD_KEY));
    } catch (JSONException e) {
      return RestMethod.UNKNOWN;
    } catch (IllegalArgumentException e) {
      return RestMethod.UNKNOWN;
    }
  }

  @Override
  public String getPath() {
    return uri.getPath();
  }

  @Override
  public String getUri() {
    return uri.toString();
  }

  @Override
  public Map<String, List<String>> getArgs() {
    return parameterValues;
  }

  @Override
  public void retain() {
    // nothing to do
  }

  @Override
  public void release() {
    //nothing to do
  }
}
