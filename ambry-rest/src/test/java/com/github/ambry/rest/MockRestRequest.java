package com.github.ambry.rest;

import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockRestRequest implements RestRequest {
  public static String REST_METHOD_KEY = "restMethod";
  public static String URI_KEY = "uri";
  public static String HEADERS_KEY = "headers";

  private JSONObject data;
  private QueryStringDecoder query;
  private String[] pathParts;

  public MockRestRequest(JSONObject data) {
    this.data = data;
    this.query = new QueryStringDecoder(getUri());
  }

  public RestMethod getRestMethod() {
    try {
      return RestMethod.valueOf(data.getString(REST_METHOD_KEY));
    } catch (JSONException e) {
      return null;
    }
  }

  public String getPath() {
    return query.path();
  }

  public String getPathPart(int part) {
    if (pathParts == null) {
      String path = getPath();
      if (path == null) {
        return null;
      }
      path = path.startsWith("/") ? path.substring(1) : path;
      pathParts = path.split("/");
    }
    if (part >= pathParts.length) {
      return null;
    }
    return pathParts[part];
  }

  public String getUri() {
    try {
      return data.getString(URI_KEY);
    } catch (JSONException e) {
      return null;
    }
  }

  public Object getValueOfHeader(String name) {
    try {
      return data.getJSONObject(HEADERS_KEY).get(name);
    } catch (JSONException e) {
      return null;
    }
  }

  public List<String> getValuesOfParameterInURI(String parameter) {
    return query.parameters().get(parameter);
  }

  public void release() {
    //nothing to do
  }
}
