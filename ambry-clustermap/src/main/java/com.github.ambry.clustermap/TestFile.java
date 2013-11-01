package com.github.ambry.clustermap;

import org.json.JSONException;
import org.json.JSONObject;
public class TestFile {
  private JSONObject obj;

  public TestFile(String input) throws JSONException {
    obj = new JSONObject(input);
  }

  public JSONObject getJsonObj() {
    return obj;
  }
}