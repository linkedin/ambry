package com.github.ambry.rest;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockRestContent implements RestContent {
  public static String IS_LAST_KEY = "isLast";

  private JSONObject data;

  public MockRestContent(JSONObject data) {
    this.data = data;
  }

  public boolean isLast() {
    try {
      return data.getBoolean(IS_LAST_KEY);
    } catch (JSONException e) {
      return false;
    }
  }

  public void release() {
    //nothing to do
  }
}
