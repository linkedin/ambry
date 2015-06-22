package com.github.ambry.restservice;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of {@link RestRequestContent} that be used in tests.
 * <p/>
 * The underlying data is in the form of a {@link JSONObject} that has the following fields: -
 * "content" - (object that should be serializable to String) - the actual content.
 * "isLast" - boolean - true if this the last content (end marker), false otherwise.
 */
public class MockRestRequestContent implements RestRequestContent {
  public static String CONTENT_KEY = "content";
  public static String IS_LAST_KEY = "isLast";

  private final JSONObject data;

  public MockRestRequestContent(JSONObject data)
      throws InstantiationException {
    if (data.has(IS_LAST_KEY) && data.has(CONTENT_KEY)) {
      this.data = data;
    } else {
      throw new InstantiationException("Given JSONObject cannot be converted to MockRestRequestContent");
    }
  }

  @Override
  public boolean isLast() {
    try {
      return data.getBoolean(IS_LAST_KEY);
    } catch (JSONException e) {
      return false;
    }
  }

  @Override
  public byte[] getBytes() {
    try {
      return data.get(CONTENT_KEY).toString().getBytes();
    } catch (JSONException e) {
      return null;
    }
  }

  @Override
  public void retain() {
    //nothing to do
  }

  @Override
  public void release() {
    //nothing to do
  }
}
