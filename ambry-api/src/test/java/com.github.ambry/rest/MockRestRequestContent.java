package com.github.ambry.rest;

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

  private final boolean isLast;
  private final byte[] content;

  public MockRestRequestContent(JSONObject data)
      throws InstantiationException {
    try {
      if (data.has(IS_LAST_KEY) && data.has(CONTENT_KEY)) {
        isLast = data.getBoolean(IS_LAST_KEY);
        content = data.get(CONTENT_KEY).toString().getBytes();
      } else {
        throw new InstantiationException("Given JSONObject cannot be converted to MockRestRequestContent");
      }
    } catch (JSONException e) {
      throw new InstantiationException("Could not retrieve some keys from the JSONObject " + e);
    }
  }

  @Override
  public boolean isLast() {
    return isLast;
  }

  @Override
  public int getContentSize() {
    return content.length;
  }

  @Override
  public void getBytes(int srcIndex, byte[] dst, int dstIndex, int length) {
    System.arraycopy(content, srcIndex, dst, dstIndex, length);
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
