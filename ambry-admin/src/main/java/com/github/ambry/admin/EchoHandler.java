package com.github.ambry.admin;

import com.github.ambry.restservice.RestRequestInfo;
import com.github.ambry.restservice.RestRequestMetadata;
import com.github.ambry.restservice.RestResponseHandler;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Performs the custom {@link AdminOperationType#echo} operation supported by the admin.
 *
 */
class EchoHandler {
  static String TEXT_KEY = "text";

  /**
   * Handles {@link AdminOperationType#echo} operations.
   * <p/>
   * Extracts the parameters from the {@link RestRequestMetadata}, performs an echo if possible and writes the response
   * to the client via a {@link RestResponseHandler}.
   * <p/>
   * Flushes the written data and closes the connection on receiving an end marker (the last part of
   * {@link com.github.ambry.restservice.RestRequestContent} in the request). Any other content is ignored.
   * @param restRequestInfo
   * @throws RestServiceException
   */
  public static void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    RestResponseHandler responseHandler = restRequestInfo.getRestResponseHandler();
    if (restRequestInfo.getRestRequestContent() == null) {
      String echoStr = echo(restRequestInfo.getRestRequestMetadata()).toString();
      responseHandler.setContentType("application/json");
      responseHandler.addToResponseBody(echoStr.getBytes(), true);
    } else if (restRequestInfo.getRestRequestContent().isLast()) {
      responseHandler.flush();
      responseHandler.close();
    }
  }

  /**
   * Refers to the text provided by the client in the URI and returns a {@link JSONObject} representation of the echo.
   * @param restRequestMetadata
   * @return - A {@link JSONObject} that wraps the echoed string.
   * @throws RestServiceException
   */
  private static JSONObject echo(RestRequestMetadata restRequestMetadata)
      throws RestServiceException {
    Map<String, List<String>> parameters = restRequestMetadata.getArgs();
    if (parameters != null && parameters.containsKey(TEXT_KEY)) {
      try {
        // TODO: opportunity for batch get here.
        String text = parameters.get(TEXT_KEY).get(0);
        return packageResult(text);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to construct result object - ", e,
            RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      throw new RestServiceException("Request missing parameter - " + TEXT_KEY, RestServiceErrorCode.MissingArgs);
    }
  }

  /**
   * Packages the echoed string into a {@link JSONObject}.
   * @param text - the echoed text.
   * @return - A {@link JSONObject} that wraps the text.
   * @throws JSONException
   */
  private static JSONObject packageResult(String text)
      throws JSONException {
    JSONObject result = new JSONObject();
    result.put(TEXT_KEY, text);
    return result;
  }
}
