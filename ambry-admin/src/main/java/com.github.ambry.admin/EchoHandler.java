package com.github.ambry.admin;

import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs the custom {@link AdminBlobStorageService#ECHO} operation supported by the admin.
 */
class EchoHandler {
  protected static String TEXT_KEY = "text";
  private static Logger logger = LoggerFactory.getLogger(EchoHandler.class);

  /**
   * Handles {@link AdminBlobStorageService#ECHO} operations.
   * <p/>
   * Extracts the echo text from the {@code restRequest}, performs an echo if possible, packages the echo response in
   * a {@link JSONObject} and makes the object available via a {@link ReadableStreamChannel}.
   * <p/>
   * Content sent via the {@code restRequest} is ignored.
   * @param restRequest {@link RestRequest} containing details of the request.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers in.
   * @param adminMetrics {@link AdminMetrics} instance to track errors and latencies.
   * @return a {@link ReadableStreamChannel} that contains the echo response.
   * @throws RestServiceException if there was any problem constructing the response.
   */
  public static ReadableStreamChannel handleGetRequest(RestRequest restRequest, RestResponseChannel restResponseChannel,
      AdminMetrics adminMetrics)
      throws RestServiceException {
    logger.trace("Handling echo - {}", restRequest.getUri());
    adminMetrics.echoRate.mark();
    long startTime = System.currentTimeMillis();
    ReadableStreamChannel channel = null;
    try {
      String echoStr = echo(restRequest, adminMetrics).toString();
      restResponseChannel.setContentType("application/json");
      channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(echoStr.getBytes()));
    } finally {
      // TODO: rethink these processing metrics.
      long processingTime = System.currentTimeMillis() - startTime;
      logger.trace("Processing echo response for request {} took {} ms", restRequest.getUri(), processingTime);
      adminMetrics.echoProcessingTimeInMs.update(processingTime);
    }
    return channel;
  }

  /**
   * Refers to the text provided by the client in the URI and returns a {@link JSONObject} representation of the echo.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @param adminMetrics {@link AdminMetrics} instance to track errors and latencies.
   * @return A {@link JSONObject} that wraps the echoed string.
   * @throws RestServiceException if there were missing parameters or if there was a {@link JSONException} while
   *                                building the response.
   */
  private static JSONObject echo(RestRequest restRequest, AdminMetrics adminMetrics)
      throws RestServiceException {
    Map<String, List<String>> parameters = restRequest.getArgs();
    if (parameters != null && parameters.containsKey(TEXT_KEY)) {
      String text = parameters.get(TEXT_KEY).get(0);
      logger.trace("Text to echo for request {} is {}", restRequest.getUri(), text);
      try {
        return packageResult(text);
      } catch (JSONException e) {
        adminMetrics.echoGetResponseBuildingError.inc();
        throw new RestServiceException("Unable to construct result JSON object during echo GET - ", e,
            RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      adminMetrics.echoGetMissingParameterError.inc();
      throw new RestServiceException("Request for echo GET missing parameter - " + TEXT_KEY,
          RestServiceErrorCode.MissingArgs);
    }
  }

  /**
   * Packages the echoed string into a {@link JSONObject}.
   * @param text the echoed text.
   * @return A {@link JSONObject} that wraps the text.
   * @throws JSONException if there was an error building the {@link JSONObject}.
   */
  private static JSONObject packageResult(String text)
      throws JSONException {
    JSONObject result = new JSONObject();
    result.put(TEXT_KEY, text);
    return result;
  }
}
