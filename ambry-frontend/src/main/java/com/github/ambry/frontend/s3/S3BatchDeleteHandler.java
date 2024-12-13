package com.github.ambry.frontend.s3;

import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;


public class S3BatchDeleteHandler extends S3BaseHandler {

  // S3PostHandler -> S3BatchDeleteHandler -> S3DeleteHandler -> S3DeleteObjectHandler -> DeleteBlobHandler
  public S3BatchDeleteHandler(S3DeleteHandler s3DeleteHandler, FrontendMetrics frontendMetrics) {
    super();
  }

  /**
   * Handles the S3 request and construct the response.
   *
   * @param restRequest         the {@link RestRequest} that contains the request headers and body.
   * @param restResponseChannel the {@link RestResponseChannel} that contains the response headers and body.
   * @param callback            the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback callback)
      throws RestServiceException {
   // Implement the loop for deleting keys using the s3DeleteHandler.doHandle method
  }
}
