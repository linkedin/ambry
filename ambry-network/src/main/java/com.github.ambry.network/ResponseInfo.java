package com.github.ambry.network;

import java.nio.ByteBuffer;


/**
 * The response from a {@link NetworkClient} comes in the form of an object of this class.
 * This class consists of the request associated with this response, along with either a non-null exception if there
 * was an error sending the request or a non-null ByteBuffer containing the successful response received for this
 * request.
 */
public class ResponseInfo {
  private final Send request;
  private final NetworkClientErrorCode error;
  private final ByteBuffer response;

  /**
   * Constructs a ResponseInfo with the given parameters.
   * @param request the request associated with this response.
   * @param error the error encountered in sending this request, if there is any.
   * @param response the response received for this request.
   */
  public ResponseInfo(Send request, NetworkClientErrorCode error, ByteBuffer response) {
    this.request = request;
    this.error = error;
    this.response = response;
  }

  /**
   * @return the request associated with this response.
   */
  public Send getRequest() {
    return request;
  }

  /**
   * @return the error encountered in sending this request.
   */
  public NetworkClientErrorCode getError() {
    return error;
  }

  /**
   * @return the response received for this request.
   */
  public ByteBuffer getResponse() {
    return response;
  }
}
