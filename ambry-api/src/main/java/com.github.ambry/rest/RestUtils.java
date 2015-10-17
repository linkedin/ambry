package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.util.List;
import java.util.Map;


/**
 * Common utility functions that will be used across implementations of REST interfaces.
 */
public class RestUtils {

  /**
   * Builds {@link BlobProperties} given a {@link RestRequest}.
   * @param restRequest the {@link RestRequest} to use.
   * @return the {@link BlobProperties} extracted from {@code restRequest}.
   * @throws RestServiceException if required headers aren't present or if they aren't in the format or number
   *                                    expected.
   */
  public static BlobProperties buildBlobProperties(RestRequest restRequest)
      throws RestServiceException {
    Map<String, List<String>> args = restRequest.getArgs();

    String blobSizeStr = null;
    long blobSize;
    try {
      blobSizeStr = getHeader(args, RestConstants.Headers.Blob_Size, true);
      blobSize = Long.parseLong(blobSizeStr);
      if (blobSize < 0) {
        throw new RestServiceException(RestConstants.Headers.Blob_Size + "[" + blobSize + "] is less than 0",
            RestServiceErrorCode.InvalidArgs);
      }
    } catch (NumberFormatException e) {
      throw new RestServiceException(
          RestConstants.Headers.Blob_Size + "[" + blobSizeStr + "] could not parsed into a number",
          RestServiceErrorCode.InvalidArgs);
    }

    long ttl = Utils.Infinite_Time;
    String ttlStr = getHeader(args, RestConstants.Headers.TTL, false);
    if (ttlStr != null) {
      try {
        ttl = Long.parseLong(ttlStr);
        if (ttl < -1) {
          throw new RestServiceException(RestConstants.Headers.TTL + "[" + ttl + "] is not valid (has to be >=-1)",
              RestServiceErrorCode.InvalidArgs);
        }
      } catch (NumberFormatException e) {
        throw new RestServiceException(RestConstants.Headers.TTL + "[" + ttlStr + "] could not parsed into a number",
            RestServiceErrorCode.InvalidArgs);
      }
    }

    boolean isPrivate = false;
    String isPrivateStr = getHeader(args, RestConstants.Headers.Private, false);
    if (isPrivateStr != null) {
      if (isPrivateStr.toLowerCase().equals("true")) {
        isPrivate = true;
      } else if (!isPrivateStr.toLowerCase().equals("false")) {
        throw new RestServiceException(
            RestConstants.Headers.Private + "[" + isPrivateStr + "] has an invalid value (allowed values:true, false)",
            RestServiceErrorCode.InvalidArgs);
      }
    }

    String serviceId = getHeader(args, RestConstants.Headers.Service_Id, true);
    String contentType = getHeader(args, RestConstants.Headers.Content_Type, true);
    String ownerId = getHeader(args, RestConstants.Headers.Owner_Id, false);

    return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, ttl);
  }

  /**
   * Builds user metadata given a {@link RestRequest}.
   * @param restRequest the {@link RestRequest} to use.
   * @return the user metadata extracted from {@code restRequest}.
   */
  public static byte[] buildUsermetadata(RestRequest restRequest) {
    // TODO: after discussion.
    return new byte[0];
  }

  /**
   * Gets the value of the header {@code header} in {@code args}.
   * @param args a map of arguments to be used to look for {@code header}.
   * @param header the name of the header.
   * @param required if {@code true}, {@link IllegalArgumentException} will be thrown if {@code header} is not present
   *                 in {@code args}.
   * @return the value of {@code header} in {@code args} if it exists. If it does not exist and {@code required} is
   *          {@code false}, then returns null.
   * @throws RestServiceException if {@code required} is {@code true} and {@code header} does not exist in
   *                                    {@code args} or if there is more than one value for {@code header} in
   *                                    {@code args}.
   */
  private static String getHeader(Map<String, List<String>> args, String header, boolean required)
      throws RestServiceException {
    String value = null;
    if (args.containsKey(header)) {
      List<String> values = args.get(header);
      if (values.size() == 1) {
        value = values.get(0);
        if (value == null && required) {
          throw new RestServiceException("Request has null valu for header: " + header,
              RestServiceErrorCode.InvalidArgs);
        }
      } else {
        throw new RestServiceException("Request has too many values for header: " + header,
            RestServiceErrorCode.InvalidArgs);
      }
    } else if (required) {
      throw new RestServiceException("Request does not have required header: " + header,
          RestServiceErrorCode.MissingArgs);
    }
    return value;
  }
}
