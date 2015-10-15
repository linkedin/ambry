package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.util.List;
import java.util.Map;

// TODO: need a RestUtils test.

/**
 * Common utility functions that will be used across implementations of REST interfaces.
 */
public class RestUtils {

  /**
   * Builds {@link BlobProperties} given a {@link RestRequest}.
   * @param restRequest the {@link RestRequest} to use.
   * @return the {@link BlobProperties} extracted from {@code restRequest}.
   * @throws IllegalArgumentException if required headers aren't present or if they aren't in the format or number
   *                                    expected.
   */
  public static BlobProperties buildBlobProperties(RestRequest restRequest) {
    Map<String, List<String>> args = restRequest.getArgs();

    String blobSizeStr = null;
    long blobSize;
    try {
      blobSizeStr = getHeader(args, RestConstants.Headers.Blob_Size, true);
      blobSize = Long.parseLong(blobSizeStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          RestConstants.Headers.Blob_Size + "[" + blobSizeStr + "] could not parsed into a number");
    }

    long ttl = Utils.Infinite_Time;
    String ttlStr = getHeader(args, RestConstants.Headers.TTL, false);
    if (ttlStr != null) {
      try {
        ttl = Long.parseLong(ttlStr);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            RestConstants.Headers.TTL + "[" + ttlStr + "] could not parsed into a number");
      }
    }

    boolean isPrivate = false;
    String isPrivateStr = getHeader(args, RestConstants.Headers.Private, false);
    if (isPrivateStr != null) {
      if (isPrivateStr.toLowerCase().equals("true")) {
        isPrivate = true;
      } else if (!isPrivateStr.toLowerCase().equals("false")) {
        throw new IllegalArgumentException(
            RestConstants.Headers.Private + "[" + isPrivateStr + "] has an invalid value (allowed values:true, false)");
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
    return null;
  }

  /**
   * Gets the value of the header {@code header} in {@code args}.
   * @param args a map of arguments to be used to look for {@code header}.
   * @param header the name of the header.
   * @param required if {@code true}, {@link IllegalArgumentException} will be thrown if {@code header} is not present
   *                 in {@code args}.
   * @return the value of {@code header} in {@code args} if it exists. If it does not exist and {@code required} is
   *          {@code false}, then returns null.
   * @throws IllegalArgumentException if {@code required} is {@code true} and {@code header} does not exist in
   *                                    {@code args} or if there is more than one value for {@code header} in
   *                                    {@code args}.
   */
  private static String getHeader(Map<String, List<String>> args, String header, boolean required) {
    String value = null;
    if (args.containsKey(header)) {
      List<String> values = args.get(header);
      if (values.size() == 1) {
        value = values.get(0);
      } else {
        throw new IllegalArgumentException("Request has too many values for header: " + header);
      }
    } else if (required) {
      throw new IllegalArgumentException("Request does not have required header: " + header);
    }
    return value;
  }
}
