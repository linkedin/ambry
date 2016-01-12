package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Common utility functions that will be used across implementations of REST interfaces.
 */
public class RestUtils {

  /**
   * Ambry specific HTTP headers.
   */
  public static final class Headers {
    /**
     * mandatory in request; long; size of blob in bytes
     */
    public final static String Blob_Size = "x-ambry-blob-size";
    /**
     * mandatory in request; string; name of service
     */
    public final static String Service_Id = "x-ambry-service-id";
    /**
     * optional in request; date string; default unset ("infinite ttl")
     */
    public final static String TTL = "x-ambry-ttl";
    /**
     * optional in request; 'true' or 'false' case insensitive; default 'false'; indicates private content
     */
    public final static String Private = "x-ambry-private";
    /**
     * mandatory in request; string; default unset; content type of blob
     */
    public final static String Content_Type = "x-ambry-content-type";
    /**
     * optional in request; string; default unset; member id.
     * <p/>
     * Expected usage is to set to member id of content owner.
     */
    public final static String Owner_Id = "x-ambry-owner-id";
    /**
     * not allowed  in request. Allowed in response only; string; time at which blob was created.
     */
    public final static String Creation_Time = "x-ambry-creation-time";
    /**
     * prefix for any header to be set as user metadata for the given blob
     */
    public final static String UserMetaData_Header_Prefix = "x-ambry-um-";
    /**
     * prefix for old style user metadata that will be served as headers
     */
    public final static String UserMetaData_OldStyle_Prefix = "x-ambry-oldstyle-um-";
  }

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
      blobSizeStr = getHeader(args, Headers.Blob_Size, true);
      blobSize = Long.parseLong(blobSizeStr);
      if (blobSize < 0) {
        throw new RestServiceException(Headers.Blob_Size + "[" + blobSize + "] is less than 0",
            RestServiceErrorCode.InvalidArgs);
      }
    } catch (NumberFormatException e) {
      throw new RestServiceException(Headers.Blob_Size + "[" + blobSizeStr + "] could not parsed into a number",
          RestServiceErrorCode.InvalidArgs);
    }

    long ttl = Utils.Infinite_Time;
    String ttlStr = getHeader(args, Headers.TTL, false);
    if (ttlStr != null) {
      try {
        ttl = Long.parseLong(ttlStr);
        if (ttl < -1) {
          throw new RestServiceException(Headers.TTL + "[" + ttl + "] is not valid (has to be >= -1)",
              RestServiceErrorCode.InvalidArgs);
        }
      } catch (NumberFormatException e) {
        throw new RestServiceException(Headers.TTL + "[" + ttlStr + "] could not parsed into a number",
            RestServiceErrorCode.InvalidArgs);
      }
    }

    boolean isPrivate;
    String isPrivateStr = getHeader(args, Headers.Private, false);
    if (isPrivateStr == null || isPrivateStr.toLowerCase().equals("false")) {
      isPrivate = false;
    } else if (isPrivateStr.toLowerCase().equals("true")) {
      isPrivate = true;
    } else {
      throw new RestServiceException(
          Headers.Private + "[" + isPrivateStr + "] has an invalid value (allowed values:true, false)",
          RestServiceErrorCode.InvalidArgs);
    }

    String serviceId = getHeader(args, Headers.Service_Id, true);
    String contentType = getHeader(args, Headers.Content_Type, true);
    String ownerId = getHeader(args, Headers.Owner_Id, false);

    return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, ttl);
  }

  /**
   * Builds user metadata given a {@link RestRequest}.
   * @param restRequest the {@link RestRequest} to use.
   * @return the user metadata extracted from {@code restRequest}.
   */
  public static byte[] buildUsermetadata(RestRequest restRequest) {
    Map<String, List<String>> args = restRequest.getArgs();
    Map<String, List<String>> userMetadataMap = new HashMap<String, List<String>>();
    int size = 0;
    size += 4; // total number of entries
    for (Map.Entry<String, List<String>> entry : args.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(Headers.UserMetaData_Header_Prefix)) {
        userMetadataMap.put(key, args.get(key));
        size += 4; // key size
        size += key.getBytes().length;
        size += 4; // size of value list
        for (String value : entry.getValue()) {
          size += 4; // size of each value
          size += value.getBytes().length;
        }
      }
    }
    ByteBuffer userMetadata = null;
    if (size == 4) {
      userMetadata = ByteBuffer.allocate(0);
    } else {
      userMetadata = ByteBuffer.allocate(size);
      userMetadata.putInt(userMetadataMap.size());
      for (Map.Entry<String, List<String>> entry : userMetadataMap.entrySet()) {
        String key = entry.getKey();
        Utils.serializeNullableASCIIEncodedString(userMetadata, key);
        userMetadata.putInt(entry.getValue().size());
        for (String value : entry.getValue()) {
          Utils.serializeNullableASCIIEncodedString(userMetadata, value);
        }
      }
    }
    return userMetadata.array();
  }

  /**
   * Fetches User metadata from the byte array
   * @param userMetadataArray the byte array which has the user metadata
   * @return Map<String,List<String>> the User Metadata that is read from the byte array
   */
  public static Map<String, List<String>> getUserMetadataFromByteArray(byte[] userMetadataArray) {
    ByteBuffer userMetadata = ByteBuffer.wrap(userMetadataArray);
    Map<String, List<String>> toReturn = new HashMap<String, List<String>>();
    if (userMetadata.remaining() != 0) {
      int size = userMetadata.getInt();
      int counter = 0;
      while (counter++ < size) {
        String key = Utils.deserializeNullableASCIIString(userMetadata);
        int valueSize = userMetadata.getInt();
        int valueCounter = 0;
        ArrayList<String> values = new ArrayList<String>();
        while (valueCounter++ < valueSize) {
          String value = Utils.deserializeNullableASCIIString(userMetadata);
          values = getListFromHeaderValue(value);
        }
        toReturn.put(key, values);
      }
    }
    return toReturn;
  }

  /**
   * Returns the header value as list of strings
   * @param value Header value to be parsed
   * @return ArrayList<String> list of string obtained from header value
   */
  public static ArrayList<String> getListFromHeaderValue(String value) {
    ArrayList<String> values = new ArrayList<String>();
    value = value.substring(1, value.length() - 1);
    String[] valueArray = value.split("\"");
    for (int i = 1; i < valueArray.length; i += 2) {
      values.add(valueArray[i]);
    }
    return values;
  }

  /**
   * Returns the header value combining a list of strings
   * @param input List<String> from which header value has to be constructed
   * @return String header value obtained from the list of strings
   */
  public static String getHeaderValueFromList(List<String> input) {
    String toReturn = "[";
    if (input.size() > 0) {
      for (String str : input) {
        toReturn += "\"" + str + "\",";
      }
      toReturn = toReturn.substring(0, toReturn.length() - 1);
    }
    toReturn += "]";
    return toReturn;
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
          throw new RestServiceException("Request has null value for header: " + header,
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

  /**
   * Sets entries from the passed in HashMap to the @{link JSONObject} headers
   * @param headers  @{link JSONObject} to which the new headers are to be added
   * @param userMetadata @{@Map} which has the new entries that has to be added
   * @throws org.json.JSONException
   */
  public static void setAmbryHeaders(JSONObject headers, Map<String, List<String>> userMetadata)
      throws JSONException {
    for (String key : userMetadata.keySet()) {
      headers.put(key, userMetadata.get(key));
    }
  }
}
