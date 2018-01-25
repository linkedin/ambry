/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Common utility functions that will be used across implementations of REST interfaces.
 */
public class RestUtils {

  /**
   * Ambry specific HTTP headers.
   */
  public static final class Headers {
    // general headers
    /**
     * {@code "Cache-Control"}
     */
    public static final String CACHE_CONTROL = "Cache-Control";
    /**
     * {@code "Content-Length"}
     */
    public static final String CONTENT_LENGTH = "Content-Length";
    /**
     * {@code "Content-Type"}
     */
    public static final String CONTENT_TYPE = "Content-Type";
    /**
     * {@code "Date"}
     */
    public static final String DATE = "Date";
    /**
     * {@code "Expires"}
     */
    public static final String EXPIRES = "Expires";
    /**
     * {@code "Last-Modified"}
     */
    public static final String LAST_MODIFIED = "Last-Modified";
    /**
     * {@code "Location"}
     */
    public static final String LOCATION = "Location";
    /**
     * {@code "Pragma"}
     */
    public static final String PRAGMA = "Pragma";
    /**
     * {@code "Accept-Ranges"}
     */
    public static final String ACCEPT_RANGES = "Accept-Ranges";
    /**
     * {@code "Content-Range"}
     */
    public static final String CONTENT_RANGE = "Content-Range";
    /**
     * {@code "Range"}
     */
    public static final String RANGE = "Range";

    // ambry specific headers
    /**
     * mandatory in request; long; size of blob in bytes
     */
    public final static String BLOB_SIZE = "x-ambry-blob-size";
    /**
     * mandatory in request; string; name of service
     */
    public final static String SERVICE_ID = "x-ambry-service-id";
    /**
     * for put request; string; name of target account
     */
    public final static String TARGET_ACCOUNT_NAME = "x-ambry-target-account-name";
    /**
     * for put request; string; name of the target container
     */
    public final static String TARGET_CONTAINER_NAME = "x-ambry-target-container-name";
    /**
     * optional in request; date string; default unset ("infinite ttl")
     */
    public final static String TTL = "x-ambry-ttl";
    /**
     * optional in request; 'true' or 'false' case insensitive; default 'false'; indicates private content
     */
    public final static String PRIVATE = "x-ambry-private";
    /**
     * mandatory in request; string; default unset; content type of blob
     */
    public final static String AMBRY_CONTENT_TYPE = "x-ambry-content-type";
    /**
     * optional in request; string; default unset; member id.
     * <p/>
     * Expected usage is to set to member id of content owner.
     */
    public final static String OWNER_ID = "x-ambry-owner-id";
    /**
     * optional in request; defines an option while getting the blob and is optional support in a
     * {@link BlobStorageService}. Valid values are available in {@link GetOption}. Defaults to {@link GetOption#None}
     */
    public final static String GET_OPTION = "x-ambry-get-option";
    /**
     * not allowed  in request. Allowed in response only; string; time at which blob was created.
     */
    public final static String CREATION_TIME = "x-ambry-creation-time";
    /**
     * The type of signed URL requested (for e.g, POST or GET).
     */
    public static final String URL_TYPE = "x-ambry-url-type";
    /**
     * The TTL (in secs) of the signed URL.
     */
    public static final String URL_TTL = "x-ambry-url-ttl-secs";
    /**
     * The maximum size of the blob that can be uploaded using the URL.
     */
    public static final String MAX_UPLOAD_SIZE = "x-ambry-max-upload-size";
    /**
     * The blob ID requested by the URL.
     */
    public static final String BLOB_ID = "x-ambry-blob-id";
    /**
     * The signed URL header name in the response for signed url requests.
     */
    public static final String SIGNED_URL = "x-ambry-signed-url";
    /**
     * prefix for any header to be set as user metadata for the given blob
     */
    public final static String USER_META_DATA_HEADER_PREFIX = "x-ambry-um-";

    /**
     * Header to contain the Cookies
     */
    public final static String COOKIE = "Cookie";
    /**
     * Header to be set by the clients during a Get blob call to denote, that blob should be served only if the blob
     * has been modified after the value set for this header.
     */
    public static final String IF_MODIFIED_SINCE = "If-Modified-Since";

    /**
     * Header that is set in the response of OPTIONS request that specifies the allowed methods.
     */
    public static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";

    /**
     * Header that is set in the response of OPTIONS request that specifies the validity of the options returned.
     */
    public static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";
    /**
     * Header that is set in the response of GetBlobInfo
     * 'true' or 'false' case insensitive; true indicates content is encrypted at the storage layer. false otherwise
     */
    public final static String ENCRYPTED_IN_STORAGE = "x-ambry-encrypted-in-storage";
  }

  /**
   * Ambry specific keys used internally in a {@link RestRequest}.
   */
  public static final class InternalKeys {

    /**
     * The key for the target {@link com.github.ambry.account.Account} indicated by the request.
     */
    public final static String TARGET_ACCOUNT_KEY = "ambry-internal-key-target-account";

    /**
     * The key for the target {@link com.github.ambry.account.Container} indicated by the request.
     */
    public final static String TARGET_CONTAINER_KEY = "ambry-internal-key-target-container";
  }

  /**
   * Permitted sub-resources of a blob.
   */
  public enum SubResource {
    /**
     * User metadata and BlobProperties i.e., blob properties returned in headers and user metadata as content/headers.
     */
    BlobInfo,

    /**
     * User metadata on its own i.e., no "blob properties" headers returned with response.
     */
    UserMetadata,

    /**
     * All the replicas of the blob ID returned as content (Admin only).
     * <p/>
     * "replicas" here means the string representation of all the replicas (i.e. host:port/path) where the blob might
     * reside.
     */
    Replicas
  }

  public static final class MultipartPost {
    public final static String BLOB_PART = "Blob";
    public final static String USER_METADATA_PART = "UserMetadata";
  }

  public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String BYTE_RANGE_UNITS = "bytes";
  private static final int CRC_SIZE = 8;
  private static final short USER_METADATA_VERSION_V1 = 1;
  private static final String BYTE_RANGE_PREFIX = BYTE_RANGE_UNITS + "=";
  private static Logger logger = LoggerFactory.getLogger(RestUtils.class);

  /**
   * Builds {@link BlobProperties} given the arguments associated with a request.
   * @param args the arguments associated with the request. Cannot be {@code null}.
   * @return the {@link BlobProperties} extracted from the arguments.
   * @throws RestServiceException if required arguments aren't present or if they aren't in the format or number
   *                                    expected.
   */
  public static BlobProperties buildBlobProperties(Map<String, Object> args) throws RestServiceException {
    Account account = getAccountFromArgs(args);
    Container container = getContainerFromArgs(args);
    String serviceId = getHeader(args, Headers.SERVICE_ID, true);
    String contentType = getHeader(args, Headers.AMBRY_CONTENT_TYPE, true);
    String ownerId = getHeader(args, Headers.OWNER_ID, false);

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

    // This field should not matter on newly created blobs, because all privacy/cacheability decisions should be made
    // based on the container properties and ACLs. For now, BlobProperties still includes this field, though.
    boolean isPrivate = !container.isCacheable();
    return new BlobProperties(-1, serviceId, ownerId, contentType, isPrivate, ttl, account.getId(), container.getId(),
        container.isEncrypted());
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |   size    |  total   |           |          |             |            |            |            |            |
   * | version | excluding |  no of   | key1 size |   key1   | value1 size |  value 1   |  key2 size |     ...    |     Crc    |
   * |(2 bytes)| ver & crc | entries  | (4 bytes) |(key1 size| (4 bytes)   |(value1 size|  (4 bytes) |     ...    |  (8 bytes) |
   * |         | (4 bytes) | (4 bytes)|           |   bytes) |             |   bytes)   |            |            |            |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version        - The version of the user metadata record
   *
   *  size exluding
   *  ver & CRC      - The size of the user metadata content excluding the version and the CRC
   *
   *  total no of
   *  entries        - Total number of entries in user metadata
   *
   *  key1 size      - Size of 1st key
   *
   *  key1           - Content of key1
   *
   *  value1 size    - Size of 1st value
   *
   *  value1         - Content of value1
   *
   *  key2 size      - Size of 2nd key
   *
   *  crc        - The crc of the user metadata record
   *
   */

  /**
   * Builds user metadata given the arguments associated with a request.
   * @param args the arguments associated with the request.
   * @return the user metadata extracted from arguments.
   * @throws RestServiceException if usermetadata arguments have null values.
   */
  public static byte[] buildUsermetadata(Map<String, Object> args) throws RestServiceException {
    ByteBuffer userMetadata;
    if (args.containsKey(MultipartPost.USER_METADATA_PART)) {
      userMetadata = (ByteBuffer) args.get(MultipartPost.USER_METADATA_PART);
    } else {
      Map<String, String> userMetadataMap = new HashMap<String, String>();
      int sizeToAllocate = 0;
      for (Map.Entry<String, Object> entry : args.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith(Headers.USER_META_DATA_HEADER_PREFIX)) {
          // key size
          sizeToAllocate += 4;
          String keyToStore = key.substring(Headers.USER_META_DATA_HEADER_PREFIX.length());
          sizeToAllocate += keyToStore.getBytes(StandardCharsets.US_ASCII).length;
          String value = getHeader(args, key, true);
          userMetadataMap.put(keyToStore, value);
          // value size
          sizeToAllocate += 4;
          sizeToAllocate += value.getBytes(StandardCharsets.US_ASCII).length;
        }
      }
      if (sizeToAllocate == 0) {
        userMetadata = ByteBuffer.allocate(0);
      } else {
        // version
        sizeToAllocate += 2;
        // size excluding version and crc
        sizeToAllocate += 4;
        // total number of entries
        sizeToAllocate += 4;
        // crc size
        sizeToAllocate += CRC_SIZE;
        userMetadata = ByteBuffer.allocate(sizeToAllocate);
        userMetadata.putShort(USER_METADATA_VERSION_V1);
        // total size = sizeToAllocate - version size - sizeToAllocate size - crc size
        userMetadata.putInt(sizeToAllocate - 6 - CRC_SIZE);
        userMetadata.putInt(userMetadataMap.size());
        for (Map.Entry<String, String> entry : userMetadataMap.entrySet()) {
          String key = entry.getKey();
          Utils.serializeString(userMetadata, key, StandardCharsets.US_ASCII);
          Utils.serializeString(userMetadata, entry.getValue(), StandardCharsets.US_ASCII);
        }
        Crc32 crc = new Crc32();
        crc.update(userMetadata.array(), 0, sizeToAllocate - CRC_SIZE);
        userMetadata.putLong(crc.getValue());
      }
    }
    return userMetadata.array();
  }

  /**
   * Gets deserialized metadata from the byte array if possible
   * @param userMetadata the byte array which has the user metadata
   * @return the user metadata that is read from the byte array, or {@code null} if the {@code userMetadata} cannot be
   * parsed in expected format
   */
  public static Map<String, String> buildUserMetadata(byte[] userMetadata) throws RestServiceException {
    Map<String, String> toReturn = null;
    if (userMetadata.length > 0) {
      try {
        ByteBuffer userMetadataBuffer = ByteBuffer.wrap(userMetadata);
        short version = userMetadataBuffer.getShort();
        switch (version) {
          case USER_METADATA_VERSION_V1:
            int sizeToRead = userMetadataBuffer.getInt();
            if (sizeToRead != (userMetadataBuffer.remaining() - 8)) {
              logger.trace("Size didn't match. Returning null");
            } else {
              int entryCount = userMetadataBuffer.getInt();
              int counter = 0;
              if (entryCount > 0) {
                toReturn = new HashMap<>();
              }
              while (counter++ < entryCount) {
                String key = Utils.deserializeString(userMetadataBuffer, StandardCharsets.US_ASCII);
                String value = Utils.deserializeString(userMetadataBuffer, StandardCharsets.US_ASCII);
                toReturn.put(Headers.USER_META_DATA_HEADER_PREFIX + key, value);
              }
              long actualCRC = userMetadataBuffer.getLong();
              Crc32 crc32 = new Crc32();
              crc32.update(userMetadata, 0, userMetadata.length - CRC_SIZE);
              long expectedCRC = crc32.getValue();
              if (actualCRC != expectedCRC) {
                logger.trace("corrupt data while parsing user metadata Expected CRC " + expectedCRC + " Actual CRC "
                    + actualCRC);
                toReturn = null;
              }
            }
            break;
          default:
            logger.trace("Failed to parse version in new format. Returning null");
        }
      } catch (RuntimeException e) {
        logger.trace("Runtime Exception on parsing user metadata. Returning null");
        toReturn = null;
      }
    }
    return toReturn;
  }

  /**
   * Build a {@link GetBlobOptions} object from an argument map for a certain sub-resource.
   * @param args the arguments associated with the request. This is typically a map of header names and query string
   *             arguments to values.
   * @param subResource the {@link SubResource} for the request, or {@code null} if no sub-resource is requested.
   * @param getOption the {@link GetOption} required.
   * @return a populated {@link GetBlobOptions} object.
   * @throws RestServiceException if the {@link GetBlobOptions} could not be constructed.
   */
  public static GetBlobOptions buildGetBlobOptions(Map<String, Object> args, SubResource subResource,
      GetOption getOption) throws RestServiceException {
    String rangeHeaderValue = getHeader(args, Headers.RANGE, false);
    if (subResource != null && rangeHeaderValue != null) {
      throw new RestServiceException("Ranges not supported for sub-resources.", RestServiceErrorCode.InvalidArgs);
    }
    return new GetBlobOptionsBuilder().operationType(
        subResource == null ? GetBlobOptions.OperationType.All : GetBlobOptions.OperationType.BlobInfo)
        .getOption(getOption)
        .range(rangeHeaderValue != null ? RestUtils.buildByteRange(rangeHeaderValue) : null)
        .build();
  }

  /**
   * Build the value for the Content-Range header that corresponds to the provided range and blob size. The returned
   * Content-Range header value will be in the following format: {@code {a}-{b}/{c}}, where {@code {a}} is the inclusive
   * start byte offset of the returned range, {@code {b}} is the inclusive end byte offset of the returned range, and
   * {@code {c}} is the total size of the blob in bytes. This function also generates the range length in bytes.
   * @param range a {@link ByteRange} used to generate the Content-Range header.
   * @param blobSize the total size of the associated blob in bytes.
   * @return a {@link Pair} containing the content range header value and the content length in bytes.
   */
  public static Pair<String, Long> buildContentRangeAndLength(ByteRange range, long blobSize)
      throws RestServiceException {
    try {
      range = range.toResolvedByteRange(blobSize);
    } catch (IllegalArgumentException e) {
      throw new RestServiceException("Range provided was not satisfiable.", e,
          RestServiceErrorCode.RangeNotSatisfiable);
    }
    return new Pair<>(BYTE_RANGE_UNITS + " " + range.getStartOffset() + "-" + range.getEndOffset() + "/" + blobSize,
        range.getRangeSize());
  }

  /**
   * Looks at the URI to determine the type of operation required or the blob ID that an operation needs to be
   * performed on.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @param subResource the {@link RestUtils.SubResource} if one is present. {@code null} otherwise.
   * @param prefixesToRemove the list of prefixes that need to be removed from the URI before extraction. Removal of
   *                         prefixes earlier in the list will be preferred to removal of the ones later in the list.
   * @return extracted operation type or blob ID from the URI.
   */
  public static String getOperationOrBlobIdFromUri(RestRequest restRequest, RestUtils.SubResource subResource,
      List<String> prefixesToRemove) {
    String path = restRequest.getPath();
    int startIndex = 0;

    // remove query string.
    int endIndex = path.indexOf("?");
    if (endIndex == -1) {
      endIndex = path.length();
    }

    // remove prefix.
    if (prefixesToRemove != null) {
      for (String prefix : prefixesToRemove) {
        if (path.startsWith(prefix)) {
          startIndex = prefix.length();
          break;
        }
      }
    }

    // remove subresource if present.
    if (subResource != null) {
      // "- 1" removes the "slash" that precedes the sub-resource.
      endIndex = endIndex - subResource.name().length() - 1;
    }
    String operationOrBlobId = path.substring(startIndex, endIndex);
    if ((operationOrBlobId.isEmpty() || operationOrBlobId.equals("/")) && restRequest.getArgs()
        .containsKey(Headers.BLOB_ID)) {
      operationOrBlobId = restRequest.getArgs().get(Headers.BLOB_ID).toString();
    }
    return operationOrBlobId;
  }

  /**
   * Determines if URI is for a blob sub-resource, and if so, returns that sub-resource
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @return sub-resource if the URI includes one; null otherwise.
   */
  public static RestUtils.SubResource getBlobSubResource(RestRequest restRequest) {
    String path = restRequest.getPath();
    final int minSegmentsRequired = path.startsWith("/") ? 3 : 2;
    String[] segments = path.split("/");
    RestUtils.SubResource subResource = null;
    if (segments.length >= minSegmentsRequired) {
      try {
        subResource = RestUtils.SubResource.valueOf(segments[segments.length - 1]);
      } catch (IllegalArgumentException e) {
        // nothing to do.
      }
    }
    return subResource;
  }

  /**
   * Fetch time in ms for the {@code dateString} passed in, since epoch
   * @param dateString the String representation of the date that needs to be parsed
   * @return Time in ms since epoch. Note http time is kept in Seconds so last three digits will be 000.
   *         Returns null if the {@code dateString} is not in the expected format or could not be parsed
   */
  public static Long getTimeFromDateString(String dateString) {
    try {
      SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
      return dateFormatter.parse(dateString).getTime();
    } catch (ParseException e) {
      logger.warn("Could not parse milliseconds from an HTTP date header (" + dateString + ").");
      return null;
    }
  }

  /**
   * Reduces the precision of a time in milliseconds to seconds precision. Result returned is in milliseconds with last
   * three digits 000. Useful for comparing times kept in milliseconds that get converted to seconds and back (as is
   * done with HTTP date format).
   *
   * @param ms time that needs to be parsed
   * @return milliseconds with seconds precision (last three digits 000).
   */
  public static long toSecondsPrecisionInMs(long ms) {
    return ms - (ms % 1000);
  }

  /**
   * Gets the {@link GetOption} required by the request.
   * @param restRequest the representation of the request.
   * @return the required {@link GetOption}. Defaults to {@link GetOption#None}.
   * @throws RestServiceException if the {@link RestUtils.Headers#GET_OPTION} is present but not recognized.
   */
  public static GetOption getGetOption(RestRequest restRequest) throws RestServiceException {
    GetOption options = GetOption.None;
    Map<String, Object> args = restRequest.getArgs();
    Object value = args.get(RestUtils.Headers.GET_OPTION);
    if (value != null) {
      String str = (String) value;
      boolean foundMatch = false;
      for (GetOption getOption : GetOption.values()) {
        if (str.equalsIgnoreCase(getOption.name())) {
          options = getOption;
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        throw new RestServiceException("Unrecognized value for [" + RestUtils.Headers.GET_OPTION + "]: " + str,
            RestServiceErrorCode.InvalidArgs);
      }
    }
    return options;
  }

  /**
   * Gets the isPrivate setting from the args.
   * @param args The args where to include the isPrivate setting.
   * @return A boolean to indicate the value of the isPrivate flag.
   * @throws RestServiceException if exception occurs during parsing the arg.
   */
  public static boolean isPrivate(Map<String, Object> args) throws RestServiceException {
    boolean isPrivate;
    String isPrivateStr = getHeader(args, Headers.PRIVATE, false);
    if (isPrivateStr == null || isPrivateStr.toLowerCase().equals("false")) {
      isPrivate = false;
    } else if (isPrivateStr.toLowerCase().equals("true")) {
      isPrivate = true;
    } else {
      throw new RestServiceException(
          Headers.PRIVATE + "[" + isPrivateStr + "] has an invalid value (allowed values:true, false)",
          RestServiceErrorCode.InvalidArgs);
    }
    return isPrivate;
  }

  /**
   * Ensures the required headers are present.
   * @param restRequest The {@link RestRequest} to ensure header presence. Cannot be {@code null}.
   * @param requiredHeaders A set of headers to check presence. Cannot be {@code null}.
   * @throws RestServiceException if any of the headers is missing.
   */
  public static void ensureRequiredHeadersOrThrow(RestRequest restRequest, Set<String> requiredHeaders)
      throws RestServiceException {
    Map<String, Object> args = restRequest.getArgs();
    for (String header : requiredHeaders) {
      getHeader(args, header, true);
    }
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
  public static String getHeader(Map<String, Object> args, String header, boolean required)
      throws RestServiceException {
    String value = null;
    if (args.containsKey(header)) {
      Object valueObj = args.get(header);
      value = valueObj != null ? valueObj.toString() : null;
      if (value == null && required) {
        throw new RestServiceException("Request has null value for header: " + header,
            RestServiceErrorCode.InvalidArgs);
      }
    } else if (required) {
      throw new RestServiceException("Request does not have required header: " + header,
          RestServiceErrorCode.MissingArgs);
    }
    return value;
  }

  /**
   * Gets the value of a header as a {@link Long}
   * @param args a map of arguments to be used to look for {@code header}.
   * @param header the name of the header.
   * @param required if {@code true}, {@link IllegalArgumentException} will be thrown if {@code header} is not present
   *                 in {@code args}.
   * @return the value of {@code header} in {@code args} if it exists. If it does not exist and {@code required} is
   *          {@code false}, then returns null.
   * @throws RestServiceException same as cases of {@link #getHeader(Map, String, boolean)} and if the value cannot be
   *                              converted to a {@link Long}.
   */
  public static Long getLongHeader(Map<String, Object> args, String header, boolean required)
      throws RestServiceException {
    // if getHeader() is no longer called, tests for this function have to be changed.
    String value = getHeader(args, header, required);
    if (value != null) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw new RestServiceException("Invalid value for " + header + ": " + value, e,
            RestServiceErrorCode.InvalidArgs);
      }
    }
    return null;
  }

  /**
   * Extract the injected {@link Account} from a map of arguments.
   * @param args a map of arguments that possibly contains the injected {@link Account} and {@link Container}.
   * @return the {@link Account}
   * @throws RestServiceException
   */
  public static Account getAccountFromArgs(Map<String, Object> args) throws RestServiceException {
    Object account = args.get(InternalKeys.TARGET_ACCOUNT_KEY);
    if (account == null) {
      throw new RestServiceException(InternalKeys.TARGET_ACCOUNT_KEY + " is not set",
          RestServiceErrorCode.InternalServerError);
    }
    if (!(account instanceof Account)) {
      throw new RestServiceException(InternalKeys.TARGET_ACCOUNT_KEY + " not instance of Account",
          RestServiceErrorCode.InternalServerError);
    }
    return (Account) account;
  }

  /**
   * Extract the injected {@link Container} from a map of arguments.
   * @param args a map of arguments that possibly contains the injected {@link Account} and {@link Container}.
   * @return a {@link Container}
   * @throws RestServiceException
   */
  public static Container getContainerFromArgs(Map<String, Object> args) throws RestServiceException {
    Object container = args.get(InternalKeys.TARGET_CONTAINER_KEY);
    if (container == null) {
      throw new RestServiceException(InternalKeys.TARGET_CONTAINER_KEY + " is not set",
          RestServiceErrorCode.InternalServerError);
    }
    if (!(container instanceof Container)) {
      throw new RestServiceException(InternalKeys.TARGET_CONTAINER_KEY + " not instance of Container",
          RestServiceErrorCode.InternalServerError);
    }
    return (Container) container;
  }

  /**
   * Build a {@link ByteRange} given a Range header value. This method can parse the following Range
   * header syntax:
   * {@code Range:bytes=byte_range} where {@code bytes=byte_range} supports the following range syntax:
   * <ul>
   *   <li>For bytes {@code {a}} through {@code {b}} inclusive: {@code bytes={a}-{b}}</li>
   *   <li>For all bytes including and after {@code {a}}: {@code bytes={a}-}</li>
   *   <li>For the last {@code {b}} bytes of a file: {@code bytes=-{b}}</li>
   * </ul>
   * @param rangeHeaderValue the value of the Range header.
   * @return The {@link ByteRange} parsed from the arguments.
   * @throws RestServiceException if no range header was found, or if a valid range could not be parsed from the header
   *                              value,
   */
  private static ByteRange buildByteRange(String rangeHeaderValue) throws RestServiceException {
    if (!rangeHeaderValue.startsWith(BYTE_RANGE_PREFIX)) {
      throw new RestServiceException("Invalid byte range syntax; does not start with '" + BYTE_RANGE_PREFIX + "'",
          RestServiceErrorCode.InvalidArgs);
    }
    ByteRange range;
    try {
      int hyphenIndex = rangeHeaderValue.indexOf('-', BYTE_RANGE_PREFIX.length());
      String startOffsetStr = rangeHeaderValue.substring(BYTE_RANGE_PREFIX.length(), hyphenIndex);
      String endOffsetStr = rangeHeaderValue.substring(hyphenIndex + 1);
      if (startOffsetStr.isEmpty()) {
        range = ByteRange.fromLastNBytes(Long.parseLong(endOffsetStr));
      } else if (endOffsetStr.isEmpty()) {
        range = ByteRange.fromStartOffset(Long.parseLong(startOffsetStr));
      } else {
        range = ByteRange.fromOffsetRange(Long.parseLong(startOffsetStr), Long.parseLong(endOffsetStr));
      }
    } catch (Exception e) {
      throw new RestServiceException(
          "Valid byte range could not be parsed from Range header value: " + rangeHeaderValue,
          RestServiceErrorCode.InvalidArgs);
    }
    return range;
  }
}
