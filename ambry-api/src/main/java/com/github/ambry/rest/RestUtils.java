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
import com.github.ambry.router.ByteRanges;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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
     * {@code "allow"}
     */
    public final static String ALLOW = "allow";

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
     * for put or get account request; string; name of target account
     */
    public final static String TARGET_ACCOUNT_NAME = "x-ambry-target-account-name";
    /**
     * for get account request; short; numerical ID of target account
     */
    public final static String TARGET_ACCOUNT_ID = "x-ambry-target-account-id";
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
     * Header that is set in the response of GetBlobInfo.
     * 'true' or 'false'; case insensitive; true indicates content is encrypted at the storage layer. false otherwise
     */
    public final static String ENCRYPTED_IN_STORAGE = "x-ambry-encrypted-in-storage";
    /**
     * optional in request; defines an option while getting the blob and is optional support in a
     * {@link RestRequestService}. Valid values are available in {@link GetOption}. Defaults to {@link GetOption#None}
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
     * An externalAssetTag for this blob.
     */
    public static final String EXTERNAL_ASSET_TAG = "x-ambry-external-asset-tag";
    /**
     * The blob ID requested by the URL.
     */
    public static final String BLOB_ID = "x-ambry-blob-id";
    /**
     * The signed URL header name in the response for signed url requests.
     */
    public static final String SIGNED_URL = "x-ambry-signed-url";
    /**
     * Boolean field set to "true" for getting chunk upload URLs with {@code GET /signedUrl} that will eventually be
     * stitched together.
     */
    public static final String CHUNK_UPLOAD = "x-ambry-chunk-upload";

    /**
     * This header will carry a UUID that represents a "session." For example, when performing a stitched upload, each
     * chunk upload should be a part of the same session.
     */
    public static final String SESSION = "x-ambry-session";

    /**
     * prefix for any header to be set as user metadata for the given blob
     */
    public final static String USER_META_DATA_HEADER_PREFIX = "x-ambry-um-";

    /**
     * Response header indicating the reason a request is non compliant.
     */
    public final static String NON_COMPLIANCE_WARNING = "x-ambry-non-compliance-warning";

  }

  public static final class TrackingHeaders {

    /**
     * Response header for the the name of the datacenter that the frontend responding belongs to.
     */
    public static final String DATACENTER_NAME = "x-ambry-datacenter";
    /**
     * Response header for the hostname of the responding frontend.
     */
    public static final String FRONTEND_NAME = "x-ambry-frontend";
    /**
     * A list of all tracking headers.
     */
    public static final List<String> TRACKING_HEADERS;

    static {
      Field[] fields = RestUtils.TrackingHeaders.class.getDeclaredFields();
      TRACKING_HEADERS = new ArrayList<>(fields.length);
      try {
        for (Field field : fields) {
          if (field.getType() == String.class) {
            TRACKING_HEADERS.add(field.get(null).toString());
          }
        }
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Could not get values of the tracking headers", e);
      }
    }
  }

  /**
   * Ambry specific keys used internally in a {@link RestRequest}.
   */
  public static final class InternalKeys {
    private static final String KEY_PREFIX = "ambry-internal-key-";

    /**
     * The key for the target {@link com.github.ambry.account.Account} indicated by the request.
     */
    public static final String TARGET_ACCOUNT_KEY = KEY_PREFIX + "target-account";

    /**
     * The key for the target {@link com.github.ambry.account.Container} indicated by the request.
     */
    public static final String TARGET_CONTAINER_KEY = KEY_PREFIX + "target-container";

    /**
     * The key for the metadata {@code Map<String, String>} to include in a signed ID. This argument should be non-null
     * to indicate that a signed ID should be created and returned to the requester on a POST request.
     */
    public static final String SIGNED_ID_METADATA_KEY = KEY_PREFIX + "signed-id-metadata";

    /**
     * To be set if the operation knows the keep-alive behavior it prefers on error. Valid values are boolean.
     * Not authoritative, only a hint
     */
    public static final String KEEP_ALIVE_ON_ERROR_HINT = KEY_PREFIX + "keep-alive-on-error-hint";

    /**
     * To be set to {@code true} if tracking info should be attached to frontend responses.
     */
    public static final String SEND_TRACKING_INFO = KEY_PREFIX + "send-tracking-info";

    /**
     * Set to {@code true} (assumed {@code false} if absent) if the user metadata needs to be sent as the body of the
     * response.
     */
    public static final String SEND_USER_METADATA_AS_RESPONSE_BODY = KEY_PREFIX + "send-user-metadata-as-response-body";

    /**
     * The key for the {@link RequestPath} that represents the parsed path of an incoming request.
     */
    public static final String REQUEST_PATH = KEY_PREFIX + "request-path";
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
    Replicas,

    /**
     * Used when fetching individual segments/data blobs from a composite blob.
     * Will come in the resource URI in the form "*\/Segment\/<NON-NEGATIVE_INTEGER>",
     * where NON-NEGATIVE_INTEGER is a non-negative integer that represents the index
     * of a segment one wants to GET
     */
    Segment
  }

  public static final class MultipartPost {
    public final static String BLOB_PART = "Blob";
    public final static String USER_METADATA_PART = "UserMetadata";
  }

  public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String BYTE_RANGE_UNITS = "bytes";
  public static final String SIGNED_ID_PREFIX = "signedId/";
  public static final String JSON_CONTENT_TYPE = "application/json";
  private static final String BYTE_RANGE_PREFIX = BYTE_RANGE_UNITS + "=";

  private static final int CRC_SIZE = 8;
  private static final short USER_METADATA_VERSION_V1 = 1;

  private static final Logger logger = LoggerFactory.getLogger(RestUtils.class);

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
    String externalAssetTag = getHeader(args, Headers.EXTERNAL_ASSET_TAG, false);

    long ttl = Utils.Infinite_Time;
    Long ttlFromHeader = getLongHeader(args, Headers.TTL, false);
    if (ttlFromHeader != null) {
      if (ttlFromHeader < -1) {
        throw new RestServiceException(Headers.TTL + "[" + ttlFromHeader + "] is not valid (has to be >= -1)",
            RestServiceErrorCode.InvalidArgs);
      }
      ttl = ttlFromHeader;
    }

    // This field should not matter on newly created blobs, because all privacy/cacheability decisions should be made
    // based on the container properties and ACLs. For now, BlobProperties still includes this field, though.
    boolean isPrivate = !container.isCacheable();
    return new BlobProperties(-1, serviceId, ownerId, contentType, isPrivate, ttl, account.getId(), container.getId(),
        container.isEncrypted(), externalAssetTag);
  }

  /**
   * Builds user metadata given the arguments associated with a request.
   * <p>
   * The following binary format will be used:
   * <pre>
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
   * </pre>
   *
   * @param args the arguments associated with the request.
   * @return the user metadata extracted from arguments.
   * @throws RestServiceException if usermetadata arguments have null values.
   */
  public static byte[] buildUserMetadata(Map<String, Object> args) throws RestServiceException {
    ByteBuffer userMetadata;
    if (args.containsKey(MultipartPost.USER_METADATA_PART)) {
      userMetadata = (ByteBuffer) args.get(MultipartPost.USER_METADATA_PART);
    } else {
      Map<String, String> userMetadataMap = new HashMap<String, String>();
      int sizeToAllocate = 0;
      for (Map.Entry<String, Object> entry : args.entrySet()) {
        String key = entry.getKey();
        if (key.toLowerCase().startsWith(Headers.USER_META_DATA_HEADER_PREFIX)) {
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
  public static Map<String, String> buildUserMetadata(byte[] userMetadata) {
    if (userMetadata.length == 0) {
      return Collections.emptyMap();
    }
    Map<String, String> toReturn = null;
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
              logger.error(
                  "corrupt data while parsing user metadata Expected CRC " + expectedCRC + " Actual CRC " + actualCRC);
              toReturn = null;
            }
          }
          break;
        default:
          toReturn = null;
          logger.trace("Failed to parse version in new format. Returning null");
      }
    } catch (RuntimeException e) {
      logger.trace("Runtime Exception on parsing user metadata. Returning null");
      toReturn = null;
    }
    return toReturn;
  }

  /**
   * Build a {@link GetBlobOptions} object from an argument map for a certain sub-resource.
   * @param args the arguments associated with the request. This is typically a map of header names and query string
   *             arguments to values.
   * @param subResource the {@link SubResource} for the request, or {@code null} if no sub-resource is requested.
   * @param getOption the {@link GetOption} required.
   * @param blobSegmentIdx index of blob segment one wishes to GET, {@link GetBlobOptions#NO_BLOB_SEGMENT_IDX_SPECIFIED}
   *                       if not used
   * @return a populated {@link GetBlobOptions} object.
   * @throws RestServiceException if the {@link GetBlobOptions} could not be constructed.
   */
  public static GetBlobOptions buildGetBlobOptions(Map<String, Object> args, SubResource subResource,
      GetOption getOption, int blobSegmentIdx) throws RestServiceException {
    String rangeHeaderValue = getHeader(args, Headers.RANGE, false);
    if (subResource != null && !subResource.equals(SubResource.Segment) && rangeHeaderValue != null) {
      throw new RestServiceException("Ranges not supported for sub-resources that aren't Segment.",
          RestServiceErrorCode.InvalidArgs);
    }
    return new GetBlobOptionsBuilder().operationType(
        subResource == null || subResource == SubResource.Segment ? GetBlobOptions.OperationType.All
            : GetBlobOptions.OperationType.BlobInfo)
        .getOption(getOption)
        .blobSegment(blobSegmentIdx)
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
   * Extract the {@link RequestPath} object from {@link RestRequest} arguments.
   * @param restRequest the {@link RestRequest}.
   * @return the {@link RequestPath}
   */
  public static RequestPath getRequestPath(RestRequest restRequest) {
    return (RequestPath) Objects.requireNonNull(restRequest.getArgs().get(InternalKeys.REQUEST_PATH),
        InternalKeys.REQUEST_PATH + " not set in " + restRequest);
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
   * @param defaultGetOption the {@link GetOption} to use if the {@code restRequest} doesn't have one. Can be
   * {@code null}.
   * @return the required {@link GetOption}. Defaults to {@link GetOption#None}.
   * @throws RestServiceException if the {@link RestUtils.Headers#GET_OPTION} is present but not recognized.
   */
  public static GetOption getGetOption(RestRequest restRequest, GetOption defaultGetOption)
      throws RestServiceException {
    GetOption option = defaultGetOption == null ? GetOption.None : defaultGetOption;
    Object value = restRequest.getArgs().get(RestUtils.Headers.GET_OPTION);
    if (value != null) {
      String str = (String) value;
      boolean foundMatch = false;
      for (GetOption getOption : GetOption.values()) {
        if (str.equalsIgnoreCase(getOption.name())) {
          option = getOption;
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        throw new RestServiceException("Unrecognized value for [" + RestUtils.Headers.GET_OPTION + "]: " + str,
            RestServiceErrorCode.InvalidArgs);
      }
    }
    return option;
  }

  /**
   * Gets the isPrivate setting from the args.
   * @param args The args where to include the isPrivate setting.
   * @return A boolean to indicate the value of the isPrivate flag.
   * @throws RestServiceException if exception occurs during parsing the arg.
   */
  public static boolean isPrivate(Map<String, Object> args) throws RestServiceException {
    return getBooleanHeader(args, Headers.PRIVATE, false);
  }

  /**
   * Determine if {@link Headers#CHUNK_UPLOAD} is set in the request args.
   * @param args The request arguments.
   * @return {@code true} if {@link Headers#CHUNK_UPLOAD} is set.
   * @throws RestServiceException if exception occurs during parsing the arg.
   */
  public static boolean isChunkUpload(Map<String, Object> args) throws RestServiceException {
    return getBooleanHeader(args, Headers.CHUNK_UPLOAD, false);
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
  public static String getHeader(Map<String, ?> args, String header, boolean required) throws RestServiceException {
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
   * @param required if {@code true}, {@link RestServiceException} will be thrown if {@code header} is not present
   *                 in {@code args}.
   * @return the value of {@code header} in {@code args} if it exists. If it does not exist and {@code required} is
   *          {@code false}, then returns null.
   * @throws RestServiceException same as cases of {@link #getHeader(Map, String, boolean)} and if the value cannot be
   *                              converted to a {@link Long}.
   */
  public static Long getLongHeader(Map<String, ?> args, String header, boolean required) throws RestServiceException {
    return getNumericalHeader(args, header, required, Long::parseLong);
  }

  /**
   * Gets the value of a header as a {@link Number}, using the provided converter function to parse the string.
   * @param args a map of arguments to be used to look for {@code header}.
   * @param header the name of the header.
   * @param required if {@code true}, {@link RestServiceException} will be thrown if {@code header} is not present
   *                 in {@code args}.
   * @param converter a function to convert from a {@link String} to the desired numerical type. This should throw
   *                  {@link NumberFormatException} if the argument is not a valid number.
   * @return the value of {@code header} in {@code args} if it exists. If it does not exist and {@code required} is
   *          {@code false}, then returns null.
   * @throws RestServiceException same as cases of {@link #getHeader(Map, String, boolean)} and if the value cannot be
   *                              converted to a {@link Long}.
   */
  public static <T extends Number> T getNumericalHeader(Map<String, ?> args, String header, boolean required,
      Function<String, T> converter) throws RestServiceException {
    // if getHeader() is no longer called, tests for this function have to be changed.
    String value = getHeader(args, header, required);
    if (value != null) {
      try {
        return converter.apply(value);
      } catch (NumberFormatException e) {
        throw new RestServiceException("Invalid value for " + header + ": " + value, e,
            RestServiceErrorCode.InvalidArgs);
      }
    }
    return null;
  }

  /**
   * Gets the value of a header as a {@code boolean}.
   * @param args a map of arguments to be used to look for {@code header}.
   * @param header the name of the header.
   * @param required if {@code true}, {@link RestServiceException} will be thrown if {@code header} is not present
   *                 in {@code args}.
   * @return {@code true} if the header's value is {@code "true"} (case-insensitive), or {@code false} if the header's
   *         value is {@code "false} (case-insensitive) or the header is not present and {@code required} is
   *         {@code false}.
   * @throws RestServiceException same as cases of {@link #getHeader(Map, String, boolean)} and if the value cannot be
   *                              converted to a {@code boolean}.
   */
  public static boolean getBooleanHeader(Map<String, Object> args, String header, boolean required)
      throws RestServiceException {
    boolean booleanValue;
    String stringValue = getHeader(args, header, required);
    if (stringValue == null || "false".equalsIgnoreCase(stringValue)) {
      booleanValue = false;
    } else if ("true".equalsIgnoreCase(stringValue)) {
      booleanValue = true;
    } else {
      throw new RestServiceException(
          header + "[" + stringValue + "] has an invalid value (allowed values: true, false)",
          RestServiceErrorCode.InvalidArgs);
    }
    return booleanValue;
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
   * Check preconditions for request if the {@code restRequest} contains the target account and container.
   * @param restRequest the {@link RestRequest} that contains the {@link Account} and {@link Container} details.
   * @throws RestServiceException if preconditions check failed.
   */
  public static void accountAndContainerNamePreconditionCheck(RestRequest restRequest) throws RestServiceException {
    String accountNameFromHeader = getHeader(restRequest.getArgs(), Headers.TARGET_ACCOUNT_NAME, false);
    String containerNameFromHeader = getHeader(restRequest.getArgs(), Headers.TARGET_CONTAINER_NAME, false);
    if (accountNameFromHeader != null) {
      Account targetAccount = getAccountFromArgs(restRequest.getArgs());
      String accountNameFromBlobId = targetAccount.getName();
      if (!accountNameFromHeader.equals(accountNameFromBlobId)) {
        throw new RestServiceException(
            "Account name: " + accountNameFromHeader + " from request doesn't match the account name from Blob id : "
                + accountNameFromBlobId, RestServiceErrorCode.PreconditionFailed);
      }
      if (containerNameFromHeader != null) {
        Container targetContainer = getContainerFromArgs(restRequest.getArgs());
        String containerNameFromBlobId = targetContainer.getName();
        if (!containerNameFromHeader.equals(containerNameFromBlobId)) {
          throw new RestServiceException("Container name: " + containerNameFromHeader
              + "from request doesn't match the container name from Blob id : " + containerNameFromBlobId,
              RestServiceErrorCode.PreconditionFailed);
        }
      }
    } else if (containerNameFromHeader != null) {
      throw new RestServiceException(
          "Only container name is set in request with no corresponding account name is not allowed.",
          RestServiceErrorCode.BadRequest);
    }
  }

  /**
   * Sets the user metadata in the headers of the response.
   * @param userMetadata the user metadata that needs to be sent.
   * @param restResponseChannel the {@link RestResponseChannel} that is used for sending the response.
   * @return {@code true} if the user metadata was successfully deserialized into headers, {@code false} if not.
   * @throws RestServiceException if there are any problems setting the header.
   */
  public static boolean setUserMetadataHeaders(byte[] userMetadata, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    Map<String, String> userMetadataMap = buildUserMetadata(userMetadata);
    boolean setHeaders = userMetadataMap != null;
    if (setHeaders) {
      for (Map.Entry<String, String> entry : userMetadataMap.entrySet()) {
        restResponseChannel.setHeader(entry.getKey(), entry.getValue());
      }
    }
    return setHeaders;
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
        range = ByteRanges.fromLastNBytes(Long.parseLong(endOffsetStr));
      } else if (endOffsetStr.isEmpty()) {
        range = ByteRanges.fromStartOffset(Long.parseLong(startOffsetStr));
      } else {
        range = ByteRanges.fromOffsetRange(Long.parseLong(startOffsetStr), Long.parseLong(endOffsetStr));
      }
    } catch (Exception e) {
      throw new RestServiceException(
          "Valid byte range could not be parsed from Range header value: " + rangeHeaderValue,
          RestServiceErrorCode.InvalidArgs);
    }
    return range;
  }
}
