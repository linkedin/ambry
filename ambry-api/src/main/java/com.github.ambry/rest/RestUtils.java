package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
     * not allowed  in request. Allowed in response only; string; time at which blob was created.
     */
    public final static String CREATION_TIME = "x-ambry-creation-time";
    /**
     * prefix for any header to be set as user metadata for the given blob
     */
    public final static String UserMetaData_Header_Prefix = "x-ambry-um-";

    // prefix for old style user metadata that will be served as headers
    protected final static String UserMetaData_OldStyle_Prefix = "x-ambry-oldstyle-um-";
  }

  public static final class MultipartPost {
    public final static String Blob_Part = "Blob";
  }

  private static final int Crc_Size = 8;
  private static final short UserMetadata_Version_V1 = 1;

  // Max size of a value for user metadata as key value pairs
  protected static final int Max_UserMetadata_Value_Size = 1024 * 8;

  private static Logger logger = LoggerFactory.getLogger(RestUtils.class);

  /**
   * Builds {@link BlobProperties} given a {@link RestRequest}.
   * @param restRequest the {@link RestRequest} to use.
   * @return the {@link BlobProperties} extracted from {@code restRequest}.
   * @throws RestServiceException if required headers aren't present or if they aren't in the format or number
   *                                    expected.
   */
  public static BlobProperties buildBlobProperties(RestRequest restRequest)
      throws RestServiceException {
    Map<String, Object> args = restRequest.getArgs();

    String blobSizeStr = null;
    long blobSize;
    try {
      blobSizeStr = getHeader(args, Headers.BLOB_SIZE, true);
      blobSize = Long.parseLong(blobSizeStr);
      if (blobSize < 0) {
        throw new RestServiceException(Headers.BLOB_SIZE + "[" + blobSize + "] is less than 0",
            RestServiceErrorCode.InvalidArgs);
      }
    } catch (NumberFormatException e) {
      throw new RestServiceException(Headers.BLOB_SIZE + "[" + blobSizeStr + "] could not parsed into a number",
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

    String serviceId = getHeader(args, Headers.SERVICE_ID, true);
    String contentType = getHeader(args, Headers.AMBRY_CONTENT_TYPE, true);
    String ownerId = getHeader(args, Headers.OWNER_ID, false);

    return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, ttl);
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
   * Builds user metadata given a {@link RestRequest}.
   * @param restRequest the {@link RestRequest} to use.
   * @return the user metadata extracted from {@code restRequest}.
   */
  public static byte[] buildUsermetadata(RestRequest restRequest)
      throws RestServiceException {
    Map<String, Object> args = restRequest.getArgs();
    Map<String, String> userMetadataMap = new HashMap<String, String>();
    int sizeToAllocate = 0;
    for (Map.Entry<String, Object> entry : args.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(Headers.UserMetaData_Header_Prefix)) {
        // key size
        sizeToAllocate += 4;
        String keyToStore = key.substring(Headers.UserMetaData_Header_Prefix.length());
        sizeToAllocate += keyToStore.length();
        String value = getHeader(args, key, true);
        userMetadataMap.put(keyToStore, value);
        // value size
        sizeToAllocate += 4;
        sizeToAllocate += value.getBytes().length;
      }
    }
    ByteBuffer userMetadata = null;
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
      sizeToAllocate += Crc_Size;
      userMetadata = ByteBuffer.allocate(sizeToAllocate);
      userMetadata.putShort(UserMetadata_Version_V1);
      // total size = sizeToAllocate - version size - sizeToAllocate size - crc size
      userMetadata.putInt(sizeToAllocate - 6 - Crc_Size);
      userMetadata.putInt(userMetadataMap.size());
      for (Map.Entry<String, String> entry : userMetadataMap.entrySet()) {
        String key = entry.getKey();
        Utils.serializeString(userMetadata, key, StandardCharsets.US_ASCII);
        Utils.serializeString(userMetadata, entry.getValue(), StandardCharsets.US_ASCII);
      }
      Crc32 crc = new Crc32();
      crc.update(userMetadata.array(), 0, sizeToAllocate - Crc_Size);
      userMetadata.putLong(crc.getValue());
    }
    return userMetadata.array();
  }

  /**
   * Fetches user metadata from the byte array
   * @param userMetadata the byte array which has the user metadata
   * @return Map<String,String> the user metadata that is read from the byte array
   */
  public static Map<String, String> buildUserMetadata(byte[] userMetadata)
      throws RestServiceException {
    Map<String, String> toReturn = new HashMap<String, String>();
    boolean oldStyle = false;
    if (userMetadata.length > 0) {
      try {
        ByteBuffer userMetadataBuffer = ByteBuffer.wrap(userMetadata);
        short version = userMetadataBuffer.getShort();
        switch (version) {
          case UserMetadata_Version_V1:
            int sizeToRead = userMetadataBuffer.getInt();
            if (sizeToRead != (userMetadataBuffer.remaining() - 8)) {
              logger.trace("Size didn't match. Returning as old format");
              oldStyle = true;
            } else {
              int entryCount = userMetadataBuffer.getInt();
              int counter = 0;
              while (counter++ < entryCount) {
                String key = Utils.deserializeString(userMetadataBuffer, StandardCharsets.US_ASCII);
                String value = Utils.deserializeString(userMetadataBuffer, StandardCharsets.US_ASCII);
                toReturn.put(Headers.UserMetaData_Header_Prefix + key, value);
              }
              long actualCRC = userMetadataBuffer.getLong();
              Crc32 crc32 = new Crc32();
              crc32.update(userMetadata, 0, userMetadata.length - Crc_Size);
              long expectedCRC = crc32.getValue();
              if (actualCRC != expectedCRC) {
                logger.trace("corrupt data while parsing user metadata Expected CRC " + expectedCRC + " Actual CRC "
                    + actualCRC);
                oldStyle = true;
              }
            }
            break;
          default:
            logger.trace("Failed to parse version in new format. Returning as old format");
            oldStyle = true;
        }
      } catch (RuntimeException e) {
        logger.trace("Runtime Exception on parsing user metadata. Returning as old format");
        oldStyle = true;
      }
    }
    if (oldStyle) {
      toReturn = getOldStyleUserMetadataAsHashMap(userMetadata);
    }
    return toReturn;
  }

  /**
   * Returns a default representation of user metadata that is not in the expected format
   * @param userMetadata byte[] which contains the user metadata in old style
   * @return user metadata in the form of Map<String, String>
   */
  private static Map<String, String> getOldStyleUserMetadataAsHashMap(byte[] userMetadata) {
    int totalSize = userMetadata.length;
    Map<String, String> toReturn = new HashMap<String, String>();
    int sizeRead = 0;
    int counter = 0;
    while (sizeRead < totalSize) {
      String key = Headers.UserMetaData_OldStyle_Prefix + counter++;
      int sizeToRead = Math.min(totalSize - sizeRead, Max_UserMetadata_Value_Size);
      String value = new String(userMetadata, sizeRead, sizeToRead, StandardCharsets.US_ASCII);
      toReturn.put(key, value);
      sizeRead += sizeToRead;
    }
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
  private static String getHeader(Map<String, Object> args, String header, boolean required)
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
}
