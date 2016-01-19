package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
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

  public static final int Crc_Size = 8;
  public static final short UserMetadata_Version_V1 = 1;
  // Max size of a value for user metadata as key value pairs
  public static final int Max_UserMetadata_Value_Size = 1024 * 1024 * 8;

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
   *  key2           - Content of key2
   *
   *  value2 size    - Size of 2nd value
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
    Map<String, List<String>> args = restRequest.getArgs();
    Map<String, String> userMetadataMap = new HashMap<String, String>();
    int sizeToAllocate = 0;
    for (Map.Entry<String, List<String>> entry : args.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(Headers.UserMetaData_Header_Prefix)) {
        // key size
        sizeToAllocate += 4;
        sizeToAllocate += key.getBytes().length;
        String value = getHeader(args, key, true);
        userMetadataMap.put(key, value);
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
      userMetadata.putInt(sizeToAllocate - 6 - Crc_Size);
      userMetadata.putInt(userMetadataMap.size());
      for (Map.Entry<String, String> entry : userMetadataMap.entrySet()) {
        String key = entry.getKey();
        Utils.serializeAsciiEncodedString(userMetadata, key);
        Utils.serializeAsciiEncodedString(userMetadata, entry.getValue());
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
    try {
      if (userMetadata.length > 0) {
        try {
          CrcInputStream crcstream = new CrcInputStream(new ByteArrayInputStream(userMetadata));
          DataInputStream streamData = new DataInputStream(crcstream);
          short version = streamData.readShort();
          switch (version) {
            case UserMetadata_Version_V1:
              ByteBuffer userMetadataBuffer = deserializeUserMetadata(crcstream);
              if (userMetadataBuffer.hasRemaining()) {
                int size = userMetadataBuffer.getInt();
                int counter = 0;
                while (counter++ < size) {
                  String key = Utils.deserializeAsciiEncodedString(userMetadataBuffer);
                  String value = Utils.deserializeAsciiEncodedString(userMetadataBuffer);
                  toReturn.put(key, value);
                }
              }
              break;
            default:
              logger.trace("Failed to parse version in new format. Returning as old format");
              oldStyle = true;
          }
        } catch (IOException e) {
          logger.trace("IOException on parsing user metadata. Returning as old format");
          oldStyle = true;
        } catch (RuntimeException e) {
          logger.trace("Runtime Exception on parsing user metadata. Returning as old format");
          oldStyle = true;
        }
      }
      if (oldStyle) {
        toReturn = getOldStyleUserMetadataAsHashMap(userMetadata);
      }
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
    return toReturn;
  }

  /**
   * Deserialize User Metadata to a {@link ByteBuffer}
   * @param crcStream Stream from which User metadata has to be deserialized
   * @return ByteBuffer which contains the user metadata
   * @throws IOException
   */
  private static ByteBuffer deserializeUserMetadata(CrcInputStream crcStream)
      throws IOException {
    DataInputStream dataStream = new DataInputStream(crcStream);
    int usermetadataSize = dataStream.readInt();
    byte[] userMetadaBuffer = Utils.readBytesFromStream(dataStream, usermetadataSize);
    long actualCRC = crcStream.getValue();
    long expectedCRC = dataStream.readLong();
    if (actualCRC != expectedCRC) {
      logger.error("corrupt data while parsing user metadata Expected CRC " + expectedCRC + " Actual CRC " + actualCRC);
      throw new IllegalStateException("User metadata is corrupt");
    }
    return ByteBuffer.wrap(userMetadaBuffer);
  }

  /**
   * Returns old style user metadata as a HashMap<String, String> to be sent in as headers
   * @param userMetadata byte[] which contains the user metadata in old style
   * @return Map<String, String> user metadata in the form of Map<String, String>
   */
  private static Map<String, String> getOldStyleUserMetadataAsHashMap(byte[] userMetadata)
      throws UnsupportedEncodingException {
    int totalSize = userMetadata.length;
    Map<String, String> toReturn = new HashMap<String, String>();
    int sizeRead = 0;
    int counter = 0;
    ByteBuffer userMetadataBuffer = ByteBuffer.wrap(userMetadata);
    while (sizeRead < totalSize) {
      String key = Headers.UserMetaData_OldStyle_Prefix + counter++;
      int sizeToRead = Math.min(totalSize - sizeRead, Max_UserMetadata_Value_Size);
      byte[] userMetadataPart = new byte[sizeToRead];
      userMetadataBuffer.get(userMetadataPart);
      String value = new String(userMetadataPart, "US-ASCII");
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
}
