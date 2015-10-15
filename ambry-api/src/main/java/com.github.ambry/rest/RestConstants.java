package com.github.ambry.rest;

/**
 * HTTP REST Ambry API constants.
 */
public final class RestConstants {
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
     * optional in request; string; default unset; content type of blob
     */
    public final static String Content_Type = "x-ambry-content-type";
    /**
     * optional in request; string; default unset; member id.
     * <p/>
     * Expected usage is to set to member id of content owner.
     */
    public final static String Owner_Id = "x-ambry-owner-id";
    /**
     * not allowed  in request / response only; string; time at which blob was created.
     */
    public final static String Creation_Time = "x-ambry-creation-time";
    /**
     * Header to indicate if the client request is internal to LinkedIn
     */
    public final static String AMBRY_CLIENT_HEADER = "x-li-ambry-client";
    /**
     * Value of x-li-ambry-client header for internal clients.
     */
    public final static String AMBRY_CLIENT_HEADER_INTERNAL = "Internal";
  }
}

