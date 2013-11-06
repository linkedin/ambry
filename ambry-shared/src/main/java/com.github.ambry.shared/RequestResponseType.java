package com.github.ambry.shared;

/**
 * Type of request response
 */
public enum RequestResponseType {
  PutRequest,
  GetRequest,
  PutReponse,
  GetResponse,
  DeleteRequest,
  DeleteResponse,
  TTLRequest,
  TTLResponse
}