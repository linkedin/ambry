package com.github.ambry.shared;

/**
 * Type of request response. Do not change this order. Add
 * new entries to the end of the list.
 */
public enum RequestResponseType {
  PutRequest,
  PutResponse,
  GetRequest,
  GetResponse,
  DeleteRequest,
  DeleteResponse,
  TTLRequest,
  TTLResponse
}