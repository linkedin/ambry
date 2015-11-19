package com.github.ambry.protocol;

/**
 * Type of request response. Do not change this order. Add
 * new entries to the end of the list.
 */
public enum RequestOrResponseType {
  PutRequest,
  PutResponse,
  GetRequest,
  GetResponse,
  DeleteRequest,
  DeleteResponse,
  TTLRequest, // Unsupported
  TTLResponse, // Unsupported
  ReplicaMetadataRequest,
  ReplicaMetadataResponse
}