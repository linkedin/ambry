package com.github.ambry.rest;

import com.github.ambry.storageservice.BlobStorageServiceErrorCode;


/**
 * All the error codes that accompany a RestException
 */
public enum RestErrorCode {
  BadRequest,
  DuplicateRequest,
  NoRequest,
  UnknownOperationType,
  UnknownHttpMethod,
  InternalServerError,
  ChannelActiveTasksFailure,
  HandlerSelectionError,
  HttpObjectConversionFailure,
  MessageHandleFailure,
  RequestProcessingFailure,
  ResponseBuildingFailure,
  ReponseHandlerMissing,
  RestObjectMissing,
  RestRequestMissing;

  /**
   * Provides a RestErrorCode given BlobStorageServiceErrorCode
   *
   * @param code
   * @return the RestErrorCode that this BlobStorageServiceErrorCode maps to
   */
  public static RestErrorCode getRestErrorCode(BlobStorageServiceErrorCode code) {
    switch (code) {
      case BadRequest:
      case UnknownOperationType:
        return BadRequest;
      case InternalError:
      default:
        return InternalServerError;
    }
  }
}
