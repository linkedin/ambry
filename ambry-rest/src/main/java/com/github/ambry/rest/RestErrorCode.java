package com.github.ambry.rest;

/**
 * All the error codes that accompany a RestException
 */
public enum RestErrorCode {
  BadRequest,
  DuplicateRequest,
  MalformedRequest,
  NoRequest,
  UnknownAction,
  UnknownHttpMethod,
  InternalServerError,
  ChannelActiveTasksFailure,
  HandlerSelectionError,
  HttpObjectConversionFailure,
  NoMessageHandlers,
  RequestHandleFailure,
  ReponseHandlerMissing,
  RestObjectMissing,
  RestRequestMissing,
  UnknownHttpObject,
  UnknownObject;

  /**
   * Provides a http equivalent error group a given code belongs to
   * @param code
   * @return the http equivalent error group this code belongs to
   */
  public static RestErrorCode getErrorGroup(RestErrorCode code) {
    switch (code) {
      case DuplicateRequest:
      case MalformedRequest:
      case NoRequest:
      case UnknownAction:
      case UnknownHttpMethod:
        return BadRequest;
      case ChannelActiveTasksFailure:
      case HandlerSelectionError:
      case HttpObjectConversionFailure:
      case NoMessageHandlers:
      case RequestHandleFailure:
      case ReponseHandlerMissing:
      case RestObjectMissing:
      case RestRequestMissing:
      case UnknownHttpObject:
      case UnknownObject:
      default:
        return InternalServerError;
    }
  }
}
