package com.github.ambry.restservice;

/**
 * All the error codes that accompany a RestServiceException
 */
public enum RestServiceErrorCode {
  BadExecutionData,
  BadRequest,
  DuplicateRequest,
  NoRequest,
  UnknownOperationType,
  UnknownRestMethod,
  InternalServerError,
  ChannelActiveTasksFailure,
  HandlerSelectionError,
  HttpObjectConversionFailure,
  MessageHandleFailure,
  MessageQueueingFailure,
  RequestProcessingFailure,
  ResponseBuildingFailure,
  ReponseHandlerMissing,
  RestObjectMissing,
  RestRequestMissing;
}
