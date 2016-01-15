package com.github.ambry.rest;

/**
 * All the REST response statuses.
 */
public enum ResponseStatus {
  // 2xx
  /**
   * 200 OK - Resource found and all good.
   */
  Ok,
  /**
   * 201 - Resource was created.
   */
  Created,
  /**
   * 202 - Request was accepted.
   */
  Accepted,

  // 4xx
  /**
   * 400 - Request was not correct.
   */
  BadRequest,
  /**
   * 404 Not Found - Resource was not found.
   */
  NotFound,
  /**
   * 410 Gone - Resource has been deleted or has expired.
   */
  Gone,

  // 5xx
  /**
   * 500 - Internal server failure resulted in request not being honored.
   */
  InternalServerError;

  /**
   * Gets the ResponseStatus that corresponds to the {@code restServiceErrorCode}.
   * @param restServiceErrorCode the input {@link RestServiceErrorCode}.
   * @return the ResponseStatus that the {@code restServiceErrorCode} belongs to.
   */
  public static ResponseStatus getResponseStatus(RestServiceErrorCode restServiceErrorCode) {
    switch (restServiceErrorCode) {
      case Deleted:
        return ResponseStatus.Gone;
      case NotFound:
        return ResponseStatus.NotFound;
      case BadRequest:
      case InvalidArgs:
      case InvalidRequestState:
      case MalformedRequest:
      case MissingArgs:
      case UnknownHttpObject:
      case UnsupportedHttpMethod:
      case UnsupportedOperation:
        return ResponseStatus.BadRequest;
      case InternalServerError:
      case ChannelCreationTasksFailure:
      case ChannelWriteError:
      case IllegalResponseMetadataStateTransition:
      case InternalObjectCreationError:
      case RequestChannelClosed:
      case RequestResponseQueuingFailure:
      case ResponseBuildingFailure:
      case ServiceUnavailable:
      case UnsupportedRestMethod:
      default:
        return ResponseStatus.InternalServerError;
    }
  }
}
