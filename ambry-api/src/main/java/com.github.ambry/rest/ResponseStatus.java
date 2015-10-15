package com.github.ambry.rest;

/**
 * All the REST response statuses.
 */
public enum ResponseStatus {
  // 2xx
  /**
   * 200 OK - resource found and all good.
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
   * 404 Not Found - resource was not found.
   */
  NotFound,
  /**
   * 410 Gone - resource has been deleted.
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
      case MalformedRequest:
      case MissingArgs:
      case NoRequest:
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
      case RequestResponseHandlerSelectionError:
      case RequestResponseQueueingFailure:
      case ResponseBuildingFailure:
      case ServiceUnavailable:
      case UnsupportedRestMethod:
      default:
        return ResponseStatus.InternalServerError;
    }
  }
}
