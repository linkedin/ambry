/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

import com.github.ambry.frontend.IdConverter;
import com.github.ambry.router.RouterErrorCode;


/**
 * All the error codes that accompany a {@link RestServiceException}. Each of these error codes are expected to go
 * into certain "groups" that map to HTTP error codes.
 * <p/>
 * The groups are:
 * {@link ResponseStatus#Gone}
 * {@link ResponseStatus#NotFound}
 * {@link ResponseStatus#BadRequest}
 * {@link ResponseStatus#InternalServerError}
 * {@link ResponseStatus#Forbidden}
 * {@link ResponseStatus#ProxyAuthenticationRequired}
 * {@link ResponseStatus#Unauthorized}
 * <p/>
 * About logging:
 * Generally, error codes not belonging to the group {@link #InternalServerError} are logged at DEBUG level.
 */
public enum RestServiceErrorCode {
  /**
   * Resource has been deleted or has expired.
   */
  Deleted,

  /**
   * Resource was not found.
   */
  NotFound,

  /**
   * Resource scan still in progress and result not yet available
   */
  ResourceScanInProgress,

  /**
   * Resource scan has deducted that the resource is not safe for serving
   */
  ResourceDirty,

  /**
   * An authenticated client is not authorized to access a resource.
   */
  AccessDenied,

  /**
   * Client has sent a request that cannot be processed due to authorization failure.
   */
  Unauthorized,

  /**
   * Generic BadRequest error code when a client provides a request that is not fit for processing.
   */
  BadRequest,

  /**
   * Client has sent arguments (whether in the URI or in the headers) that are not in the format that is expected or if
   * the number of values for an argument expected by the server does not match what the client sent.
   */
  InvalidArgs,

  /**
   * Client has sent request content without sending request metadata first or has sent content when no content
   * was expected (for e.g. content with {@link RestMethod#GET}).
   */
  InvalidRequestState,

  /**
   * The account is not valid.
   */
  InvalidAccount,

  /**
   * The container is not valid.
   */
  InvalidContainer,

  /**
   * Client has sent a request that cannot be decoded using the REST protocol (usually HTTP).
   */
  MalformedRequest,

  /**
   * Client has sent a request that is missing some arguments (whether in the URI or in the headers) necessary to
   * service the request.
   */
  MissingArgs,

  /**
   * Client is requesting a HTTP method that is not supported.
   */
  UnsupportedHttpMethod,

  /**
   * Range request is not satisfiable (because the provided range is invalid or outside of the bounds of an object.)
   */
  RangeNotSatisfiable,

  /**
   * Generic InternalServerError that is a result of problems on the server side that is not caused by the client and
   * there is nothing that a client can do about it.
   */
  InternalServerError,

  /**
   * Indicates that {@link IdConverter} encountered some exception during ID conversion
   */
  IdConverterServiceError,

  /**
   * Indicates that a {@link RestRequest} has been closed and an operation could not be performed on it.
   */
  RequestChannelClosed,

  /**
   * Indicates that the submitted request or response could not be queued in the AsyncRequestResponseHandler.
   */
  RequestResponseQueuingFailure,

  /**
   * The request is larger than what the server is willing or is configured to accept.
   */
  RequestTooLarge,

  /**
   * Indicates that the service is unavailable because one or more of the components is not started, is shutdown, has
   * crashed or is temporarily unable to respond.
   */
  ServiceUnavailable,

  /**
   * Application rate limit exceeded/Application quota limit exceeded.
   */
  TooManyRequests,

  /**
   * Indicates a {@link RestMethod} is not supported (May also indicate a bug where behaviour for a new
   * {@link RestMethod} has not been defined in the implementation).
   */
  UnsupportedRestMethod,

  /**
   * There is insufficient capacity to service the request.
   */
  InsufficientCapacity,

  /**
   * The conditions given in the request header fields evaluated to false.
   */
  PreconditionFailed,

  /**
   * Action not allowed
   */
  NotAllowed;

  /**
   * Gets the RestServiceErrorCode that corresponds to the {@code routerErrorCode}.
   * @param routerErrorCode input {@link RouterErrorCode}.
   * @return the RestServiceErrorCode that the {@code routerErrorCode} belongs to.
   */
  public static RestServiceErrorCode getRestServiceErrorCode(RouterErrorCode routerErrorCode) {
    switch (routerErrorCode) {
      case BlobTooLarge:
      case InvalidBlobId:
      case InvalidPutArgument:
      case BadInputChannel:
        return BadRequest;
      case BlobDeleted:
      case BlobExpired:
        return Deleted;
      case BlobAuthorizationFailure:
        return AccessDenied;
      case BlobDoesNotExist:
        return NotFound;
      case BlobUpdateNotAllowed:
        return NotAllowed;
      case RangeNotSatisfiable:
        return RangeNotSatisfiable;
      case OperationTimedOut:
      case AmbryUnavailable:
      case RouterClosed:
        return ServiceUnavailable;
      case InsufficientCapacity:
        return InsufficientCapacity;
      case UnexpectedInternalError:
      case ChannelClosed:
      default:
        return InternalServerError;
    }
  }
}
