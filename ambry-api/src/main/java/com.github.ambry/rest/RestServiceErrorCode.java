/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
   * Generic BadRequest error code when a client provides a request that is not fit for processing.
   */
  BadRequest,
  /**
   * Resource scan still in progress and result not yet available
   */
  ResourceScanInProgress,
  /**
   * Resource scan has deducted that the resource is not safe for serving
   */
  ResourceDirty,
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
   * Client has sent a request that cannot be decoded using the REST protocol (usually HTTP).
   */
  MalformedRequest,
  /**
   * Client has sent a request that is missing some arguments (whether in the URI or in the headers) necessary to
   * service the request.
   */
  MissingArgs,
  /**
   * Client has sent a request that cannot be processed due to authorization failure.
   */
  Unauthorized,
  /**
   * Indicates that HttpObject received was not of a recognized type (Currently this is internal to Netty and this
   * error indicates that the received HttpObject was neither HttpRequest nor HttpContent).
   */
  UnknownHttpObject,
  /**
   * Client is requesting a HTTP method that is not supported.
   */
  UnsupportedHttpMethod,
  /**
   * Client has requested for an operation that is not supported by the {@link BlobStorageService}.
   */
  UnsupportedOperation,

  /**
   * Generic InternalServerError that is a result of problems on the server side that is not caused by the client and
   * there is nothing that a client can do about it.
   */
  InternalServerError,
  /**
   * Indicates that an object that is needed for the request could not be created due to an internal server error.
   */
  InternalObjectCreationError,
  /**
   * Indicates that a {@link RestRequest} has been closed and an operation could not be performed on it.
   */
  RequestChannelClosed,
  /**
   * Indicates that the submitted request or response could not be queued in the AsyncRequestResponseHandler.
   */
  RequestResponseQueuingFailure,
  /**
   * Indicates that there was a problem building the response.
   */
  ResponseBuildingFailure,
  /**
   * Indicates that an internal service is unavailable either because it is not started, is shutdown or has crashed.
   */
  ServiceUnavailable,
  /**
   * Indicates a {@link RestMethod} is not supported (May also indicate a bug where behaviour for a new
   * {@link RestMethod} has not been defined in the implementation).
   */
  UnsupportedRestMethod;

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
      case BlobDoesNotExist:
        return NotFound;
      case AmbryUnavailable:
      case InsufficientCapacity:
      case OperationTimedOut:
      case RouterClosed:
      case UnexpectedInternalError:
      default:
        return InternalServerError;
    }
  }
}
