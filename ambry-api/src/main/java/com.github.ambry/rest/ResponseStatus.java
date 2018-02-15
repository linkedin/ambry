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

  /**
   * 206 - Partial content.
   */
  PartialContent,
  // 3xx
  /**
   * 304 Not Modified
   */
  NotModified,

  // 4xx
  /**
   * 400 - Request was not correct.
   */
  BadRequest,

  /**
   * 401 - Request Unauthorized
   */
  Unauthorized,

  /**
   * 403 - Request forbidden
   */
  Forbidden,

  /**
   * 404 Not Found - Resource was not found.
   */
  NotFound,

  /**
   * 407 - Proxy authentication required
   */
  ProxyAuthenticationRequired,

  /**
   * 410 Gone - Resource has been deleted or has expired.
   */
  Gone,

  /**
   * 413 Request Entity Too Large - The request is larger than what the server is willing to accept
   */
  RequestTooLarge,

  /**
   * 416 Range Not Satisfiable - A range request is invalid or outside of the bounds of an object.
   */
  RangeNotSatisfiable,

  // 5xx
  /**
   * 500 - Internal server failure resulted in request not being honored.
   */
  InternalServerError,

  /**
   * 503 - Service is unavailable
   */
  ServiceUnavailable,

  /**
   * 507 - Insufficient capacity to complete the request.
   */
  InsufficientCapacity;

  /**
   * Gets the ResponseStatus that corresponds to the {@code restServiceErrorCode}.
   * @param restServiceErrorCode the input {@link RestServiceErrorCode}.
   * @return the ResponseStatus that the {@code restServiceErrorCode} belongs to.
   */
  public static ResponseStatus getResponseStatus(RestServiceErrorCode restServiceErrorCode) {
    switch (restServiceErrorCode) {
      case RequestTooLarge:
        return RequestTooLarge;
      case Deleted:
        return Gone;
      case NotFound:
        return NotFound;
      case BadRequest:
      case InvalidArgs:
      case InvalidAccount:
      case InvalidContainer:
      case InvalidRequestState:
      case MalformedRequest:
      case MissingArgs:
      case UnsupportedHttpMethod:
        return BadRequest;
      case ResourceDirty:
      case AccessDenied:
        return Forbidden;
      case Unauthorized:
        return Unauthorized;
      case ResourceScanInProgress:
        return ProxyAuthenticationRequired;
      case RangeNotSatisfiable:
        return RangeNotSatisfiable;
      case ServiceUnavailable:
        return ServiceUnavailable;
      case InsufficientCapacity:
        return InsufficientCapacity;
      case IdConverterServiceError:
      case InternalServerError:
      case RequestChannelClosed:
      case RequestResponseQueuingFailure:
      case UnsupportedRestMethod:
      default:
        return InternalServerError;
    }
  }
}
