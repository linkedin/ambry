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
  Ok(200),

  /**
   * 201 - Resource was created.
   */
  Created(201),

  /**
   * 202 - Request was accepted.
   */
  Accepted(202),

  /**
   * 206 - Partial content.
   */
  PartialContent(206),
  // 3xx
  /**
   * 304 Not Modified
   */
  NotModified(304),

  // 4xx
  /**
   * 400 - Request was not correct.
   */
  BadRequest(400),

  /**
   * 401 - Request Unauthorized
   */
  Unauthorized(401),

  /**
   * 403 - Request forbidden
   */
  Forbidden(403),

  /**
   * 404 Not Found - Resource was not found.
   */
  NotFound(404),

  /**
   * 405 Method Not Allowed - Method in request is not allowed on the resource
   */
  MethodNotAllowed(405),

  /**
   * 407 - Proxy authentication required
   */
  ProxyAuthenticationRequired(407),

  /**
   * 410 Gone - Resource has been deleted or has expired.
   */
  Gone(410),

  /**
   * 412 Precondition Failed - The conditions given in the request header fields evaluated to false
   */
  PreconditionFailed(412),

  /**
   * 413 Request Entity Too Large - The request is larger than what the server is willing to accept
   */
  RequestTooLarge(413),

  /**
   * 416 Range Not Satisfiable - A range request is invalid or outside of the bounds of an object.
   */
  RangeNotSatisfiable(416),

  /**
   * 429 Application rate limit exceeded/Application quota limit exceeded.
   */
  TooManyRequests(429),

  // 5xx
  /**
   * 500 - Internal server failure resulted in request not being honored.
   */
  InternalServerError(500),

  /**
   * 503 - Service is unavailable
   */
  ServiceUnavailable(503),

  /**
   * 507 - Insufficient capacity to complete the request.
   */
  InsufficientCapacity(507);

  private final int statusCode;

  /**
   * ResponseStatus ctor.
   * @param statusCode the status code associated with this ResponseStatus
   */
  ResponseStatus(int statusCode) {
    this.statusCode = statusCode;
  }

  /**
   * @return status code of this {@link ResponseStatus}
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * @return {@code true} if status code is 2xx which means success. {@code false} otherwise.
   */
  public boolean isSuccess() {
    return statusCode >= 200 && statusCode < 300;
  }

  /**
   * @return {@code true} if status code is 3xx which means redirection. {@code false} otherwise.
   */
  public boolean isRedirection() {
    return statusCode >= 300 && statusCode < 400;
  }

  /**
   * @return {@code true} if status code is 4xx which means client error. {@code false} otherwise.
   */
  public boolean isClientError() {
    return statusCode >= 400 && statusCode < 500;
  }

  /**
   * @return {@code true} if status code is 5xx which means server error. {@code false} otherwise.
   */
  public boolean isServerError() {
    return statusCode >= 500;
  }

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
      case NotAllowed:
        return MethodNotAllowed;
      case ResourceScanInProgress:
        return ProxyAuthenticationRequired;
      case RangeNotSatisfiable:
        return RangeNotSatisfiable;
      case ServiceUnavailable:
        return ServiceUnavailable;
      case TooManyRequests:
        return TooManyRequests;
      case InsufficientCapacity:
        return InsufficientCapacity;
      case PreconditionFailed:
        return PreconditionFailed;
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
