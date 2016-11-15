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
  Ok, /**
   * 201 - Resource was created.
   */
  Created, /**
   * 202 - Request was accepted.
   */
  Accepted, /**
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
  BadRequest, /**
   * 401 - Request Unauthorized
   */
  Unauthorized, /**
   * 403 - Request forbidden
   */
  Forbidden, /**
   * 404 Not Found - Resource was not found.
   */
  NotFound, /**
   * 407 - Proxy authentication required
   */
  ProxyAuthenticationRequired, /**
   * 410 Gone - Resource has been deleted or has expired.
   */
  Gone, /**
   * 416 Range Not Satisfiable - A range request is invalid or outside of the bounds of an object.
   */
  RangeNotSatisfiable,

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
      case UnsupportedHttpMethod:
        return ResponseStatus.BadRequest;
      case ResourceDirty:
        return ResponseStatus.Forbidden;
      case Unauthorized:
        return ResponseStatus.Unauthorized;
      case ResourceScanInProgress:
        return ResponseStatus.ProxyAuthenticationRequired;
      case RangeNotSatisfiable:
        return ResponseStatus.RangeNotSatisfiable;
      case IdConverterServiceError:
      case InternalServerError:
      case RequestChannelClosed:
      case RequestResponseQueuingFailure:
      case ServiceUnavailable:
      case UnsupportedRestMethod:
      default:
        return ResponseStatus.InternalServerError;
    }
  }
}
