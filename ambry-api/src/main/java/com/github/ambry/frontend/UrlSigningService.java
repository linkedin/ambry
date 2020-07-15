/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.commons.Callback;


/**
 * Responsible for providing and verifying signed URLs.
 */
public interface UrlSigningService {

  /**
   * Gets a signed URL as described by {@code restRequest}. May not do any checking to ensure that the request is
   * authorized to generate a signed URL.
   * @param restRequest the {@link RestRequest} that contains all the details for generating a signed URL.
   * @return a signed URL.
   * @throws RestServiceException if the URL could not be generated.
   */
  String getSignedUrl(RestRequest restRequest) throws RestServiceException;

  /**
   * @param restRequest the {@link RestRequest} to check.
   * @return {@code true} if the request is signed request. {@code false} otherwise
   */
  boolean isRequestSigned(RestRequest restRequest);

  /**
   * Verifies that the signature in {@code restRequest} is valid. If the implementation does not require any blocking
   * remote calls, this method can be simpler to implement. If remote calls are required, it is preferred to implement
   * both this method and {@link #verifySignedRequest(RestRequest, Callback)}.
   * @param restRequest the {@link RestRequest} to check.
   * @throws RestServiceException if there are problems verifying the URL.
   */
  void verifySignedRequest(RestRequest restRequest) throws RestServiceException;

  /**
   * Verifies that the signature in {@code restRequest} is valid. Any remote calls in the implementation should be made
   * asynchronously.
   * @param restRequest the {@link RestRequest} to check.
   * @param callback the {@link Callback} that will be called after signature verification.
   */
  default void verifySignedRequest(RestRequest restRequest, Callback<Void> callback) {
    Exception exception = null;
    try {
      verifySignedRequest(restRequest);
    } catch (Exception e) {
      exception = e;
    } finally {
      callback.onCompletion(null, exception);
    }
  }
}
