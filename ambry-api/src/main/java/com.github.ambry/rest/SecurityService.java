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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.router.Callback;
import java.io.Closeable;
import java.util.concurrent.Future;


/**
 * Responsible for performing any security validations on the Rest request and response. Validations could involve
 * authentication, authorization, security checks and so on which the implementation can decide. This could also involve
 * setting headers while responding, based on the request.
 * Exceptions are returned via {@link Callback}s on any validation failure.
 */
public interface SecurityService extends Closeable {

  /**
   * Perform security validations (if any) on the {@link RestRequest} asynchronously and invokes the
   * {@link Callback} when the validation completes.
   * @param restRequest {@link RestRequest} upon which validations has to be performed
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @return A future that would contain information about whether processing of request succeeded or not,
   * eventually.
   */
  public Future<Void> processRequest(RestRequest restRequest, Callback<Void> callback);

  /**
   * Perform security validations (if any) on the response for {@link RestRequest} asynchronously, sets headers if need
   * be and invokes the {@link Callback} when the validation completes. Similar to
   * {@link SecurityService#processRequest(RestRequest, Callback)}, validations could involve security checks and
   * setting some headers with the response.
   * @param restRequest {@link RestRequest} whose response have to be validated
   * @param responseChannel the {@link RestResponseChannel} over which the response is sent
   * @param blobInfo the {@link BlobInfo} pertaining to the rest request made
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @return A future that would contain information about whether processing of request succeeded or not,
   * eventually.
   */
  public Future<Void> processResponse(RestRequest restRequest, RestResponseChannel responseChannel, BlobInfo blobInfo,
      Callback<Void> callback);
}
