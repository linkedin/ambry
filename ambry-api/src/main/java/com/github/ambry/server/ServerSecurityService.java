/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.commons.Callback;
import com.github.ambry.router.FutureResult;
import java.io.Closeable;
import java.util.concurrent.Future;
import javax.net.ssl.SSLSession;


/**
 * Responsible for performing any security validations on the HTTP2 connection terminating on server.
 * Validations could involve authentication, authorization, security checks and so on which the implementation
 * can decide. This could also involve setting headers while responding, based on the request.
 * Exceptions are returned via {@link Callback}s on any validation failure.
 */
public interface ServerSecurityService extends Closeable {

  /**
   * Performs security validations (if any) before allowing the HTTP2 connection setup to be complete and invokes the
   * {@code callback} once done.
   * @param sslSession the {@link SSLSession} to process.
   * @param callback the callback to invoke once processing is finished.
   */
  void validateConnection(SSLSession sslSession, Callback<Void> callback);

  /**
   * Performs security validations (if any) on the individual stream {@link RestRequest} asynchronously and invokes the
   * {@link Callback} when the validation completes.
   * @param restRequest {@link RestRequest} upon which validations have to be performed
   * @param callback The {@link Callback} which will be invoked on the completion of the request. Cannot be null.
   */
  void validateRequest(RestRequest restRequest, Callback<Void> callback);

  /**
   * Similar to {@link #validateConnection(SSLSession, Callback)} but returns a {@link Future}
   * instead of requiring a callback.
   * @param sslSession {@link SSLSession} upon which validations has to be performed
   * @return a {@link Future} that is completed when the processing is done.
   */
  default Future<Void> validateConnection(SSLSession sslSession) {
    FutureResult<Void> futureResult = new FutureResult<>();
    validateConnection(sslSession, futureResult::done);
    return futureResult;
  }

  /**
   * Similar to {@link #validateRequest(RestRequest, Callback)} but returns a {@link Future} instead of requiring
   * a callback.
   * @param restRequest {@link RestRequest} upon which validations has to be performed
   * @return a {@link Future} that is completed when the processing is done.
   */
  default Future<Void> validateRequest(RestRequest restRequest) {
    FutureResult<Void> futureResult = new FutureResult<>();
    validateRequest(restRequest, futureResult::done);
    return futureResult;
  }
}
