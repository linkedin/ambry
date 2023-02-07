/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.Callback;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Utils;
import java.util.Collections;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;


/**
 * {@link NamedBlobGetHandler} is a simple wrapper around {@link GetBlobHandler} as the only difference between
 * named blob get and regular blob get is that named blob provides a named blob id instead of regular blob id.
 *
 * In the {@link #handle} method, we would first replace the named blob id in the {@link RequestPath} with the
 * corresponding regular blob id and then call {@link GetBlobHandler#handle} to handle the request.
 */
class NamedBlobGetHandler {
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final GetBlobHandler getBlobHandler;
  private final NamedBlobDb namedBlobDb;

  /**
   * Construct a {@link NamedBlobGetHandler} for handling requests for getting blobs in named blob accounts.
   * @param accountAndContainerInjector The {@link AccountAndContainerInjector} to use.
   * @param namedBlobDb The {@link NamedBlobDb} to use.
   * @param metrics The {@link FrontendMetrics} instance where metrics are recorded
   * @param getBlobHandler The {@link GetBlobHandler}.
   */
  NamedBlobGetHandler(AccountAndContainerInjector accountAndContainerInjector, NamedBlobDb namedBlobDb,
      FrontendMetrics metrics, GetBlobHandler getBlobHandler) {
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.namedBlobDb = namedBlobDb;
    this.metrics = metrics;
    this.getBlobHandler = getBlobHandler;
  }

  /**
   * Asynchronously handle named blob get requests.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    RequestPath requestPath = getRequestPath(restRequest);
    accountAndContainerInjector.injectAccountContainerAndDatasetForNamedBlob(restRequest,
        getMetricsGroupForGet(metrics, requestPath.getSubResource()));

    // Get blob id and reconstruct RequestPath
    // this is the three-part named blob id, including account name, container name and the custom blob name.
    String namedBlobId = requestPath.getOperationOrBlobId(false);
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(namedBlobId, Collections.emptyMap());
    GetOption getOption = getGetOption(restRequest, GetOption.None);
    namedBlobDb.get(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(), namedBlobPath.getBlobName(),
        getOption).thenApply(NamedBlobRecord::getBlobId).whenComplete((blobId, exception) -> {
      if (exception != null) {
        callback.onCompletion(null, Utils.extractFutureExceptionCause(exception));
        return;
      }
      RequestPath newRequestPath =
          new RequestPath(requestPath.getPrefix(), requestPath.getClusterName(), requestPath.getPathAfterPrefixes(),
              "/" + blobId + "." + DEFAULT_EXTENSION, requestPath.getSubResource(), requestPath.getBlobSegmentIdx());
      // Replace RequestPath in the RestRequest and call GetBlobHandler.handle.
      restRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
      restRequest.setArg(InternalKeys.FILENAME_HINT, newRequestPath.getOperationOrBlobId(true));
      try {
        getBlobHandler.handle(newRequestPath, restRequest, restResponseChannel, callback);
      } catch (RestServiceException e) {
        callback.onCompletion(null, e);
      }
    });
  }
}
