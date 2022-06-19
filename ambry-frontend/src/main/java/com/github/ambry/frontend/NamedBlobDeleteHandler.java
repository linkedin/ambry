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
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.GregorianCalendar;

import static com.github.ambry.rest.RestUtils.*;


/**
 * {@link NamedBlobDeleteHandler} is a simple wrapper around {@link DeleteBlobHandler} as the only difference between
 * named blob delete and regular blob delete is that named blob provides a named blob id instead of a regular blob id.
 *
 * In the {@link #handle} method, we would first replace the named blob id in the {@link RequestPath} with the
 * corresponding regular blob id and then call {@link DeleteBlobHandler#handle} to handle the request.
 */
class NamedBlobDeleteHandler {
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final DeleteBlobHandler deleteBlobHandler;
  private final NamedBlobDb namedBlobDb;

  /**
   * Construct a {@link NamedBlobDeleteHandler} for handling requests for deleting blobs in named blob accounts.
   * @param accountAndContainerInjector The {@link AccountAndContainerInjector} to use.
   * @param namedBlobDb The {@link NamedBlobDb} to use.
   * @param metrics The {@link FrontendMetrics} instance where metrics are recorded
   * @param deleteBlobHandler The {@link DeleteBlobHandler}.
   */
  NamedBlobDeleteHandler(AccountAndContainerInjector accountAndContainerInjector, NamedBlobDb namedBlobDb,
      FrontendMetrics metrics, DeleteBlobHandler deleteBlobHandler) {
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.namedBlobDb = namedBlobDb;
    this.metrics = metrics;
    this.deleteBlobHandler = deleteBlobHandler;
  }

  /**
   * Asynchronously handle named blob delete requests.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
      throws RestServiceException {
    RequestPath requestPath = getRequestPath(restRequest);
    // Get blob id and reconstruct RequestPath
    // this is the three-part named blob id, including account name, container name and the custom blob name.
    String namedBlobId = requestPath.getOperationOrBlobId(false);
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(namedBlobId, Collections.emptyMap());
    // This is a three steps operation
    // 1. Get regular blob id from named blob id.
    // 2. Delete regular blob id from Router
    // 3. Delete database row.
    // If we failed at step 1, or step 1 returns NotFound, then  we just return the error back to client, indicating failure
    // If we failed at step 2, client can resend the request to ambry so we can resume the operation. If regular blob is already
    //    deleted, Router would do a nop and return success
    // If we failed at step 3, client can still resend the delete request to delete this blob again.
    namedBlobDb.get(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(), namedBlobPath.getBlobName())
        .thenApply(NamedBlobRecord::getBlobId)
        .whenComplete((blobId, exception) -> {
          if (exception != null) {
            Exception dbException = Utils.extractFutureExceptionCause(exception);
            if (dbException instanceof RestServiceException
                && ((RestServiceException) dbException).getErrorCode() == RestServiceErrorCode.Deleted) {
              // Already deleted from named blob database, return as nop.
              Exception responseException = null;
              try {
                restResponseChannel.setStatus(ResponseStatus.Accepted);
                restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
                restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
              } catch (Exception restResponseException) {
                responseException = restResponseException;
              }
              callback.onCompletion(null, responseException);
            } else {
              callback.onCompletion(null, dbException);
            }
            return;
          }
          RequestPath newRequestPath =
              new RequestPath(requestPath.getPrefix(), requestPath.getClusterName(), requestPath.getPathAfterPrefixes(),
                  "/ " + blobId, requestPath.getSubResource(), requestPath.getBlobSegmentIdx());
          // Replace RequestPath in the RestRequest and call GetBlobHandler.handle.
          restRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
          try {
            //accountAndContainerInjector.injectAccountAndContainerForNamedBlob(restRequest,
            //    metrics.deleteBlobMetricsGroup);
            deleteBlobHandler.handle(restRequest, restResponseChannel, newRequestPath, (v, e) -> {
              if (e != null) {
                callback.onCompletion(null, e);
              } else {
                namedBlobDb.delete(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
                    namedBlobPath.getBlobName()).whenComplete((dbDeleteResult, dbDeleteException) -> {
                  callback.onCompletion(null, Utils.extractFutureExceptionCause(dbDeleteException));
                });
              }
            });
          } catch (RestServiceException e) {
            callback.onCompletion(null, e);
          }
        });
  }
}
