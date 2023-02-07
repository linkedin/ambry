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
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Utils;
import java.util.Collections;

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
    accountAndContainerInjector.injectAccountContainerAndDatasetForNamedBlob(restRequest, metrics.deleteBlobMetricsGroup);
    RequestPath requestPath = getRequestPath(restRequest);
    // Get blob id and reconstruct RequestPath
    // this is the three-part named blob id, including account name, container name and the custom blob name.
    String namedBlobId = requestPath.getOperationOrBlobId(false);
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(namedBlobId, Collections.emptyMap());
    // This is a three steps operation
    // 1. Delete database row.
    // 2. Delete regular blob id from Router
    // Ie we failed at step 1, or step 1 returns NotFound, then  we just return the error back to client, indicating failure
    // If we failed at step 2, client can resend the request to ambry so we can resume the operation. If regular blob is already
    //    deleted, Router would do a nop and return success
    // The reason why we don't do this in a get-RouterDelete-delete is because in between get and delete, if another client sends
    // delete and put, then we will have a totally different blob id under this blob name. And we shouldn't delete this new blob id
    // from database.
    namedBlobDb.delete(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(), namedBlobPath.getBlobName())
        .thenApply(DeleteResult::getBlobId)
        .whenComplete((blobId, exception) -> {
          if (exception != null) {
            Exception dbException = Utils.extractFutureExceptionCause(exception);
            callback.onCompletion(null, dbException);
            return;
          }
          // What if this blobName is already deleted?
          // We still want to issue DELETE to router, since router might still have a valid blob because we delete blobName
          // from database before deleting blob from router. We might fail after we delete from database and the client retries
          // delete. Even if client doesn't retry delete, we still have a background job to delete the orphan blob from router.
          RequestPath newRequestPath =
              new RequestPath(requestPath.getPrefix(), requestPath.getClusterName(), requestPath.getPathAfterPrefixes(),
                  "/" + blobId + "." + DEFAULT_EXTENSION, requestPath.getSubResource(),
                  requestPath.getBlobSegmentIdx());
          // Replace RequestPath in the RestRequest and call DeleteBlobHandler.handle.
          restRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
          try {
            deleteBlobHandler.handle(restRequest, restResponseChannel, callback);
          } catch (RestServiceException e) {
            callback.onCompletion(null, e);
          }
        });
  }
}
