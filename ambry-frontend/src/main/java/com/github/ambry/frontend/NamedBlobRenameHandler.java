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

import com.github.ambry.account.Container;
import com.github.ambry.commons.Callback;
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.github.ambry.rest.RestUtils.*;


/**
 * {@link NamedBlobRenameHandler} is a simple wrapper around {@link DeleteBlobHandler} as the only difference between
 * named blob delete and regular blob delete is that named blob provides a named blob id instead of a regular blob id.
 *
 * In the {@link #handle} method, we would first replace the named blob id in the {@link RequestPath} with the
 * corresponding regular blob id and then call {@link DeleteBlobHandler#handle} to handle the request.
 */
class NamedBlobRenameHandler {
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final NamedBlobDb namedBlobDb;

  /**
   * Construct a {@link NamedBlobRenameHandler} for handling requests for deleting blobs in named blob accounts.
   * @param accountAndContainerInjector The {@link AccountAndContainerInjector} to use.
   * @param namedBlobDb The {@link NamedBlobDb} to use.
   * @param metrics The {@link FrontendMetrics} instance where metrics are recorded
   */
  NamedBlobRenameHandler(AccountAndContainerInjector accountAndContainerInjector, NamedBlobDb namedBlobDb,
      FrontendMetrics metrics) {
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.namedBlobDb = namedBlobDb;
    this.metrics = metrics;
  }

  /**
   * Asynchronously handle named blob delete requests.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
      throws RestServiceException, ExecutionException, InterruptedException {
    accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.deleteBlobMetricsGroup);
    RequestPath requestPath = getRequestPath(restRequest);
    // Get blob id and reconstruct RequestPath
    // this is the three-part named blob id, including account name, container name and the custom blob name.
    String namedBlobId = requestPath.getOperationOrBlobId(false);
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(namedBlobId, Collections.emptyMap());

    String newName = (String) restRequest.getArgs().get("rename");
    NamedBlobRecord record = new NamedBlobRecord(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
        namedBlobPath.getBlobName(), "", 0, 0, 0);

    Container container = (Container) restRequest.getArgs().get(InternalKeys.TARGET_CONTAINER_KEY);
    if(!container.isHierarchicalNameSpaceEnabled()){
      callback.onCompletion(null, new RestServiceException("Rename is not allowed", RestServiceErrorCode.BadRequest));
      return;
    }

    namedBlobDb.renameDirectory(record, newName).get();
    callback.onCompletion(null, null);

  }
}
