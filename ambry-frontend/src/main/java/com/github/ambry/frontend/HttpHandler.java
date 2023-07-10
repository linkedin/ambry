package com.github.ambry.frontend;

import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;


@FunctionalInterface
public interface HttpHandler {
  void handleRequest(RestRequest request, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException;
}
