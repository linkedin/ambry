package com.github.ambry.frontend;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;


@FunctionalInterface
public interface RestRequestMatcher {
  boolean match(RestRequest restRequest) throws RestServiceException;

  RestRequestMatcher ALL_MATCHER = restRequest -> true;
}
