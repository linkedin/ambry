/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.named;

import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import java.util.List;


/**
 * An implementation of the {@link TransactionStateTracker}. It determine if a request should be retried on another
 * datacenter based on an exception received from the last datacenter tried (processFailure). If processFailure returned
 * true, the next call to getDatacenter should return the datacenter to retry on.
 */
class GetTransactionStateTracker implements TransactionStateTracker {
  private String nextDatacenter;
  private int counter;
  private final List<String> remoteDatacenters;

  /**
   * Constructor for {@link GetTransactionStateTracker}.
   * @param remoteDatacenters list of the remote datacenter names.
   * @param localDatacenter the name of the local datacenter.
   */
  GetTransactionStateTracker(List<String> remoteDatacenters, String localDatacenter) {
    this.remoteDatacenters = remoteDatacenters;
    this.nextDatacenter = localDatacenter;
  }

  /**
   * @param throwable The {@link Throwable} thrown in execution of retrying.
   * @return {@code true} if the {@link Throwable} is {@link RestServiceException} and Get request return not found.
   */
  @Override
  public boolean processFailure(Throwable throwable) {
    boolean isRetriable = throwable instanceof RestServiceException && RestServiceErrorCode.NotFound.equals(
        ((RestServiceException) throwable).getErrorCode());
    if (isRetriable) {
      nextDatacenter = remoteDatacenters.get(counter++);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String getNextDatacenter() {
    return nextDatacenter;
  }
}
