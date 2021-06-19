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

/**
 * An operation to determine if a request should be retried on another datacenter based on an exception received from the last datacenter
 * tried (processFailure). If processFailure returned true, the next call to getDatacenter should return the datacenter to retry on.
 */
interface TransactionStateTracker {
  /**
   * This method should also set state such that getNextDatacenter will return correct DC next call.
   * @return true if the error can be retried, false if the error is terminal
   */
  boolean processFailure(Throwable t);

  /**
   * @return datacenter to try on, probably start from local datacenter
   */
  String getNextDatacenter();
}
