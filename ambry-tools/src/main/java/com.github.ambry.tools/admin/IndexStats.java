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
package com.github.ambry.tools.admin;

import java.util.concurrent.atomic.AtomicLong;


public class IndexStats {
  private AtomicLong totalPutRecords = new AtomicLong(0);
  private AtomicLong totalDeleteRecords = new AtomicLong(0);
  private AtomicLong totalDuplicatePutRecords = new AtomicLong(0);
  private AtomicLong totalDeleteBeforePutRecords = new AtomicLong(0);
  private AtomicLong totalPutAfterDeleteRecords = new AtomicLong(0);
  private AtomicLong totalDuplicateDeleteRecords = new AtomicLong(0);

  AtomicLong getTotalPutRecords() {
    return totalPutRecords;
  }

  void incrementTotalPutRecords() {
    this.totalPutRecords.incrementAndGet();
  }

  AtomicLong getTotalDeleteRecords() {
    return totalDeleteRecords;
  }

  void incrementTotalDeleteRecords() {
    this.totalDeleteRecords.incrementAndGet();
  }

  AtomicLong getTotalDuplicatePutRecords() {
    return totalDuplicatePutRecords;
  }

  void incrementTotalDuplicatePutRecords() {
    this.totalDuplicatePutRecords.incrementAndGet();
  }

  AtomicLong getTotalDeleteBeforePutRecords() {
    return totalDeleteBeforePutRecords;
  }

  void incrementTotalDeleteBeforePutRecords() {
    this.totalDeleteBeforePutRecords.incrementAndGet();
  }

  AtomicLong getTotalPutAfterDeleteRecords() {
    return totalPutAfterDeleteRecords;
  }

  void incrementTotalPutAfterDeleteRecords() {
    this.totalPutAfterDeleteRecords.incrementAndGet();
  }

  AtomicLong getTotalDuplicateDeleteRecords() {
    return totalDuplicateDeleteRecords;
  }

  void incrementTotalDuplicateDeleteRecords() {
    this.totalDuplicateDeleteRecords.incrementAndGet();
  }
}