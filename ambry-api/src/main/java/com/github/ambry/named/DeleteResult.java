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
 *
 */

package com.github.ambry.named;

import java.util.Objects;


/**
 * Class to convey information about a successful deletion from {@link NamedBlobDb}.
 */
public class DeleteResult {
  private final String blobId;
  private final boolean alreadyDeleted;
  private boolean hasDataIssue;

  /**
   * @param blobId the blob ID from the deleted record.
   * @param alreadyDeleted {@code true} if the record indicated that the blob was already deleted before this call.
   */
  public DeleteResult(String blobId, boolean alreadyDeleted) {
    this.blobId = blobId;
    this.alreadyDeleted = alreadyDeleted;
  }

  /**
   * @return the blob ID from the deleted record.
   */
  public String getBlobId() {
    return blobId;
  }

  /**
   * @return {@code true} if the record indicated that the blob was already deleted before this call.
   */
  public boolean isAlreadyDeleted() {
    return alreadyDeleted;
  }


  /**
   * @param hasDataIssue whether has data issue for this query (eg data inconsistency between old and new tables).
   */
  public void setHasDataIssue(boolean hasDataIssue) {
    this.hasDataIssue = hasDataIssue;
  }


  /**
   * @return the hasDataIssue for the named blob query.
   */
  public boolean getHasDataIssue() {
    return hasDataIssue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteResult record = (DeleteResult) o;
    return Objects.equals(getBlobId(), record.getBlobId()) && Objects.equals(isAlreadyDeleted(), record.isAlreadyDeleted());
  }

  public String toString() {
    return "DeleteResult[blobId=" + getBlobId() + ",isAlreadyDeleted=" + isAlreadyDeleted() + "]";
  }
}
