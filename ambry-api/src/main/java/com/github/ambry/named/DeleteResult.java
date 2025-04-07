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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * Class to convey information about a successful deletion from {@link NamedBlobDb}.
 */
public class DeleteResult {
  private final List<BlobVersion> blobVersions;
  private final String blobId;
  private final boolean alreadyDeleted;

  /**
   * @param blobId the blob ID from the deleted record.
   * @param alreadyDeleted {@code true} if the record indicated that the blob was already deleted before this call.
   */
  public DeleteResult(String blobId, boolean alreadyDeleted) {
    this.blobId = blobId;
    this.alreadyDeleted = alreadyDeleted;
    blobVersions = new ArrayList<>();
    blobVersions.add(new BlobVersion(blobId, 0, alreadyDeleted));
  }

  public DeleteResult(List<BlobVersion> blobVersions) {
    this.blobVersions = Collections.unmodifiableList(new ArrayList<>(blobVersions));
    this.blobId = this.blobVersions.size() > 0 ? this.blobVersions.get(0).getBlobId() : null;
    this.alreadyDeleted = this.blobVersions.size() > 0 ? this.blobVersions.get(0).isAlreadyDeleted() : false;
  }

  public List<BlobVersion> getBlobVersions() {
    return blobVersions;
  }

  public String getBlobIdsForAllVersions() {
    // BlobId is in base64 format, "," character is not allowed in base64 encoding.
    return blobVersions.stream().map(BlobVersion::getBlobId).reduce((a, b) -> a + "," + b).orElse("");
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteResult record = (DeleteResult) o;
    return Objects.equals(blobId, record.blobId) && Objects.equals(alreadyDeleted, record.alreadyDeleted);
  }

  @Override
  public String toString() {
    return "DeleteResult[blobId=" + getBlobId() + ",isAlreadyDeleted=" + isAlreadyDeleted() + "]";
  }

  public static class BlobVersion {
    private final String blobId;
    private final long version;
    private final boolean alreadyDeleted;

    public BlobVersion(String blobId, long version, boolean alreadyDeleted) {
      this.blobId = blobId;
      this.version = version;
      this.alreadyDeleted = alreadyDeleted;
    }

    public String getBlobId() {
      return blobId;
    }

    public long getVersion() {
      return version;
    }

    public boolean isAlreadyDeleted() {
      return alreadyDeleted;
    }
  }
}
