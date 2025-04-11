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

  /**
   * @param blobId the blob ID from the deleted record.
   * @param alreadyDeleted {@code true} if the record indicated that the blob was already deleted before this call.
   */
  public DeleteResult(String blobId, boolean alreadyDeleted) {
    this(Collections.singletonList(new BlobVersion(blobId, 0, alreadyDeleted)));
  }

  public DeleteResult(List<BlobVersion> blobVersions) {
    Objects.requireNonNull(blobVersions, "blobVersions cannot be null");
    this.blobVersions = Collections.unmodifiableList(new ArrayList<>(blobVersions));
  }

  public List<BlobVersion> getBlobVersions() {
    return blobVersions;
  }

  public String getBlobIds() {
    // BlobId is in base64 format, "," character is not allowed in base64 encoding.
    return blobVersions.stream().map(BlobVersion::getBlobId).reduce((a, b) -> a + "," + b).orElse("");
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
    return Objects.equals(blobVersions, record.blobVersions);
  }

  @Override
  public String toString() {
    return "DeleteResult[BlobVersions=" + blobVersions + "]";
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

    @Override
    public String toString() {
      return "BlobVersion[blobId=" + blobId + ",version=" + version + ",isAlreadyDeleted=" + isAlreadyDeleted() + "]";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BlobVersion that = (BlobVersion) o;
      return version == that.version && alreadyDeleted == that.alreadyDeleted && Objects.equals(blobId, that.blobId);
    }
  }
}
