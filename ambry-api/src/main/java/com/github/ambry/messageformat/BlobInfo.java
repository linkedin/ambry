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
package com.github.ambry.messageformat;

/**
 * A BlobInfo class that contains both the blob property, the usermetadata and the lifeVersion for the blob.
 */
public class BlobInfo {
  private BlobProperties blobProperties;
  private byte[] userMetadata;
  private short lifeVersion;

  /**
   * Constructor to create a {@link BlobInfo}.
   * @param blobProperties The {@link BlobProperties} of this blob.
   * @param userMetadata The user metadata of this blob.
   */
  public BlobInfo(BlobProperties blobProperties, byte[] userMetadata) {
    this(blobProperties, userMetadata, (short) 0);
  }

  /**
   * Constructor to create a {@link BlobInfo}.
   * @param blobProperties The {@link BlobProperties} of this blob.
   * @param userMetadata The user metadata of this blob.
   * @param lifeVersion The lifeVersion of this blob.
   */
  public BlobInfo(BlobProperties blobProperties, byte[] userMetadata, short lifeVersion) {
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.lifeVersion = lifeVersion;
  }

  /**
   * @return The {@link BlobProperties} of this blob.
   */
  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  /**
   * @return The user metadata of this blob.
   */
  public byte[] getUserMetadata() {
    return userMetadata;
  }

  /**
   * Set the lifeVersion of this blob.
   * @param lifeVersion The lifeVersion to set.
   */
  public void setLifeVersion(short lifeVersion) {
    this.lifeVersion = lifeVersion;
  }

  /**
   * @return The lifeVersion of this blob.
   */
  public short getLifeVersion() {
    return lifeVersion;
  }
}
