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
 * A BlobInfo class that contains both the blob property and the usermetadata for the blob
 */
public class BlobInfo {

  private BlobProperties blobProperties;

  private byte[] userMetadata;

  private short lifeVersion;

  public BlobInfo(BlobProperties blobProperties, byte[] userMetadata) {
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.lifeVersion = 0;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

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
