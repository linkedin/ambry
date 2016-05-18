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
package com.github.ambry.server;

import com.github.ambry.messageformat.BlobProperties;


/**
 * All of the content of a blob
 */
class Payload {
  public byte[] blob;
  public byte[] metadata;
  public BlobProperties blobProperties;
  public String blobId;

  public Payload(BlobProperties blobProperties, byte[] metadata, byte[] blob, String blobId) {
    this.blobProperties = blobProperties;
    this.metadata = metadata;
    this.blob = blob;
    this.blobId = blobId;
  }
}

