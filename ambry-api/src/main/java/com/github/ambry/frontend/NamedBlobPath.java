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
 */
package com.github.ambry.frontend;

/**
 * Represents the blob url parsing results for named blob.
 */
public class NamedBlobPath {
  private final String accountName;
  private final String containerName;
  private final String blobName;

  /**
   * Constructs a {@link NamedBlobPath}
   * @param accountName name of the account for named blob.
   * @param containerName name of the container for named blob.
   * @param blobName name of the blob.
   */
  public NamedBlobPath(String accountName, String containerName, String blobName) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.blobName = blobName;
  }

  /**
   * Get the account name.
   * @return account name of the named blob.
   */
  public String getAccountName() {
    return accountName;
  }

  /**
   * Get the container name.
   * @return container name of the named blob.
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Get the blob name.
   * @return blob name of named blob.
   */
  public String getBlobName() {
    return blobName;
  }
}