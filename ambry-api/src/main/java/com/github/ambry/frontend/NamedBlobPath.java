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

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import java.util.Objects;


/**
 * Represents the blob url parsing results for named blob.
 */
public class NamedBlobPath {
  private final String accountName;
  private final String containerName;
  private final String blobName;

  /**
   * Parse the input path if it's a named blob request.
   * @param path the URI path that needs to be parsed. This path should already be URL decoded and have query paramaters
   *             removed. In the request handling path, this would be done by the {@link RestRequest} implementation.
   * @return the {@link NamedBlobPath} that indicates the parsing result from blobUrl.
   * @throws RestServiceException on parsing errors.
   */
  public static NamedBlobPath parse(String path) throws RestServiceException {
    Objects.requireNonNull(path, "input should not be null");
    path = path.startsWith("/") ? path.substring(1) : path;
    String[] splitPath = path.split("/", 4);
    if (splitPath.length != 4 || !Operations.NAMED_BLOB.equalsIgnoreCase(splitPath[0])) {
      throw new RestServiceException(
          "File must have name format '/named/<account_name>/<container_name>/<blob_name>'.  Received: '" + path + "'",
          RestServiceErrorCode.BadRequest);
    }
    String accountName = splitPath[1];
    String containerName = splitPath[2];
    String blobName = splitPath[3];
    return new NamedBlobPath(accountName, containerName, blobName);
  }

  /**
   * Constructs a {@link NamedBlobPath}
   * @param accountName name of the account for named blob.
   * @param containerName name of the container for named blob.
   * @param blobName name of the blob.
   */
  private NamedBlobPath(String accountName, String containerName, String blobName) {
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
