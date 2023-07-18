/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static com.github.ambry.rest.RestUtils.Headers.*;


/**
 * Represents the dataset url parsing results for dataset path.
 */
public class DatasetVersionPath {
  private final String accountName;
  private final String containerName;
  private final String datasetName;
  private final String version;

  public static DatasetVersionPath parse(RequestPath requestPath, Map<String, Object> args) throws RestServiceException {
    return parse(requestPath.getOperationOrBlobId(true), args);
  }

  /**
   * Parse the input path if it's a dataset request.
   * @param path the URI path that needs to be parsed. This path should already be URL decoded and have query paramaters
   *             removed. In the request handling path, this would be done by the {@link com.github.ambry.rest.RestRequest} implementation.
   * @param args a map containing any query parameters from the request.
   * @return the {@link DatasetVersionPath} that indicates the parsing result from blobUrl.
   * @throws RestServiceException
   */
  public static DatasetVersionPath parse(String path, Map<String, Object> args) throws RestServiceException {
    Objects.requireNonNull(path, "path should not be null");
    Objects.requireNonNull(args, "args should not be null");
    boolean isListRequest = RestUtils.getBooleanHeader(args, ENABLE_DATASET_VERSION_LISTING, false);
    path = path.startsWith("/") ? path.substring(1) : path;
    String[] splitPath = path.split("/");
    int expectedMinimumSegments = isListRequest ? 4 : 5;
    if (splitPath.length < expectedMinimumSegments) {
      throw new RestServiceException(String.format(
          "Blob name must have format '/named/<account_name>/<container_name>/<dataset_name>%s'.  Received path='%s'",
          isListRequest ? "" : "/<version>'", path), RestServiceErrorCode.BadRequest);
    }
    String accountName = splitPath[1];
    String containerName = splitPath[2];
    String datasetName;
    //if isListRequest == true, the path format should be /named/<account_name>/<container_name>/<dataset_name>
    //if isListRequest == false, the path format should be /named/<account_name>/<container_name>/<dataset_name>/<version>
    if (isListRequest) {
      datasetName = String.join("/", Arrays.copyOfRange(splitPath, 3, splitPath.length));
      return new DatasetVersionPath(accountName, containerName, datasetName, null);
    }
    datasetName = String.join("/", Arrays.copyOfRange(splitPath, 3, splitPath.length - 1));
    String version = splitPath[splitPath.length - 1];
    return new DatasetVersionPath(accountName, containerName, datasetName, version);
  }

  /**
   * Construct a {@link DatasetVersionPath}
   * @param accountName name of the parent account.
   * @param containerName name of the container.
   * @param datasetName name of the dataset.
   * @param version the version of the dataset.
   */
  private DatasetVersionPath(String accountName, String containerName, String datasetName, String version) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.datasetName = datasetName;
    this.version = version;
  }

  /**
   * @return the account name.
   */
  public String getAccountName() {return accountName;}

  /**
   * @return the container name.
   */
  public String getContainerName() {return containerName;}

  /**
   * @return the dataset name.
   */
  public String getDatasetName() {return datasetName;}

  /**
   * @return the dataset version.
   */
  public String getVersion() {return version;}

}
