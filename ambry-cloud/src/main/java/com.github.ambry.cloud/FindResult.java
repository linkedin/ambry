/**
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
package com.github.ambry.cloud;

import com.github.ambry.replication.FindToken;
import java.util.List;


/**
 * Contains the results from the replication feed after a find next entries operation.
 */
public class FindResult {
  private final List<CloudBlobMetadata> metadataList;
  private final FindToken updatedFindToken;

  /**
   * Constructor for {@link FindResult}
   * @param metadataList {@link List} of {@link CloudBlobMetadata} objects.
   * @param updatedFindToken updated {@link FindToken}
   */
  public FindResult(List<CloudBlobMetadata> metadataList, FindToken updatedFindToken) {
    this.metadataList = metadataList;
    this.updatedFindToken = updatedFindToken;
  }

  /**
   * Return {@link List} of {@link CloudBlobMetadata} objects.
   * @return {@code metadataList}
   */
  public List<CloudBlobMetadata> getMetadataList() {
    return metadataList;
  }

  /**
   * Return updated {@link FindToken}
   * @return {@code updatedFindToken}
   */
  public FindToken getUpdatedFindToken() {
    return updatedFindToken;
  }
}
