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
package com.github.ambry.account;

import java.util.List;


public interface DatasetRetentionPolicy {
  /**
   * List all the dataset version out of retention count.
   * @param allValidDatasetVersionsOrderedByVersion all the valid dataset version record ordered by version.
   * @param retentionCount the retention count.
   * @param versionSchema the version schema of the dataset,
   *                      could be used to check which version schema is allowed for this retention policy.
   * @return @return list of dataset versions out of retention.
   */
  List<DatasetVersionRecord> getDatasetVersionOutOfRetention(
      List<DatasetVersionRecord> allValidDatasetVersionsOrderedByVersion, Integer retentionCount,
      Dataset.VersionSchema versionSchema);
}
