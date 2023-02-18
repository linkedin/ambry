/**
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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ReplicationConfig;

/**
 * File manager for BackupChecker, although it can be used for other purposes
 */
public class BackupCheckerFileManager {

  public BackupCheckerFileManager(ReplicationConfig replicationConfig, MetricRegistry metricRegistry) {
  }

  /**
   * Append to a given file.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean appendToFile(String filePath, String text) {
    // open-source impl
    return true;
  }

  /**
   * Truncates a file and then writes to it.
   * Creates the file if absent.
   * @param filePath Path of the file in the system
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean truncateAndWriteToFile(String filePath, String text) {
    // open-source impl
    return true;
  }
}
