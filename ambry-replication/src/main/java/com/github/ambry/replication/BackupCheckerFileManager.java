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
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File manager for BackupChecker, although it can be used for other purposes
 */
public class BackupCheckerFileManager {
  private final Logger logger = LoggerFactory.getLogger(BackupCheckerFileManager.class);

  public BackupCheckerFileManager(ReplicationConfig replicationConfig, MetricRegistry metricRegistry) {
  }

  /**
   * Write to a given file at a given offset
   * @param filePath Path of the file in the system
   * @param options File options to use when creating the file, if absent
   * @param text Text to append
   * @param offset Offset to write to in the file
   * @return True if append was successful, false otherwise
   */
  protected boolean writeToFile(String filePath, EnumSet<StandardOpenOption> options, String text, int offset) {
    logger.info(text);
    return true;
  }

  /**
   * Appends to a given file
   * @param filePath Path of the file in the system
   * @param options File options to use when creating the file, if absent
   * @param text Text to append
   * @return True if append was successful, false otherwise
   */
  protected boolean appendToFile(String filePath, EnumSet<StandardOpenOption> options, String text) {
    return writeToFile(filePath, options, text, -1);
  }
}
