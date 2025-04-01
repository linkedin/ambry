/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer.handler;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Configuration for FileCopyHandler
 */
public class FileCopyHandlerConfig {
  /**
   * The maximum number of retries for the API call
   */
  public static final String FILECOPYHANDLER_MAX_API_RETRIES = "filecopyhandler.max.api.retries";
  @Config(FILECOPYHANDLER_MAX_API_RETRIES)
  @Default("3")
  public final int fileCopyHandlerMaxApiRetries;

  /**
   * The backoff time in milliseconds between retries
   */
  public static final String FILECOPYHANDLER_RETRY_BACKOFF_MS = "filecopyhandler.retry.backoff.ms";
  @Config(FILECOPYHANDLER_RETRY_BACKOFF_MS)
  @Default("500")
  public final int fileCopyHandlerRetryBackoffMs;

  /**
   * The chunk size for file copy
   */
  public static final String FILECOPYHANDLER_CHUNK_SIZE = "filecopyhandler.chunk.size";
  @Config(FILECOPYHANDLER_CHUNK_SIZE)
  @Default("10485760") // 10 MB
  public final int getFileCopyHandlerChunkSize;

  public static final String FILECOPYHANDLER_CONNECTION_TIMEOUT_MS = "filecopyhandler.connection.timeout.ms";
  @Config(FILECOPYHANDLER_CONNECTION_TIMEOUT_MS)
  @Default("5000")
  public final int fileCopyHandlerConnectionTimeoutMs;

  /**
   * Constructor to create FileCopyHandlerConfig
   * @param verifiableProperties the properties
   */
  public FileCopyHandlerConfig(@Nonnull VerifiableProperties verifiableProperties) {
    Objects.requireNonNull(verifiableProperties, "verifiableProperties cannot be null");

    fileCopyHandlerMaxApiRetries = verifiableProperties.getInt(FILECOPYHANDLER_MAX_API_RETRIES, 3);
    fileCopyHandlerRetryBackoffMs = verifiableProperties.getInt(FILECOPYHANDLER_RETRY_BACKOFF_MS, 500);
    getFileCopyHandlerChunkSize = verifiableProperties.getInt(FILECOPYHANDLER_CHUNK_SIZE, 10 * 1024 * 1024); // 10 MB
    fileCopyHandlerConnectionTimeoutMs = verifiableProperties.getInt(FILECOPYHANDLER_CONNECTION_TIMEOUT_MS, 5000);
  }
}
