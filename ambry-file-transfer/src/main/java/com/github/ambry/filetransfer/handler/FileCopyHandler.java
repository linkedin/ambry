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

import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import java.io.IOException;
import javax.annotation.Nonnull;


/**
 * Interface for FileCopyHandler. This Handler does file copy of
 * data from the given node to current node
 */
public interface FileCopyHandler {
  /**
   * do the file copy
   * @param fileCopyInfo the replica info
   * @throws Exception exception
   */
  void copy(@Nonnull FileCopyInfo fileCopyInfo)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException;
}
