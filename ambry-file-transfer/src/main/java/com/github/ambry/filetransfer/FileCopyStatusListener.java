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
package com.github.ambry.filetransfer;

import com.github.ambry.filetransfer.handler.FileCopyHandler;


/**
 * This interface contains methods which will be called post {@link FileCopyHandler} is
 * completed in {@link FileCopyThread}
 */
public interface FileCopyStatusListener {
  /**
   * This will be called when file copy is successful i.e.
   * no exception from {@link FileCopyHandler}
   */
  void onFileCopySuccess();

  /**
   * This will be called when file copy is failed i.e.
   * exception from {@link FileCopyHandler}
   * @param e exception
   */
  void onFileCopyFailure(Exception e);
}
