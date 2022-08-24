/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.named;

public enum PartialPutStatus {
  /**
   * The put operation for the partially readable blob has finished successfully, with all the chunks saved to server
   * successfully.
   */
  SUCCESS,

  /**
   * The put operation for the partially readable blob is still in progress.
   */
  PENDING,

  /**
   * There is an error with the put process for the partially readable blob
   */
  ERROR,

}