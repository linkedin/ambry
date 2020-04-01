/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.protocol;

/**
 * The list of options for the Get request.
 */
public enum GetOption {
  /**
   * This is the default. This returns all blobs that are not expired and not deleted
   */
  None,
  /**
   * Indicates that the blob should be returned even if it is expired
   */
  Include_Expired_Blobs,
  /**
   * Indicates that the blob should be returned even if it is deleted
   */
  Include_Deleted_Blobs,
  /**
   * Indicates that the blob should be returned regardless of whether it is deleted or expired
   */
  Include_All
}
