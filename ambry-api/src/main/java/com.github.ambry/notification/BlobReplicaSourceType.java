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
package com.github.ambry.notification;

/**
 * The enumeration of all the sources by which a replica can be created in the system
 */
public enum BlobReplicaSourceType {
  /**
   * The blob replica was created by a primary write. This means that the blob
   * was written directly to the server
   */
  PRIMARY,
  /**
   * The blob replica was created by a repair operation. This could be because the primary
   * write failed or because a replica needs to be restored from a repair
   */
  REPAIRED
}
