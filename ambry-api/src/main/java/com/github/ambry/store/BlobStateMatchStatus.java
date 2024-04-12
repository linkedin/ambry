/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

/**
 * Status code blob-state match operation
 */
public enum BlobStateMatchStatus {
  BLOB_STATE_MATCH,
  BLOB_ABSENT,
  BLOB_STATE_CLASS_MISMATCH,
  BLOB_STATE_KEY_MISMATCH,
  BLOB_STATE_SIZE_MISMATCH,
  BLOB_STATE_EXPIRY_MISMATCH,
  BLOB_STATE_OBSOLETE_MISMATCH,
  BLOB_STATE_CRC_MISMATCH,
  BLOB_STATE_VERSION_MISMATCH
}
