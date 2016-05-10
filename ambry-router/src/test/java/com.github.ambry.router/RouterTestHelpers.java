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
package com.github.ambry.router;

import com.github.ambry.messageformat.BlobProperties;


/**
 * Class with helper methods for testing the router.
 */
class RouterTestHelpers {
  /**
   * Test whether two {@link BlobProperties} have the same fields
   * @return true if the fields are equivalent in the two {@link BlobProperties}
   */
  static boolean haveEquivalentFields(BlobProperties a, BlobProperties b) {
    return a.getBlobSize() == b.getBlobSize() &&
        a.getServiceId().equals(b.getServiceId()) &&
        a.getOwnerId().equals(b.getOwnerId()) &&
        a.getContentType().equals(b.getContentType()) &&
        a.isPrivate() == b.isPrivate() &&
        a.getTimeToLiveInSeconds() == b.getTimeToLiveInSeconds() &&
        a.getCreationTimeInMs() == b.getCreationTimeInMs();
  }
}

