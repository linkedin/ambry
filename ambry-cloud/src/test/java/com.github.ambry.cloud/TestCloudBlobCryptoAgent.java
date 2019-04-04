/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;


/**
 * Dummy implementation for testing.
 */
class TestCloudBlobCryptoAgent implements CloudBlobCryptoAgent {
  @Override
  public ByteBuffer encrypt(ByteBuffer buffer) throws GeneralSecurityException {
    return buffer;
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer buffer) throws GeneralSecurityException {
    return buffer;
  }

  @Override
  public String getEncryptionContext() {
    return null;
  }
}
