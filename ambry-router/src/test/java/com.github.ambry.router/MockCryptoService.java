/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.CryptoServiceConfig;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;


/**
 * MockCryptoService to assist in testing exception cases
 */
class MockCryptoService extends GCMCryptoService {

  AtomicReference<GeneralSecurityException> exceptionOnEncryption = new AtomicReference<>();
  AtomicReference<GeneralSecurityException> exceptionOnDecryption = new AtomicReference<>();

  MockCryptoService(CryptoServiceConfig cryptoServiceConfig) {
    super(cryptoServiceConfig);
  }

  @Override
  public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
    if (exceptionOnEncryption.get() != null) {
      throw exceptionOnEncryption.get();
    }
    return super.encrypt(toEncrypt, key);
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
    if (exceptionOnDecryption.get() != null) {
      throw exceptionOnDecryption.get();
    }
    return super.decrypt(toDecrypt, key);
  }

  void clearStates() {
    exceptionOnEncryption.set(null);
    exceptionOnDecryption.set(null);
  }
}
