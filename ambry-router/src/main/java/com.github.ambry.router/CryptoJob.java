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

import java.security.GeneralSecurityException;


/**
 * CryptoJob representing the job that needs processing by {@link CryptoJobExecutorService}
 */
public interface CryptoJob extends Runnable {
  /**
   * Close the job with the given {@code gse}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callback for the job
   */
  void closeJob(GeneralSecurityException gse);
}
