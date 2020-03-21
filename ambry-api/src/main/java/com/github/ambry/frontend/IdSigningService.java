/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.frontend;

import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Pair;
import java.util.Map;


/**
 * Responsible for providing and verifying blob IDs that are signed with some additional metadata. The implementation
 * should include a secure signature field that prevents other parties from tampering with the ID. The implementation
 * can also choose to encrypt/decrypt the signed ID. The generated signed ID must be URL-safe.
 */
public interface IdSigningService {

  /**
   * Get a signed ID based on the input blob ID and provided metadata. May not do any checking to ensure that the
   * request is authorized to generate a signed ID.
   * @param blobId the blob ID to include in the signed ID.
   * @param metadata additional parameters to include in the signed ID.
   * @return a URL-safe signed ID.
   * @throws RestServiceException if the signed ID could not be generated.
   */
  String getSignedId(String blobId, Map<String, String> metadata) throws RestServiceException;

  /**
   * Implementations of this method should transparently handle any valid ID prefixes or suffixes
   * (leading slashes, etc.) that may be added by an {@link IdConverter}.
   * @param id the input ID to check.
   * @return {@code true} if the ID is signed. {@code false} otherwise
   */
  boolean isIdSigned(String id);

  /**
   * Verify that the signed ID has not been tampered with and extract the blob ID and additional metadata from the
   * signed ID. Implementations of this method should transparently handle any valid ID prefixes or suffixes
   * (leading slashes, etc.) that may be added by an {@link IdConverter}.
   * @return a {@link Pair} that contains the blob ID and additional metadata parsed from the signed ID.
   * @throws RestServiceException if there are problems verifying or parsing the ID.
   */
  Pair<String, Map<String, String>> parseSignedId(String signedId) throws RestServiceException;
}
