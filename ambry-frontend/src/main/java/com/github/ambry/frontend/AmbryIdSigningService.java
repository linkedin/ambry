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

import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;


/**
 * Reference implementation for an {@link IdSigningService}.
 * <p>
 * <b>Warning:</b> This implementation is not secure or tamper-proof, since it does not actually sign the data that it
 * is passed. A secure implementation would either need to use an authenticated encryption scheme or attach a signature.
 */
public class AmbryIdSigningService implements IdSigningService {
  @Override
  public String getSignedId(String blobId, Map<String, String> metadata) throws RestServiceException {
    try {
      String jsonString = SignedIdSerDe.toJson(blobId, metadata);
      return RestUtils.SIGNED_ID_PREFIX + Base64.encodeBase64URLSafeString(jsonString.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new RestServiceException("Error serializing signed ID", e, RestServiceErrorCode.InternalServerError);
    }
  }

  @Override
  public boolean isIdSigned(String id) {
    int searchStart = id.startsWith("/") ? 1 : 0;
    return id.startsWith(RestUtils.SIGNED_ID_PREFIX, searchStart);
  }

  @Override
  public Pair<String, Map<String, String>> parseSignedId(String signedId) throws RestServiceException {
    if (!isIdSigned(signedId)) {
      throw new RestServiceException("Expected ID to be signed: " + signedId, RestServiceErrorCode.InternalServerError);
    }
    try {
      int startIndex = RestUtils.SIGNED_ID_PREFIX.length() + (signedId.startsWith("/") ? 1 : 0);
      String base64String = signedId.substring(startIndex);
      String jsonString = new String(Base64.decodeBase64(base64String), StandardCharsets.UTF_8);
      return SignedIdSerDe.fromJson(jsonString);
    } catch (Exception e) {
      throw new RestServiceException("Error deserializing signed ID", e, RestServiceErrorCode.BadRequest);
    }
  }
}
