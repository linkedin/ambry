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
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link AmbryIdSigningService}
 */
public class AmbryIdSigningServiceTest {

  /**
   * Test that {@link AmbryIdSigningServiceFactory} works correctly.
   */
  @Test
  public void factoryTest() {
    IdSigningService idSigningService = new AmbryIdSigningServiceFactory(null, null).getIdSigningService();
    assertEquals("Type does not match expected", AmbryIdSigningService.class, idSigningService.getClass());
  }

  /**
   * Testing signing and parsing IDs use {@link AmbryIdSigningService}.
   * @throws Exception
   */
  @Test
  public void signAndParseTest() throws Exception {
    AmbryIdSigningService idSigningService = new AmbryIdSigningService();
    String blobId = "unsignedId";
    Map<String, String> metadata = Stream.of("a", "b", "c").collect(Collectors.toMap(s -> "key-" + s, s -> "val-" + s));
    assertFalse("Original blob should not be considered signed", idSigningService.isIdSigned(blobId));
    String signedId = idSigningService.getSignedId(blobId, metadata);
    for (String id : new String[]{signedId, "/" + signedId}) {
      assertTrue("ID should be considered signed", idSigningService.isIdSigned(id));
      Pair<String, Map<String, String>> idAndMetadata = idSigningService.parseSignedId(id);
      assertEquals("Unexpected blob ID from parseSignedId()", blobId, idAndMetadata.getFirst());
      assertEquals("Unexpected metadata from parseSignedId()", metadata, idAndMetadata.getSecond());
    }

    TestUtils.assertException(RestServiceException.class, () -> idSigningService.getSignedId(null, null),
        errorCodeChecker(RestServiceErrorCode.InternalServerError));

    TestUtils.assertException(RestServiceException.class,
        () -> idSigningService.parseSignedId(signedId.substring("signedId/".length())),
        errorCodeChecker(RestServiceErrorCode.InternalServerError));

    TestUtils.assertException(RestServiceException.class, () -> idSigningService.parseSignedId("signedId/abcdefg"),
        errorCodeChecker(RestServiceErrorCode.BadRequest));
  }

  /**
   * @param restServiceErrorCode the expected error code for the {@link RestServiceException} thrown.
   * @return a {@link ThrowingConsumer} to use with {@link com.github.ambry.utils.TestUtils#assertException}.
   */
  ThrowingConsumer<RestServiceException> errorCodeChecker(RestServiceErrorCode restServiceErrorCode) {
    return restServiceException -> assertEquals("Wrong RestServiceErrorCode in " + restServiceException,
        restServiceErrorCode, restServiceException.getErrorCode());
  }
}
