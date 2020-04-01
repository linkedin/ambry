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
package com.github.ambry.frontend;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.router.Callback;
import java.io.Closeable;
import java.util.concurrent.Future;


/**
 * This is a service that can be used to convert IDs across different formats.
 * </p>
 * Typical usage will be to add extensions, tailor IDs for different use-cases, encrypt/obfuscate IDs or get name
 * mappings.
 */
public interface IdConverter extends Closeable {

  /**
   * Converts an ID.
   * @param restRequest {@link RestRequest} representing the request.
   * @param input the ID that needs to be converted.
   * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
   * @return a {@link Future} that will eventually contain the converted ID.
   */
  Future<String> convert(RestRequest restRequest, String input, Callback<String> callback);
}
