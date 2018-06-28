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
package com.github.ambry.store;

/**
 * An interface for a transformation function. Transformations can modify any data in the message (including keys).
 * Needs to be thread safe.
 */
public interface Transformer {

  /**
   * Transforms the input {@link Message} into an output {@link Message}.
   * @param message the input {@link Message} to change.
   * @return the output {@link TransformationOutput}.
   */
  TransformationOutput transform(Message message);
}


