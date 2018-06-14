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
package com.github.ambry.utils;

import java.util.function.Consumer;


/**
 * Similar to {@link Consumer}, but able to throw checked exceptions.
 * @param <T> the type of the input to the operation
 */
public interface ThrowingConsumer<T> {

  /**
   * Performs this operation on the given argument.
   *
   * @param t the input argument
   */
  void accept(T t) throws Exception;
}
