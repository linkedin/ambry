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

import java.security.SecureRandom;


class CryptoUtils {

  /**
   * Generates and returns a random Hex String of the specified size
   * @param size expected key hex string size
   * @return the hex string thus generated
   */
  static String getRandomKey(int size, SecureRandom random) {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < size) {
      sb.append(Integer.toHexString(random.nextInt()));
    }
    sb.setLength(size);
    return sb.toString();
  }
}
