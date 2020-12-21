/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

/**
 * The Operation being performed from the point of view of quota calculation.
 * All the actual http operations (like POST/PUT/GET etc) will be mapped to one of the {@link QuotaOperation}s for quota
 * enforcement.
 */
public enum QuotaOperation {
  READ, WRITE
}
