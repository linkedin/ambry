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

/**
 * Used for starting/stopping an instance of {@link com.github.ambry.rest.RestServer} that acts as an Ambry frontend.
 * This can use InMemoryRouter and other testing classes if required.
 */
public class TestAmbryFrontendMain {

  public static void main(String[] args) {
    AmbryFrontendMain.main(args);
  }
}
