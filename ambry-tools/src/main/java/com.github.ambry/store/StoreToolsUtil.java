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
package com.github.ambry.store;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;


/**
 * Common utilities used by many store tools
 */
class StoreToolsUtil {

  /**
   * Fetches the properties from Property file and generates the {@link VerifiableProperties}
   * @param args String array containing the arguments passed in
   * @return the {@link VerifiableProperties} generated from the properties in the property file
   * @throws IOException
   */
  static VerifiableProperties getVerifiableProperties(String[] args) throws IOException {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> propsFileOpt = parser.accepts("propsFile", "Properties file path")
        .withRequiredArg()
        .describedAs("propsFile")
        .ofType(String.class);

    OptionSet options = parser.parse(args);
    String propsFilePath = options.valueOf(propsFileOpt);
    Properties properties = Utils.loadProps(propsFilePath);
    return new VerifiableProperties(properties);
  }
}
