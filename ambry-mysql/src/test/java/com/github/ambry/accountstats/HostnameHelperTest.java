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
package com.github.ambry.accountstats;

import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link HostnameHelper}.
 */
public class HostnameHelperTest {

  @Test
  public void testHostnameHelperBasic() throws Exception {
    String[] domains = {".github.com", ".travis.com", "noleadingdot.com", "  needtrim.com  ", "    "};
    AccountStatsMySqlConfig config = createAccountStatsMySqlConfig(String.join(",", domains));
    int port = 1000;
    HostnameHelper helper = new HostnameHelper(config, port);
    Assert.assertEquals("ambry-test1_1000", helper.simplifyHostname("ambry-test1.github.com"));
    Assert.assertEquals("ambry-test1_1000", helper.simplifyHostname("ambry-test1.travis.com"));
    Assert.assertEquals("ambry-test1_1000", helper.simplifyHostname("ambry-test1.noleadingdot.com"));
    Assert.assertEquals("ambry-test1_1000", helper.simplifyHostname("ambry-test1.needtrim.com"));
    Assert.assertEquals("ambry-test1.example.com_1000", helper.simplifyHostname("ambry-test1.example.com"));
  }

  private AccountStatsMySqlConfig createAccountStatsMySqlConfig(String domainNamesToRemove) {
    Properties properties = new Properties();
    properties.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, domainNamesToRemove);
    return new AccountStatsMySqlConfig(new VerifiableProperties(properties));
  }
}
