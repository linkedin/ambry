/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ambry.config;

import java.util.Arrays;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link FrontendConfig}.
 */
public class FrontendConfigTest {

  @Test
  public void testNamedBlobCleanupContainerDelaySeconds() {
    Properties properties = new Properties();
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("The inter-container delay should be disabled by default", 0,
        config.namedBlobCleanupContainerDelaySeconds);

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_CONTAINER_DELAY_SECONDS, "17");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("The configured inter-container delay should be used", 17,
        config.namedBlobCleanupContainerDelaySeconds);
  }

  @Test
  public void testNamedBlobCleanupContainerDelaySecondsRejectsNegativeValue() {
    Properties properties = new Properties();
    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_CONTAINER_DELAY_SECONDS, "-1");
    try {
      new FrontendConfig(new VerifiableProperties(properties));
      Assert.fail("A negative inter-container delay should be rejected");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testNamedBlobCleanupInitialDelayMaxSeconds() {
    Properties properties = new Properties();
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("The initial delay max should default to 600 seconds", 600,
        config.namedBlobCleanupInitialDelayMaxSeconds);

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_INITIAL_DELAY_MAX_SECONDS, "42");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("The configured initial delay max should be used", 42,
        config.namedBlobCleanupInitialDelayMaxSeconds);
  }

  @Test
  public void testNamedBlobCleanupInitialDelayMaxSecondsRejectsNegativeValue() {
    Properties properties = new Properties();
    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_INITIAL_DELAY_MAX_SECONDS, "-1");
    try {
      new FrontendConfig(new VerifiableProperties(properties));
      Assert.fail("A negative initial delay max should be rejected");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testNamedBlobCleanupExcludedContainers() {
    Properties properties = new Properties();
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertTrue("No containers should be excluded from cleanup by default",
        config.namedBlobCleanupExcludedContainers.isEmpty());

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_EXCLUDED_CONTAINERS, "account1/container1,account2/container2");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("The configured excluded containers should be parsed",
        Arrays.asList("account1/container1", "account2/container2"), config.namedBlobCleanupExcludedContainers);

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_EXCLUDED_CONTAINERS,
        "account1/container1, account2/container2 ,  ");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("Whitespace around comma-separated entries should be trimmed and blank entries dropped",
        Arrays.asList("account1/container1", "account2/container2"), config.namedBlobCleanupExcludedContainers);

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_EXCLUDED_CONTAINERS, "my account/my container");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("Whitespace within a name must be preserved; only outer padding is trimmed",
        Arrays.asList("my account/my container"), config.namedBlobCleanupExcludedContainers);
  }

  @Test
  public void testNamedBlobCleanupExcludedAccounts() {
    Properties properties = new Properties();
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertTrue("No accounts should be excluded from cleanup by default",
        config.namedBlobCleanupExcludedAccounts.isEmpty());

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_EXCLUDED_ACCOUNTS, "account1, account2 ,  ");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("The configured excluded accounts should be parsed and trimmed",
        Arrays.asList("account1", "account2"), config.namedBlobCleanupExcludedAccounts);

    properties.setProperty(FrontendConfig.NAMED_BLOB_CLEANUP_EXCLUDED_ACCOUNTS, "my account");
    config = new FrontendConfig(new VerifiableProperties(properties));
    Assert.assertEquals("Whitespace within an account name must be preserved; only outer padding is trimmed",
        Arrays.asList("my account"), config.namedBlobCleanupExcludedAccounts);
  }
}
