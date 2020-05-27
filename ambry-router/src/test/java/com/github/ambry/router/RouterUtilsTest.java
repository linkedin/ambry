/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


public class RouterUtilsTest {
  Random random = new Random();
  ClusterMap clusterMap;
  PartitionId partition;
  BlobId originalBlobId;
  String blobIdStr;

  public void initialize() {
    try {
      clusterMap = new MockClusterMap();
    } catch (Exception e) {
      fail("Should not get any exception.");
    }
    partition = clusterMap.getRandomWritablePartition(MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    originalBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        clusterMap.getLocalDatacenterId(), Utils.getRandomShort(random), Utils.getRandomShort(random), partition, false,
        BlobId.BlobDataType.DATACHUNK);
    blobIdStr = originalBlobId.getID();
  }

  @Test
  public void testInvalidInputString() {
    initialize();
    try {
      RouterUtils.getBlobIdFromString(null, clusterMap);
      fail("The input blob string is invalid. Should fail here");
    } catch (RouterException e) {
      assertEquals("The input blob string is invalid.", e.getErrorCode(), RouterErrorCode.InvalidBlobId);
    }
    try {
      RouterUtils.getBlobIdFromString("", clusterMap);
      fail("The input blob string is invalid. Should fail here");
    } catch (RouterException e) {
      assertEquals("", e.getErrorCode(), RouterErrorCode.InvalidBlobId);
    }
    try {
      RouterUtils.getBlobIdFromString("abc", clusterMap);
      fail("The input blob string is invalid. Should fail here");
    } catch (RouterException e) {
      assertEquals("", e.getErrorCode(), RouterErrorCode.InvalidBlobId);
    }
  }

  @Test
  public void testGoodCase() throws Exception {
    initialize();
    BlobId convertedBlobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
    assertEquals("The converted BlobId should be the same as the original.", originalBlobId, convertedBlobId);
  }

  /**
   * Test to ensure system health errors are interpreted correctly.
   */
  @Test
  public void testSystemHealthErrorInterpretation() {
    for (RouterErrorCode errorCode : RouterErrorCode.values()) {
      switch (errorCode) {
        case InvalidBlobId:
        case InvalidPutArgument:
        case BlobTooLarge:
        case BadInputChannel:
        case BlobDeleted:
        case BlobDoesNotExist:
        case BlobAuthorizationFailure:
        case BlobExpired:
        case RangeNotSatisfiable:
        case ChannelClosed:
        case BlobUpdateNotAllowed:
          Assert.assertFalse(RouterUtils.isSystemHealthError(new RouterException("", errorCode)));
          break;
        default:
          Assert.assertTrue(RouterUtils.isSystemHealthError(new RouterException("", errorCode)));
          break;
      }
    }
    Assert.assertTrue(RouterUtils.isSystemHealthError(new Exception()));
    Assert.assertFalse(RouterUtils.isSystemHealthError(Utils.convertToClientTerminationException(new Exception())));
  }

  /**
   * Test {@link RouterUtils#getAccountContainer(AccountService, short, short)}.
   */
  @Test
  public void testGetAccountContainer() {
    AccountService accountService = new InMemAccountService(false, false);
    // Both accountId and containerId are not tracked by AccountService.
    Pair<Account, Container> accountContainer =
        RouterUtils.getAccountContainer(accountService, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID);
    Assert.assertEquals("Account should be null", null, accountContainer.getFirst());
    Assert.assertEquals("Container should be null", null, accountContainer.getSecond());

    accountContainer =
        RouterUtils.getAccountContainer(accountService, Utils.getRandomShort(random), Utils.getRandomShort(random));
    Assert.assertEquals("Account should be null", null, accountContainer.getFirst());
    Assert.assertEquals("Container should be null", null, accountContainer.getSecond());

    // accountId is tracked by AccountService but containerId not.
    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    Account account = new AccountBuilder(accountId, "AccountNameOf" + accountId, Account.AccountStatus.ACTIVE).build();
    accountService.updateAccounts(Arrays.asList(account));
    accountContainer = RouterUtils.getAccountContainer(accountService, accountId, containerId);
    Assert.assertEquals("Account doesn't match", account, accountContainer.getFirst());
    Assert.assertEquals("Container should be null", null, accountContainer.getSecond());

    // Both accountId and containerId are tracked by AccountService.
    Container container =
        new ContainerBuilder(containerId, "ContainerNameOf" + containerId, Container.ContainerStatus.ACTIVE,
            "description", accountId).build();
    account =
        new AccountBuilder(accountId, "AccountNameOf" + accountId, Account.AccountStatus.ACTIVE).addOrUpdateContainer(
            container).build();
    accountService.updateAccounts(Arrays.asList(account));
    accountContainer = RouterUtils.getAccountContainer(accountService, accountId, containerId);
    Assert.assertEquals("Account doesn't match", account, accountContainer.getFirst());
    Assert.assertEquals("Container doesn't match", container, accountContainer.getSecond());
  }
}
