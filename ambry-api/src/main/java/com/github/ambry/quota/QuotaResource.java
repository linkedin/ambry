/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Resource for which quota is specified for enforced.
 */
public class QuotaResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuotaResource.class);
  public final static String DELIM = "_";
  private final String resourceId; // unique identifier for Ambry account, container or host.
  private final QuotaResourceType quotaResourceType;

  /**
   * Constructor for {@link QuotaResource}.
   * @param resourceId Id if the resource.
   * @param quotaResourceType {@link QuotaResourceType} object specifying the type of resource.
   */
  public QuotaResource(String resourceId, QuotaResourceType quotaResourceType) {
    this.resourceId = Objects.requireNonNull(resourceId);
    this.quotaResourceType = Objects.requireNonNull(quotaResourceType);
  }

  /**
   * Create {@link QuotaResource} from {@link Container}.
   * @param container {@link Container} object.
   * @return QuotaResource object.
   */
  public static QuotaResource fromContainer(Container container) {
    return new QuotaResource(
        String.join(DELIM, String.valueOf(container.getParentAccountId()), String.valueOf(container.getId())),
        QuotaResourceType.CONTAINER);
  }

  /**
   * Create {@link QuotaResource} from account id and container id.
   * @param accountId The account id.
   * @param containerId The container id.
   * @return QuotaResource object.
   */
  public static QuotaResource fromContainerId(short accountId, short containerId) {
    return new QuotaResource(String.format("%d_%d", accountId, containerId), QuotaResourceType.CONTAINER);
  }

  /**
   * Create {@link QuotaResource} from {@link Account}.
   * @param account {@link Account} object.
   * @return QuotaResource object.
   */
  public static QuotaResource fromAccount(Account account) {
    return new QuotaResource(Integer.toString(account.getId()), QuotaResourceType.ACCOUNT);
  }

  /**
   * Create {@link QuotaResource} from account id.
   * @param accountId The account id.
   * @return QuotaResource object.
   */
  public static QuotaResource fromAccountId(short accountId) {
    return new QuotaResource(Integer.toString(accountId), QuotaResourceType.ACCOUNT);
  }

  /**
   * Create {@link QuotaResource} for the specified {@link RestRequest}.
   * @param restRequest {@link RestRequest} object.
   * @return QuotaResource extracted from headers of {@link RestRequest}.
   * @throws QuotaException if appropriate headers aren't found in the {@link RestRequest}.
   */
  public static QuotaResource fromRestRequest(RestRequest restRequest) throws QuotaException {
    try {
      final Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
      if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
        return QuotaResource.fromAccountId(account.getId());
      }
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      return QuotaResource.fromContainerId(account.getId(), container.getId());
    } catch (RestServiceException rEx) {
      LOGGER.error("Could not get quota resource for request: {} due to {}", RestUtils.convertToStr(restRequest),
          rEx.getMessage());
      throw new QuotaException("Could not get quota resource for request: " + RestUtils.convertToStr(restRequest),
          false);
    }
  }

  /**
   * @return the resourceId.
   */
  public String getResourceId() {
    return resourceId;
  }

  /**
   * @return the {@link QuotaResourceType}.
   */
  public QuotaResourceType getQuotaResourceType() {
    return quotaResourceType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    QuotaResource other = (QuotaResource) obj;
    return other.getResourceId().equals(resourceId) && other.getQuotaResourceType().equals(quotaResourceType);
  }

  @Override
  public int hashCode() {
    return 89 * quotaResourceType.hashCode() + resourceId.hashCode();
  }

  @Override
  public String toString() {
    return String.format("QuotaResource: [resourceId: %s, resourceType: %s]", resourceId, quotaResourceType);
  }
}
