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
package com.github.ambry.rest;

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;


/**
 * Factory that returns null on any function.
 * <p/>
 * Public because factories are usually constructed via {@link com.github.ambry.utils.Utils#getObj(String, Object...)}
 */
public class FaultyFactory
    implements AccountServiceFactory, RestRequestServiceFactory, NioServerFactory, RestRequestResponseHandlerFactory,
               RouterFactory {

  // for RestResponseHandlerFactory
  public FaultyFactory(Object obj1, Object obj2) {
    // don't care.
  }

  // for RestRequestHandlerFactory and AccountServiceFactory
  public FaultyFactory(Object obj1, Object obj2, Object obj3) {
    // don't care.
  }

  // for RestRequestServiceFactory
  public FaultyFactory(Object obj1, Object obj2, Object obj3, Object obj4) {
    // don't care.
  }

  // for RouterFactory
  public FaultyFactory(Object obj1, Object obj2, Object obj3, Object obj4, Object obj5) {
    // don't care.
  }

  // for NioServerFactory
  public FaultyFactory(Object obj1, Object obj2, Object obj3, Object obj4, Object obj5, Object obj6) {
    // don't care.
  }

  @Override
  public RestRequestService getRestRequestService() throws InstantiationException {
    return null;
  }

  @Override
  public NioServer getNioServer() throws InstantiationException {
    return null;
  }

  @Override
  public Router getRouter() throws InstantiationException {
    return null;
  }

  @Override
  public RestResponseHandler getRestResponseHandler() throws InstantiationException {
    return null;
  }

  @Override
  public RestRequestHandler getRestRequestHandler() throws InstantiationException {
    return null;
  }

  @Override
  public AccountService getAccountService() throws InstantiationException {
    return null;
  }
}
