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

package com.github.ambry.rest;

import java.nio.ByteBuffer;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;


/**
 * A mock of {@link SSLEngine} that either delegates to a passed in {@link SSLEngine}, or returns the provided
 * objects.
 */
class MockSSLEngine extends SSLEngine {
  private final SSLEngine delegateEngine;
  private final SSLSession sessionToReturn;

  /**
   * @param delegateEngine An {@link SSLEngine} to delegate to in most cases.
   * @param sessionToReturn If not {@code null}, return this certificate in {@link #getSession()} instead of delegating.
   */
  MockSSLEngine(SSLEngine delegateEngine, SSLSession sessionToReturn) {
    this.delegateEngine = delegateEngine;
    this.sessionToReturn = sessionToReturn;
  }

  @Override
  public String getPeerHost() {
    return delegateEngine.getPeerHost();
  }

  @Override
  public int getPeerPort() {
    return delegateEngine.getPeerPort();
  }

  @Override
  public SSLEngineResult wrap(ByteBuffer byteBuffer, ByteBuffer byteBuffer1) throws SSLException {
    return delegateEngine.wrap(byteBuffer, byteBuffer1);
  }

  @Override
  public SSLEngineResult wrap(ByteBuffer[] byteBuffers, ByteBuffer byteBuffer) throws SSLException {
    return delegateEngine.wrap(byteBuffers, byteBuffer);
  }

  @Override
  public SSLEngineResult wrap(ByteBuffer[] byteBuffers, int i, int i1, ByteBuffer byteBuffer) throws SSLException {
    return delegateEngine.wrap(byteBuffers, i, i1, byteBuffer);
  }

  @Override
  public SSLEngineResult unwrap(ByteBuffer byteBuffer, ByteBuffer byteBuffer1) throws SSLException {
    return delegateEngine.unwrap(byteBuffer, byteBuffer1);
  }

  @Override
  public SSLEngineResult unwrap(ByteBuffer byteBuffer, ByteBuffer[] byteBuffers) throws SSLException {
    return delegateEngine.unwrap(byteBuffer, byteBuffers);
  }

  @Override
  public SSLEngineResult unwrap(ByteBuffer byteBuffer, ByteBuffer[] byteBuffers, int i, int i1) throws SSLException {
    return delegateEngine.unwrap(byteBuffer, byteBuffers, i, i1);
  }

  @Override
  public Runnable getDelegatedTask() {
    return delegateEngine.getDelegatedTask();
  }

  @Override
  public void closeInbound() throws SSLException {
    delegateEngine.closeInbound();
  }

  @Override
  public boolean isInboundDone() {
    return delegateEngine.isInboundDone();
  }

  @Override
  public void closeOutbound() {
    delegateEngine.closeOutbound();
  }

  @Override
  public boolean isOutboundDone() {
    return delegateEngine.isOutboundDone();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return delegateEngine.getSupportedCipherSuites();
  }

  @Override
  public String[] getEnabledCipherSuites() {
    return delegateEngine.getEnabledCipherSuites();
  }

  @Override
  public void setEnabledCipherSuites(String[] strings) {
    delegateEngine.setEnabledCipherSuites(strings);
  }

  @Override
  public String[] getSupportedProtocols() {
    return delegateEngine.getSupportedProtocols();
  }

  @Override
  public String[] getEnabledProtocols() {
    return delegateEngine.getEnabledProtocols();
  }

  @Override
  public void setEnabledProtocols(String[] strings) {
    delegateEngine.setEnabledProtocols(strings);
  }

  @Override
  public SSLSession getSession() {
    return sessionToReturn != null ? sessionToReturn : delegateEngine.getSession();
  }

  @Override
  public SSLSession getHandshakeSession() {
    return delegateEngine.getHandshakeSession();
  }

  @Override
  public void beginHandshake() throws SSLException {
    delegateEngine.beginHandshake();
  }

  @Override
  public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
    return delegateEngine.getHandshakeStatus();
  }

  @Override
  public void setUseClientMode(boolean b) {
    delegateEngine.setUseClientMode(b);
  }

  @Override
  public boolean getUseClientMode() {
    return delegateEngine.getUseClientMode();
  }

  @Override
  public void setNeedClientAuth(boolean b) {
    delegateEngine.setNeedClientAuth(b);
  }

  @Override
  public boolean getNeedClientAuth() {
    return delegateEngine.getNeedClientAuth();
  }

  @Override
  public void setWantClientAuth(boolean b) {
    delegateEngine.setWantClientAuth(b);
  }

  @Override
  public boolean getWantClientAuth() {
    return delegateEngine.getWantClientAuth();
  }

  @Override
  public void setEnableSessionCreation(boolean b) {
    delegateEngine.setEnableSessionCreation(b);
  }

  @Override
  public boolean getEnableSessionCreation() {
    return delegateEngine.getEnableSessionCreation();
  }

  @Override
  public SSLParameters getSSLParameters() {
    return delegateEngine.getSSLParameters();
  }

  @Override
  public void setSSLParameters(SSLParameters sslParameters) {
    delegateEngine.setSSLParameters(sslParameters);
  }
}
