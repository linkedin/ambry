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

import java.security.Principal;
import java.security.cert.Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;


/**
 * A mock of {@link SSLSession} that either delegates to a passed in {@link SSLSession}, or returns the provided
 * objects.
 */
public class MockSSLSession implements SSLSession {
  private SSLSession delegateSession;
  private Certificate[] peerCertsToReturn;

  /**
   * @param delegateSession An {@link SSLSession} to delegate to in most cases.
   * @param peerCertsToReturn If not {@code null}, return these certificates in {@link #getPeerCertificates()} instead
   *                          of delegating.
   */
  public MockSSLSession(SSLSession delegateSession, Certificate[] peerCertsToReturn) {
    this.delegateSession = delegateSession;
    this.peerCertsToReturn = peerCertsToReturn;
  }

  @Override
  public byte[] getId() {
    return delegateSession.getId();
  }

  @Override
  public SSLSessionContext getSessionContext() {
    return delegateSession.getSessionContext();
  }

  @Override
  public long getCreationTime() {
    return delegateSession.getCreationTime();
  }

  @Override
  public long getLastAccessedTime() {
    return delegateSession.getLastAccessedTime();
  }

  @Override
  public void invalidate() {
    delegateSession.invalidate();
  }

  @Override
  public boolean isValid() {
    return delegateSession.isValid();
  }

  @Override
  public void putValue(String s, Object o) {
    delegateSession.putValue(s, o);
  }

  @Override
  public Object getValue(String s) {
    return delegateSession.getValue(s);
  }

  @Override
  public void removeValue(String s) {
    delegateSession.removeValue(s);
  }

  @Override
  public String[] getValueNames() {
    return delegateSession.getValueNames();
  }

  @Override
  public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
    if (peerCertsToReturn != null) {
      return peerCertsToReturn;
    }
    return delegateSession.getPeerCertificates();
  }

  @Override
  public Certificate[] getLocalCertificates() {
    return delegateSession.getLocalCertificates();
  }

  @Override
  public javax.security.cert.X509Certificate[] getPeerCertificateChain() throws SSLPeerUnverifiedException {
    return delegateSession.getPeerCertificateChain();
  }

  @Override
  public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
    return delegateSession.getPeerPrincipal();
  }

  @Override
  public Principal getLocalPrincipal() {
    return delegateSession.getLocalPrincipal();
  }

  @Override
  public String getCipherSuite() {
    return delegateSession.getCipherSuite();
  }

  @Override
  public String getProtocol() {
    return delegateSession.getProtocol();
  }

  @Override
  public String getPeerHost() {
    return delegateSession.getPeerHost();
  }

  @Override
  public int getPeerPort() {
    return delegateSession.getPeerPort();
  }

  @Override
  public int getPacketBufferSize() {
    return delegateSession.getPacketBufferSize();
  }

  @Override
  public int getApplicationBufferSize() {
    return delegateSession.getApplicationBufferSize();
  }
}
