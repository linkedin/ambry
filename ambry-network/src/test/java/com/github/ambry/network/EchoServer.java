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
package com.github.ambry.network;

import com.github.ambry.commons.SSLFactory;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;


/**
 * A simple server that takes size delimited byte arrays and just echos them back to the sender.
 * Supports both plaintext and SSL connections
 */
public class EchoServer extends Thread {
  public final int port;
  private final ServerSocket serverSocket;
  private final List<Thread> threads;
  private final List<Socket> sockets;
  private final List<Exception> exceptions;
  private final AtomicBoolean renegotiate = new AtomicBoolean();

  /**
   * Create an EchoServer that supports plaintext connections
   */
  public EchoServer(int port) throws Exception {
    this(null, port);
  }

  /**
   * Create an EchoServer that supports SSL connections
   */
  public EchoServer(SSLFactory sslFactory, int port) throws Exception {
    this.port = port;
    if (sslFactory == null) {
      this.serverSocket = new ServerSocket(port);
    } else {
      SSLContext sslContext = sslFactory.getSSLContext();
      this.serverSocket = sslContext.getServerSocketFactory().createServerSocket(port);

      // enable mutual authentication
      ((SSLServerSocket) this.serverSocket).setNeedClientAuth(true);
    }
    this.threads = Collections.synchronizedList(new ArrayList<Thread>());
    this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
    this.exceptions = Collections.synchronizedList(new ArrayList<Exception>());
  }

  /**
   * Test client to handle renegotiation from server
   * It only affects the next connection rather than all connections
   * After setting renegotiate to true, the next connection to the server will be renegotiated
   */
  public void renegotiate() {
    renegotiate.set(true);
  }

  @Override
  public void run() {
    try {
      while (true) {
        final Socket socket = serverSocket.accept();
        sockets.add(socket);
        Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              socket.setSoTimeout(3000);
              DataInputStream input = new DataInputStream(socket.getInputStream());
              DataOutputStream output = new DataOutputStream(socket.getOutputStream());
              while (socket.isConnected() && !socket.isClosed()) {
                long size = input.readLong();
                if (renegotiate.compareAndSet(true, false)) {
                  ((SSLSocket) socket).startHandshake();
                }
                byte[] bytes = new byte[(int) size - 8];
                input.readFully(bytes);
                output.writeLong(size);
                output.write(bytes);
                output.flush();
              }
            } catch (IOException e) {
              // ignore, caused by client closed connection
            } finally {
              try {
                socket.close();
              } catch (IOException e) {
                exceptions.add(e);
              }
            }
          }
        };
        thread.start();
        threads.add(thread);
      }
    } catch (IOException e) {
      exceptions.add(e);
    }
  }

  public void closeConnections() throws IOException {
    for (Socket socket : sockets) {
      socket.close();
    }
  }

  public void close() throws IOException, InterruptedException {
    this.serverSocket.close();
    closeConnections();
    for (Thread t : threads) {
      t.join();
    }
    join();
  }

  public int getExceptionCount() {
    return exceptions.size();
  }
}
