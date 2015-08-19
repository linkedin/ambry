package com.github.ambry.network;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
 */
public class SSLBlockingEchoServer extends Thread {
  public final int port;
  private final ServerSocket serverSocket;
  private final List<Thread> threads;
  private final List<Socket> sockets;
  private final List<Exception> exceptions;
  private final AtomicBoolean renegotiate = new AtomicBoolean();

  public SSLBlockingEchoServer(SSLFactory sslFactory, int sslPort) throws Exception {
    if (sslFactory == null) {
      this.serverSocket = new ServerSocket(0);
    } else {
      SSLContext sslContext = sslFactory.createSSLContext();
      this.serverSocket = sslContext.getServerSocketFactory().createServerSocket(sslPort);

      // enable mutual authentication
      ((SSLServerSocket)this.serverSocket).setNeedClientAuth(true);
    }
    this.port = this.serverSocket.getLocalPort();
    this.threads = Collections.synchronizedList(new ArrayList<Thread>());
    this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
    this.exceptions = Collections.synchronizedList(new ArrayList<Exception>());
  }

  /**
   * Test client to handle renegotiation from server
   * It only affect the next connection rather than all connections
   * After setting renegotiate to true, the next connection to the server will be renegotiated
   */
  public void renegotiate() {
    renegotiate.set(true);
  }

  @Override
  public void run() {
    try {
      System.out.println("Echo server started.");
      while (true) {
        final Socket socket = serverSocket.accept();
        sockets.add(socket);
        Thread thread = new Thread() {
          @Override
          public void run() {
            try {
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

  private void closeConnections()
      throws IOException {
    for (Socket socket : sockets) {
      socket.close();
    }
  }

  public void close()
      throws IOException, InterruptedException {
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
