package com.github.ambry.network;

interface RequestQueue {
  boolean offer(Request request);

  RequestBundle take() throws InterruptedException;

  int size();
}
