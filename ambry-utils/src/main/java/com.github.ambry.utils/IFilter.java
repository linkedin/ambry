package com.github.ambry.utils;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface IFilter extends Closeable
{
  public abstract void add(ByteBuffer key);

  public abstract boolean isPresent(ByteBuffer key);

  public abstract void clear();
}