/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;


/**
 * An abstract class that implements most of the {@link ByteBufHolder} interface so the subclass of this class
 * only have to provide necessary implementation of a few methods.
 * @param <T> The subclass type.
 */
public abstract class AbstractByteBufHolder<T extends AbstractByteBufHolder<T>> implements ByteBufHolder {

  @Override
  public abstract ByteBuf content();

  @Override
  public abstract T replace(ByteBuf content);

  @Override
  public T copy() {
    return replace(content().copy());
  }

  @Override
  public T duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public T retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public int refCnt() {
    return content().refCnt();
  }

  @Override
  public T retain() {
    content().retain();
    return (T) this;
  }

  @Override
  public T retain(int increment) {
    content().retain(increment);
    return (T) this;
  }

  @Override
  public T touch() {
    if (content() != null) {
      content().touch();
    }
    return (T) this;
  }

  @Override
  public T touch(Object hint) {
    if (content() != null) {
      content().touch(hint);
    }
    return (T) this;
  }

  @Override
  public boolean release() {
    if (content() != null) {
      return content().release();
    }
    return false;
  }

  @Override
  public boolean release(int decrement) {
    if (content() != null) {
      content().release(decrement);
    }
    return false;
  }
}
