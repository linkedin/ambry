package com.github.ambry.utils;

import java.util.function.Consumer;


/**
 * Similar to {@link Consumer}, but able to throw checked exceptions.
 * @param <T> the type of the input to the operation
 */
public interface ThrowingConsumer<T> {

  /**
   * Performs this operation on the given argument.
   *
   * @param t the input argument
   */
  void accept(T t) throws Exception;
}
