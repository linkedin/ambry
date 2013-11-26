package com.github.ambry.store;

/**
 * IllegalIndexOperation
 */
public class IllegalIndexOperation extends Exception {
  private static final long serialVersionUID = 1;

  public IllegalIndexOperation(String message)
  {
    super(message);
  }

  public IllegalIndexOperation(String message, Throwable e)
  {
    super(message,e);
  }

  public IllegalIndexOperation(Throwable e)
  {
    super(e);
  }
}