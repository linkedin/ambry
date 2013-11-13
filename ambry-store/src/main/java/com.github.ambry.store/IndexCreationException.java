package com.github.ambry.store;

/**
 * IndexCreationException
 */
public class IndexCreationException extends Exception {
  private static final long serialVersionUID = 1;

  public IndexCreationException(String message)
  {
    super(message);
  }

  public IndexCreationException(String message, Throwable e)
  {
    super(message,e);
  }

  public IndexCreationException(Throwable e)
  {
    super(e);
  }
}
