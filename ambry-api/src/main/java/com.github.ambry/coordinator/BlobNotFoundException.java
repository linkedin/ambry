package com.github.ambry.coordinator;

/**
 * Error thrown when a blob is not found
 */

public class BlobNotFoundException extends Exception
{
  private static final long serialVersionUID = 1;

  public BlobNotFoundException(String message)
  {
    super(message);
  }

  public BlobNotFoundException(String message, Throwable e)
  {
    super(message,e);
  }

  public BlobNotFoundException(Throwable e)
  {
    super(e);
  }
}
