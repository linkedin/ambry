package com.github.ambry.messageformat;


// Exception used when there corrupt data is detected.
// Used when the underlying stores data is corrupt on read.
public class DataCorruptException extends Exception
{
  private static final long serialVersionUID = 1;

  public DataCorruptException(String message)
  {
    super(message);
  }

  public DataCorruptException(String message, Throwable e)
  {
    super(message,e);
  }

  public DataCorruptException(Throwable e)
  {
    super(e);
  }
}

